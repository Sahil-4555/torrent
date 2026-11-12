//go:build !js

package webtorrent

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/anacrolix/torrent/tracker"
	"github.com/coder/websocket"
	"github.com/pion/webrtc/v4"
)

func TestTrackerAnnounceEventEncoding(t *testing.T) {
	for _, test := range []struct {
		name  string
		event tracker.AnnounceEvent
		want  string
	}{
		{"regular", tracker.None, ""},
		{"started", tracker.Started, "started"},
		{"completed", tracker.Completed, "completed"},
		{"stopped", tracker.Stopped, "stopped"},
	} {
		t.Run(test.name, func(t *testing.T) {
			messages := make(chan []byte, 1)
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				conn, err := websocket.Accept(w, r, nil)
				if err != nil {
					return
				}
				defer conn.CloseNow()
				_, message, err := conn.Read(r.Context())
				if err == nil {
					messages <- message
				}
			}))
			defer server.Close()

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			conn, _, err := websocket.Dial(ctx, "ws"+strings.TrimPrefix(server.URL, "http"), nil)
			if err != nil {
				t.Fatal(err)
			}
			defer conn.CloseNow()

			tc := &TrackerClient{
				GetAnnounceRequest: func(event tracker.AnnounceEvent, _ [20]byte) (tracker.AnnounceRequest, error) {
					return tracker.AnnounceRequest{Event: event}, nil
				},
				OnAnnounceSuccessful: func(string) {},
				OnAnnounceError:      func(string, error) {},
				wsConn:               conn,
			}
			if err := tc.announce(test.event, [20]byte{}, nil); err != nil {
				t.Fatal(err)
			}

			select {
			case message := <-messages:
				var fields map[string]json.RawMessage
				if err := json.Unmarshal(message, &fields); err != nil {
					t.Fatal(err)
				}
				if got := string(fields["offers"]); got != "[]" {
					t.Fatalf("announce without offers must encode an empty array, got %s", got)
				}
				rawEvent, present := fields["event"]
				if test.event == tracker.None {
					if present {
						t.Fatalf("regular announce must omit event, got %s", rawEvent)
					}
				} else {
					if !present {
						t.Fatalf("missing event %q", test.want)
					}
					var event string
					if err := json.Unmarshal(rawEvent, &event); err != nil {
						t.Fatal(err)
					}
					if event != test.want {
						t.Fatalf("event = %q, want %q", event, test.want)
					}
				}
			case <-ctx.Done():
				t.Fatal("timed out waiting for tracker announce")
			}
		})
	}
}

// TestAnsweringPeerConnectionsAreCapped checks that a tracker flooding offers
// cannot make the client hold more than maxConcurrentAnsweringPeerConns
// answering peer connections at once. Without the cap the client creates one
// per offer, so the count tracks the flood size.
func TestAnsweringPeerConnectionsAreCapped(t *testing.T) {
	const limit = 3
	const flood = 30

	defer func(prev int) { maxConcurrentAnsweringPeerConns = prev }(maxConcurrentAnsweringPeerConns)
	maxConcurrentAnsweringPeerConns = limit

	// One valid offer, reused with distinct offer ids.
	offerPC, err := webrtc.NewPeerConnection(webrtc.Configuration{})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := offerPC.CreateDataChannel("webrtc-datachannel", nil); err != nil {
		t.Fatal(err)
	}
	rawOffer, err := offerPC.CreateOffer(nil)
	if err != nil {
		t.Fatal(err)
	}
	gather := webrtc.GatheringCompletePromise(offerPC)
	if err := offerPC.SetLocalDescription(rawOffer); err != nil {
		t.Fatal(err)
	}
	<-gather
	offer := *offerPC.LocalDescription()
	offerPC.Close()

	var infoHash [20]byte
	for i := range infoHash {
		infoHash[i] = byte(i + 1)
	}
	ihJSON := binaryToJsonString(infoHash[:])
	peerJSON := binaryToJsonString(make([]byte, 20))

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		conn.SetReadLimit(-1)
		// Drain whatever the client sends (its announce).
		go func() {
			for {
				if _, _, err := conn.Read(r.Context()); err != nil {
					return
				}
			}
		}()
		for i := 0; i < flood; i++ {
			resp := AnnounceResponse{
				Action:   "announce",
				InfoHash: ihJSON,
				PeerID:   peerJSON,
				Offer:    &offer,
				OfferID:  fmt.Sprintf("offer-%d", i),
			}
			data, _ := json.Marshal(resp)
			if err := conn.Write(r.Context(), websocket.MessageText, data); err != nil {
				return
			}
		}
		<-r.Context().Done()
	}))
	defer server.Close()

	tc := &TrackerClient{
		Url: "ws" + strings.TrimPrefix(server.URL, "http"),
		GetAnnounceRequest: func(tracker.AnnounceEvent, [20]byte) (tracker.AnnounceRequest, error) {
			return tracker.AnnounceRequest{}, nil
		},
		OnConn:               func(DataChannelConn, DataChannelContext) {},
		OnConnected:          func(error) {},
		OnDisconnected:       func(error) {},
		OnAnnounceSuccessful: func(string) {},
		OnAnnounceError:      func(string, error) {},
	}
	tc.cond.L = &tc.mu
	tc.pingTicker = time.NewTicker(time.Hour)
	defer tc.pingTicker.Stop()

	go func() { _ = tc.doWebsocket() }()
	defer func() {
		tc.mu.Lock()
		tc.closed = true
		if tc.wsConn != nil {
			tc.wsConn.CloseNow()
		}
		tc.mu.Unlock()
	}()

	// Give the client time to process the flood, tracking the peak held.
	peak := 0
	deadline := time.After(10 * time.Second)
	for {
		tc.mu.Lock()
		n := len(tc.rtcPeerConns)
		tc.mu.Unlock()
		if n > peak {
			peak = n
		}
		if peak > limit {
			break // already violated; fail below
		}
		select {
		case <-deadline:
			goto check
		case <-time.After(20 * time.Millisecond):
		}
	}
check:
	if peak < 1 {
		t.Fatal("no answering peer connections were created; flood never reached the handler")
	}
	if peak > limit {
		t.Fatalf("held %d concurrent answering peer connections, want at most %d", peak, limit)
	}
}

func TestUnansweredTrackerPingClosesConnection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		conn, err := websocket.Accept(w, r, nil)
		if err != nil {
			return
		}
		defer conn.CloseNow()
		// Deliberately do not read frames, so the client never receives a pong.
		<-r.Context().Done()
	}))
	defer server.Close()

	tc := &TrackerClient{
		Url:            "ws" + strings.TrimPrefix(server.URL, "http"),
		OnConnected:    func(error) {},
		OnDisconnected: func(error) {},
	}
	tc.cond.L = &tc.mu
	tc.pingTicker = time.NewTicker(time.Millisecond)
	defer tc.pingTicker.Stop()

	done := make(chan error, 1)
	go func() {
		done <- tc.doWebsocket()
	}()
	defer func() {
		tc.mu.Lock()
		if tc.wsConn != nil {
			tc.wsConn.CloseNow()
		}
		tc.mu.Unlock()
	}()

	select {
	case err := <-done:
		if err == nil {
			t.Fatal("expected read loop to exit after unanswered ping")
		}
	case <-time.After(pingTimeout + 5*time.Second):
		t.Fatal("tracker read loop did not exit after unanswered ping")
	}
}
