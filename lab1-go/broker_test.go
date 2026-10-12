package main

import (
	"encoding/json"
	"net"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func newTestBroker(t *testing.T) *Broker {
	t.Helper()
	b, e := openBroker(filepath.Join(t.TempDir(), "broker.jsonl"), []string{"info", "alert"}, 3, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	return b
}
func raw(id, typ string) string { p, _ := json.Marshal(Message{1, id, typ, "Salut"}); return string(p) }
func TestRoutingContractAndDedup(t *testing.T) {
	b := newTestBroker(t)
	for _, r := range []string{"{", raw("a", "other"), raw("", "info")} {
		if b.publish(r) == nil {
			t.Fatal("mesaj invalid acceptat")
		}
	}
	if e := b.publish(raw("a", "info")); e != nil {
		t.Fatal(e)
	}
	if e := b.publish(raw("a", "info")); e != nil {
		t.Fatal(e)
	}
	if len(b.state.Topics["info"]) != 1 || len(b.state.Topics["alert"]) != 0 {
		t.Fatal("rutare/dedup gresit")
	}
	if e := b.publish(raw("a", "alert")); e == nil {
		t.Fatal("conflict ID acceptat")
	}
}
func TestConcurrentPublish(t *testing.T) {
	b := newTestBroker(t)
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := b.publish(raw("same", "info")); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if len(b.state.Topics["info"]) != 1 {
		t.Fatal("dedup concurent esuat")
	}
}
func attach(t *testing.T, b *Broker, id, typ string) *Socket {
	t.Helper()
	server, client := net.Pipe()
	go b.serveConn(server)
	s := wrap(client)
	t.Cleanup(func() { s.Close() })
	s.SetDeadline(time.Now().Add(3 * time.Second))
	if e := s.send(Frame{Op: "subscribe", Client: id, Type: typ}); e != nil {
		t.Fatal(e)
	}
	r, e := s.receive()
	if e != nil || r.Op != "subscribed" {
		t.Fatal(r, e)
	}
	return s
}
func delivery(t *testing.T, s *Socket) Frame {
	t.Helper()
	s.SetReadDeadline(time.Now().Add(3 * time.Second))
	r, e := s.receive()
	if e != nil || r.Op != "delivery" {
		t.Fatal(r, e)
	}
	return r
}
func TestPubSubPushAndOrdering(t *testing.T) {
	b := newTestBroker(t)
	a := attach(t, b, "r1", "info")
	c := attach(t, b, "r2", "info")
	for _, id := range []string{"m1", "m2"} {
		if e := b.publish(raw(id, "info")); e != nil {
			t.Fatal(e)
		}
	}
	for _, id := range []string{"m1", "m2"} {
		for _, s := range []*Socket{a, c} {
			r := delivery(t, s)
			if r.ID != id || r.Message != raw(id, "info") {
				t.Fatal("ordine/continut gresit")
			}
			s.send(Frame{Op: "ack", ID: r.ID})
		}
	}
}
func TestCrashAfterLocalEffect(t *testing.T) {
	b := newTestBroker(t)
	b.publish(raw("x", "info"))
	s := attach(t, b, "r1", "info")
	d := delivery(t, s)
	path := filepath.Join(t.TempDir(), "inbox.jsonl")
	in, e := openInbox(path)
	if e != nil {
		t.Fatal(e)
	}
	fresh, e := in.apply(path, d.Message)
	if e != nil || !fresh {
		t.Fatal(fresh, e)
	}
	s.Close() // Niciun ACK trimis.
	// Repornim din starea persistenta, fara sa reutilizam memoria veche.
	b2, e := openBroker(b.path, []string{"info", "alert"}, 3, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	in, e = openInbox(path)
	if e != nil {
		t.Fatal(e)
	}
	s2 := attach(t, b2, "r1", "info")
	d2 := delivery(t, s2)
	fresh, e = in.apply(path, d2.Message)
	if e != nil || fresh || len(in.Order) != 1 {
		t.Fatal("efect duplicat", fresh, e)
	}
	s2.send(Frame{Op: "ack", ID: d2.ID})
}
func TestNackDeadLetter(t *testing.T) {
	b := newTestBroker(t)
	b.max = 2
	b.publish(raw("bad", "info"))
	b.publish(raw("good", "info"))
	s := attach(t, b, "r", "info")
	for i := 0; i < 2; i++ {
		r := delivery(t, s)
		if r.ID != "bad" {
			t.Fatal(r)
		}
		s.send(Frame{Op: "nack", ID: r.ID})
	}
	r := delivery(t, s)
	if r.ID != "good" {
		t.Fatal(r)
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(b.state.Subscribers["r"].Dead) != 1 {
		t.Fatal("DLQ lipsa")
	}
}
func TestMemoryMode(t *testing.T) {
	b, e := openBroker("", []string{"info"}, 3, time.Second)
	if e != nil {
		t.Fatal(e)
	}
	b.publish(raw("x", "info"))
	b2, e := openBroker("", []string{"info"}, 3, time.Second)
	if e != nil || len(b2.state.Topics["info"]) != 0 {
		t.Fatal("mod memory gresit")
	}
}
