package main

import (
	"fmt"
	"log"
	"net"
	"sync"
	"time"
)

type Stored struct {
	Message Message `json:"parsed"`
	Raw     string  `json:"raw"`
}
type Subscription struct {
	Type     string   `json:"type"`
	Cursor   int      `json:"cursor"`
	Attempts int      `json:"attempts"`
	Dead     []string `json:"dead_letter_ids"`
}
type State struct {
	Topics      map[string][]Stored      `json:"topics"`
	Subscribers map[string]*Subscription `json:"subscribers"`
	IDs         map[string]string        `json:"message_ids"`
}
type Broker struct {
	mu         sync.Mutex
	state      State
	path       string
	active     map[string]bool
	changed    chan struct{}
	max        int
	ackTimeout time.Duration
}

func openBroker(path string, types []string, max int, timeout time.Duration) (*Broker, error) {
	b := &Broker{state: State{Topics: map[string][]Stored{}, Subscribers: map[string]*Subscription{}, IDs: map[string]string{}}, path: path, active: map[string]bool{}, changed: make(chan struct{}), max: max, ackTimeout: timeout}
	if path != "" {
		if err := load(path, &b.state); err != nil {
			return nil, err
		}
	}
	for _, t := range types {
		if t == "" {
			return nil, fmt.Errorf("canal gol")
		}
		if _, ok := b.state.Topics[t]; !ok {
			b.state.Topics[t] = []Stored{}
		}
	}
	b.commit()
	return b, nil
}

// Apelata numai sub mutex (sau la initializare). Persistenta este serializata:
// ACK pentru sender este trimis NUMAI dupa Sync. Alte conexiuni sunt acceptate concurent.
func (b *Broker) commit() {
	if b.path != "" {
		if err := save(b.path, b.state); err != nil {
			panic(fmt.Sprintf("eroare persistenta: %v", err))
		}
	}
	close(b.changed)
	b.changed = make(chan struct{})
}
func (b *Broker) publish(raw string) error {
	m, err := parse(raw)
	if err != nil {
		return err
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.state.Topics[m.Type]; !ok {
		return fmt.Errorf("tip necunoscut: %s", m.Type)
	}
	if previous, ok := b.state.IDs[m.ID]; ok {
		if previous != raw {
			return fmt.Errorf("ID existent cu alt continut")
		}
		return nil
	}
	b.state.Topics[m.Type] = append(b.state.Topics[m.Type], Stored{m, raw})
	b.state.IDs[m.ID] = raw
	b.commit()
	return nil
}
func (b *Broker) subscribe(client, typ string) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if client == "" || len(client) > 100 {
		return fmt.Errorf("client invalid")
	}
	if _, ok := b.state.Topics[typ]; !ok {
		return fmt.Errorf("canal necunoscut")
	}
	if b.active[client] {
		return fmt.Errorf("client deja conectat")
	}
	s, ok := b.state.Subscribers[client]
	if ok && s.Type != typ {
		return fmt.Errorf("clientul este legat de alt tip")
	}
	if !ok {
		b.state.Subscribers[client] = &Subscription{Type: typ}
		b.commit()
	}
	b.active[client] = true
	return nil
}

// Un worker pe abonat: push bazat pe evenimente, fara polling de mesaje.
func (b *Broker) stream(s *Socket, client string) {
	defer func() { b.mu.Lock(); delete(b.active, client); b.mu.Unlock() }()
	for {
		b.mu.Lock()
		sub := b.state.Subscribers[client]
		items := b.state.Topics[sub.Type]
		if sub.Cursor >= len(items) {
			wake := b.changed
			b.mu.Unlock()
			// Heartbeat detecteaza un receiver deconectat chiar daca nu exista mesaje.
			select {
			case <-wake:
				continue
			case <-time.After(time.Second):
				if s.send(Frame{Op: "ping"}) != nil {
					return
				}
				s.SetReadDeadline(time.Now().Add(b.ackTimeout))
				f, e := s.receive()
				if e != nil || f.Op != "pong" {
					return
				}
				continue
			}
		}
		item := items[sub.Cursor]
		if sub.Attempts >= b.max {
			sub.Dead = append(sub.Dead, item.Message.ID)
			sub.Cursor++
			sub.Attempts = 0
			b.commit()
			b.mu.Unlock()
			log.Printf("DLQ client=%s id=%s", client, item.Message.ID)
			continue
		}
		sub.Attempts++
		attempt := sub.Attempts
		b.commit()
		b.mu.Unlock()
		if s.send(Frame{Op: "delivery", ID: item.Message.ID, Message: item.Raw}) != nil {
			return
		}
		s.SetReadDeadline(time.Now().Add(b.ackTimeout))
		f, e := s.receive()
		if e != nil {
			return
		} // Fara ACK: cursorul nu avanseaza, relivrare la reconectare.
		if f.ID != item.Message.ID || (f.Op != "ack" && f.Op != "nack") {
			return
		}
		if f.Op == "ack" {
			b.mu.Lock()
			sub.Cursor++
			sub.Attempts = 0
			b.commit()
			b.mu.Unlock()
			log.Printf("ACK client=%s id=%s", client, item.Message.ID)
		} else {
			log.Printf("NACK client=%s id=%s incercare=%d", client, item.Message.ID, attempt)
			time.Sleep(time.Duration(attempt) * 300 * time.Millisecond)
		}
	}
}
func (b *Broker) serveConn(c net.Conn) {
	defer c.Close()
	s := wrap(c)
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	q, err := s.receive()
	if err != nil {
		s.send(Frame{Op: "error", Error: "cadru JSON invalid"})
		return
	}
	switch q.Op {
	case "publish":
		err = b.publish(q.Message)
		if err == nil {
			s.send(Frame{Op: "accepted"})
			return
		}
	case "subscribe":
		err = b.subscribe(q.Client, q.Type)
		if err == nil {
			if s.send(Frame{Op: "subscribed"}) != nil {
				b.mu.Lock()
				delete(b.active, q.Client)
				b.mu.Unlock()
				return
			}
			b.stream(s, q.Client)
			return
		}
	default:
		err = fmt.Errorf("operatie necunoscuta")
	}
	s.send(Frame{Op: "error", Error: err.Error()})
}
func (b *Broker) serve(addr string) error {
	l, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	defer l.Close()
	log.Printf("Broker TCP %s", addr)
	for {
		c, err := l.Accept()
		if err != nil {
			return err
		}
		go b.serveConn(c)
	}
}
