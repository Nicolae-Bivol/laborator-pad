package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"time"
)

// Mesajul este serializat de sender si transmis ca text JSON nemodificat.
// Brokerul inspecteaza tipul, dar nu rescrie textul original.
type Message struct {
	Version int    `json:"version"`
	ID      string `json:"id"`
	Type    string `json:"type"`
	Body    string `json:"body"`
}
type Frame struct {
	Op      string `json:"op"`
	Client  string `json:"client,omitempty"`
	Type    string `json:"type,omitempty"`
	Message string `json:"message,omitempty"`
	ID      string `json:"id,omitempty"`
	Error   string `json:"error,omitempty"`
}

// Socket reprezinta nivelul abstract de comunicare; incadrarea este JSON Lines.
// Un singur cititor si un singur scriitor detin fiecare conexiune.
type Socket struct {
	net.Conn
	reader *bufio.Scanner
}

func wrap(c net.Conn) *Socket {
	s := bufio.NewScanner(c)
	s.Buffer(make([]byte, 4096), 64*1024)
	return &Socket{c, s}
}
func (s *Socket) receive() (Frame, error) {
	var f Frame
	if !s.reader.Scan() {
		if err := s.reader.Err(); err != nil {
			return f, err
		}
		return f, fmt.Errorf("conexiune inchisa")
	}
	err := json.Unmarshal(s.reader.Bytes(), &f)
	return f, err
}
func (s *Socket) send(f Frame) error {
	s.SetWriteDeadline(time.Now().Add(5 * time.Second))
	return json.NewEncoder(s.Conn).Encode(f)
}
func dial(addr string) (*Socket, error) {
	c, e := net.DialTimeout("tcp", addr, 3*time.Second)
	if e != nil {
		return nil, e
	}
	return wrap(c), nil
}
func parse(raw string) (Message, error) {
	var m Message
	if len(raw) > 16000 {
		return m, fmt.Errorf("mesaj prea mare")
	}
	if err := json.Unmarshal([]byte(raw), &m); err != nil {
		return m, fmt.Errorf("JSON invalid: %w", err)
	}
	if m.Version != 1 || m.ID == "" || len(m.ID) > 100 || m.Type == "" || len(m.Type) > 50 || m.Body == "" {
		return m, fmt.Errorf("contract invalid: version=1, id, type si body obligatorii")
	}
	return m, nil
}
