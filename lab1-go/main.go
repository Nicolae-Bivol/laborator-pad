package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"
)

type Inbox struct {
	Messages map[string]string `json:"messages"`
	Order    []string          `json:"order"`
}

func openInbox(path string) (*Inbox, error) {
	s := &Inbox{Messages: map[string]string{}}
	e := load(path, s)
	return s, e
}

// Efectul local este includerea mesajului in inbox. Inbox-ul insusi este si
// registrul de deduplicare: nu exista doua scrieri separate efect/ID.
func (s *Inbox) apply(path, raw string) (bool, error) {
	m, err := parse(raw)
	if err != nil {
		return false, err
	}
	if old, ok := s.Messages[m.ID]; ok {
		if old != raw {
			return false, fmt.Errorf("conflict ID")
		}
		return false, nil
	}
	s.Messages[m.ID] = raw
	s.Order = append(s.Order, m.ID)
	if err = save(path, s); err != nil {
		return false, err
	}
	return true, nil
}
func receiveSession(addr, client, typ, path string, inbox *Inbox, crash, fail bool) error {
	s, err := dial(addr)
	if err != nil {
		return err
	}
	defer s.Close()
	if err = s.send(Frame{Op: "subscribe", Client: client, Type: typ}); err != nil {
		return err
	}
	s.SetReadDeadline(time.Now().Add(5 * time.Second))
	f, err := s.receive()
	if err != nil {
		return err
	}
	if f.Op != "subscribed" {
		return fmt.Errorf("abonare: %s", f.Error)
	}
	log.Printf("Abonat client=%s tip=%s", client, typ)
	for {
		s.SetReadDeadline(time.Now().Add(15 * time.Second))
		f, err = s.receive()
		if err != nil {
			return err
		}
		if f.Op == "ping" {
			if err = s.send(Frame{Op: "pong"}); err != nil {
				return err
			}
			continue
		}
		if f.Op != "delivery" {
			return fmt.Errorf("cadru neasteptat")
		}
		m, err := parse(f.Message)
		if err != nil || m.Type != typ || m.ID != f.ID {
			return fmt.Errorf("livrare invalida")
		}
		if fail {
			log.Printf("NACK simulat id=%s", m.ID)
			if err = s.send(Frame{Op: "nack", ID: m.ID}); err != nil {
				return err
			}
			continue
		}
		fresh, err := inbox.apply(path, f.Message)
		if err != nil {
			log.Fatalf("persistenta receiver esuata: %v", err)
		}
		if fresh {
			log.Printf("PRIMIT tip=%s id=%s text=%s", m.Type, m.ID, m.Body)
		} else {
			log.Printf("DUPLICAT IGNORAT id=%s", m.ID)
		}
		if fresh && crash {
			log.Print("CADERE dupa salvare, inainte de ACK")
			os.Exit(23)
		}
		if err = s.send(Frame{Op: "ack", ID: m.ID}); err != nil {
			return err
		}
	}
}
func main() {
	if len(os.Args) < 2 {
		fmt.Println("Utilizare: go run . broker|sender|receiver [optiuni]")
		return
	}
	mode := os.Args[1]
	f := flag.NewFlagSet(mode, flag.ExitOnError)
	addr := f.String("addr", "127.0.0.1:7070", "adresa broker TCP")
	storage := f.String("storage", "persistent", "persistent sau memory (broker)")
	data := f.String("data", "data/broker.jsonl", "jurnal broker")
	types := f.String("types", "info,alert", "canale configurate la pornire")
	typ := f.String("type", "info", "tip mesaj / abonament")
	client := f.String("client", "r1", "identitate stabila receiver")
	inboxPath := f.String("inbox", "data/r1.jsonl", "jurnal exclusiv pentru acest receiver")
	id := f.String("id", "", "ID stabil mesaj; obligatoriu la sender")
	body := f.String("text", "Salut din Go!", "continut mesaj")
	crash := f.Bool("crash-after-save", false, "cadere dupa efect si inainte de ACK")
	fail := f.Bool("fail", false, "simuleaza NACK pentru toate livrarile")
	max := f.Int("max-attempts", 3, "numar maxim de livrari per abonat")
	timeout := f.Duration("ack-timeout", 5*time.Second, "timeout ACK / heartbeat")
	f.Parse(os.Args[2:])
	var err error
	switch mode {
	case "broker":
		if *max < 1 || *timeout <= 0 {
			log.Fatal("parametri invalidi")
		}
		if *storage != "memory" && *storage != "persistent" {
			log.Fatal("storage trebuie sa fie memory sau persistent")
		}
		if *storage == "memory" {
			*data = ""
		}
		var b *Broker
		b, err = openBroker(*data, strings.Split(*types, ","), *max, *timeout)
		if err == nil {
			err = b.serve(*addr)
		}
	case "sender":
		raw, _ := json.Marshal(Message{1, *id, *typ, *body})
		if _, err = parse(string(raw)); err != nil {
			break
		}
		for attempt := 1; attempt <= 3; attempt++ {
			var s *Socket
			s, err = dial(*addr)
			if err == nil {
				err = s.send(Frame{Op: "publish", Message: string(raw)})
				if err == nil {
					s.SetReadDeadline(time.Now().Add(5 * time.Second))
					var r Frame
					r, err = s.receive()
					if err == nil && r.Op != "accepted" {
						s.Close()
						log.Fatalf("mesaj refuzat: %s", r.Error)
					}
				}
				s.Close()
			}
			if err == nil {
				fmt.Println("ACCEPTAT de broker (nu inseamna inca procesat de receiver)")
				break
			}
			log.Printf("retry publicare %d: %v", attempt, err)
			time.Sleep(time.Second)
		}
	case "receiver":
		var inbox *Inbox
		inbox, err = openInbox(*inboxPath)
		if err != nil {
			break
		}
		for {
			err = receiveSession(*addr, *client, *typ, *inboxPath, inbox, *crash, *fail)
			log.Printf("reconectare: %v", err)
			time.Sleep(time.Second)
		}
	default:
		err = fmt.Errorf("mod necunoscut: %s", mode)
	}
	if err != nil {
		log.Fatal(err)
	}
}
