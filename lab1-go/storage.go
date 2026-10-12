package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

// Fiecare linie completa este un snapshot. O ultima linie incompleta este
// eliminata dupa o cadere; coruperea unei linii complete opreste pornirea.
func load(path string, target interface{}) error {
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	r := bufio.NewReader(f)
	var offset int64
	for {
		line, err := r.ReadBytes('\n')
		if err == io.EOF {
			if len(line) > 0 {
				if err = f.Truncate(offset); err != nil {
					return err
				}
				return f.Sync()
			}
			return nil
		}
		if err != nil {
			return err
		}
		if err = json.Unmarshal(line, target); err != nil {
			return fmt.Errorf("jurnal corupt la %d: %w", offset, err)
		}
		offset += int64(len(line))
	}
}

// Nu continuam dupa o eroare de scriere: rezultatul unui Sync esuat poate fi ambiguu.
func save(path string, value interface{}) error {
	data, err := json.Marshal(value)
	if err != nil {
		return err
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_APPEND, 0600)
	if err != nil {
		return err
	}
	defer f.Close()
	data = append(data, '\n')
	n, err := f.Write(data)
	if err != nil {
		return err
	}
	if n != len(data) {
		return io.ErrShortWrite
	}
	return f.Sync()
}
