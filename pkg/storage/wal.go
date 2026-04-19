package storage

import (
	"bufio"
	"os"
)

type WAL struct {
	file   *os.File
	writer *bufio.Writer
}

func NewWAL(filename string) (*WAL, error) {
	file, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_RDWR, 0666)
	if err != nil {
		return nil, err
	}

	return &WAL{
		file:   file,
		writer: bufio.NewWriter(file),
	}, nil
}

func (w *WAL) AppendCmd(cmd string) error {
	_, err := w.writer.WriteString(cmd + "\n")
	if err != nil {
		return err
	}
	// Force to disk to ensure atomicity
	err = w.writer.Flush()
	if err != nil {
		return err
	}
	return w.file.Sync()
}

func (w *WAL) Close() error {
	w.writer.Flush()
	return w.file.Close()
}

// RecoverFromWAL reads all sequential commands recorded in the WAL.
func RecoverFromWAL(filename string) ([]string, error) {
	file, err := os.OpenFile(filename, os.O_RDONLY, 0666)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil // No WAL exists, normal
		}
		return nil, err
	}
	defer file.Close()

	var cmds []string
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		line := scanner.Text()
		if line != "" {
			cmds = append(cmds, line)
		}
	}
	return cmds, scanner.Err()
}

// TruncateWAL clears the WAL file after a successful disk flush (Checkpoint)
func TruncateWAL(filename string) error {
	return os.Truncate(filename, 0)
}
