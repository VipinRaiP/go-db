package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"

	"github.com/go-db/pkg/buffer"
	"github.com/go-db/pkg/index"
	"github.com/go-db/pkg/sql"
	"github.com/go-db/pkg/storage"
)

func main() {
	fmt.Println("Welcome to GoDB")
	fmt.Println("Type .exit to save and quit.")

	pager, err := storage.NewPager("data.db")
	if err != nil {
		fmt.Printf("Failed to open pager: %v\n", err)
		os.Exit(1)
	}
	defer pager.Close()

	bufPool := buffer.NewBufferPool(pager, 10)

	var rootPageID storage.PageID = 0
	if pager.NumPages() > 0 {
		rootPageID = 0 // For demo, table 'test' is always rooted at 0
	}
	
	tree := index.NewBTree(bufPool, rootPageID)
	if pager.NumPages() == 0 {
		err = tree.InitEmpty()
		if err != nil {
			fmt.Printf("Failed to init empty tree: %v\n", err)
			os.Exit(1)
		}
	}

	exec := sql.NewExecutor()
	exec.RegisterTable("test", tree)

	// Recover from WAL
	walFile := "godb.wal"
	cmds, err := storage.RecoverFromWAL(walFile)
	if err != nil {
		fmt.Printf("Failed to recover from WAL: %v\n", err)
	} else if len(cmds) > 0 {
		fmt.Printf("Recovering %d commands from WAL...\n", len(cmds))
		for _, cmdStr := range cmds {
			stmt, err := sql.Parse(cmdStr)
			if err == nil {
				exec.Execute(stmt)
			}
		}
		// Checkpoint: flush to disk
		bufPool.FlushAllPages()
		storage.TruncateWAL(walFile)
		fmt.Println("Recovery complete.")
	}

	wal, err := storage.NewWAL(walFile)
	if err != nil {
		fmt.Printf("Failed to open WAL: %v\n", err)
		os.Exit(1)
	}
	defer wal.Close()

	reader := bufio.NewReader(os.Stdin)

	for {
		fmt.Print("godb> ")
		line, err := reader.ReadString('\n')
		if err != nil {
			fmt.Println("Error reading input")
			os.Exit(1)
		}

		line = strings.TrimSpace(line)

		if line == ".exit" {
			fmt.Println("Flushing buffers to disk...")
			err := bufPool.FlushAllPages()
			if err != nil {
				fmt.Printf("Error flushing: %v\n", err)
			}
			storage.TruncateWAL(walFile)
			break
		}

		if line == "" {
			continue
		}

		stmt, err := sql.Parse(line)
		if err != nil {
			fmt.Printf("Parse error: %v\n", err)
			continue
		}

		// Write-Ahead Logging
		if stmt.Type == sql.Insert || stmt.Type == sql.Update || stmt.Type == sql.Delete {
			err = wal.AppendCmd(line)
			if err != nil {
				fmt.Printf("WAL error: %v\n", err)
			}
		}

		result, err := exec.Execute(stmt)
		if err != nil {
			fmt.Printf("Execute error: %v\n", err)
			continue
		}

		fmt.Println(result)
	}
}
