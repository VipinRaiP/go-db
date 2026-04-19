package sql

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

type StatementType int

const (
	Insert StatementType = iota
	Select
	Update
	Delete
	CreateTable
)

type Statement struct {
	Type      StatementType
	TableName string
	ID        uint32
	Data      string
}

// Regex matchers for our limited subset
var (
	insertRegex = regexp.MustCompile(`(?i)^INSERT INTO (\w+) \(id, data\) VALUES \((\d+), "([^"]*)"\)$`)
	selectRegex = regexp.MustCompile(`(?i)^SELECT \* FROM (\w+) WHERE id = (\d+)$`)
	updateRegex = regexp.MustCompile(`(?i)^UPDATE (\w+) SET data = "([^"]*)" WHERE id = (\d+)$`)
	deleteRegex = regexp.MustCompile(`(?i)^DELETE FROM (\w+) WHERE id = (\d+)$`)
	createRegex = regexp.MustCompile(`(?i)^CREATE TABLE (\w+)$`)
)

func Parse(query string) (*Statement, error) {
	query = strings.TrimSpace(query)

	if matches := insertRegex.FindStringSubmatch(query); matches != nil {
		id, _ := strconv.ParseUint(matches[2], 10, 32)
		return &Statement{
			Type:      Insert,
			TableName: matches[1],
			ID:        uint32(id),
			Data:      matches[3],
		}, nil
	}

	if matches := selectRegex.FindStringSubmatch(query); matches != nil {
		id, _ := strconv.ParseUint(matches[2], 10, 32)
		return &Statement{
			Type:      Select,
			TableName: matches[1],
			ID:        uint32(id),
		}, nil
	}

	if matches := updateRegex.FindStringSubmatch(query); matches != nil {
		id, _ := strconv.ParseUint(matches[3], 10, 32)
		return &Statement{
			Type:      Update,
			TableName: matches[1],
			ID:        uint32(id),
			Data:      matches[2],
		}, nil
	}

	if matches := deleteRegex.FindStringSubmatch(query); matches != nil {
		id, _ := strconv.ParseUint(matches[2], 10, 32)
		return &Statement{
			Type:      Delete,
			TableName: matches[1],
			ID:        uint32(id),
		}, nil
	}

	if matches := createRegex.FindStringSubmatch(query); matches != nil {
		return &Statement{
			Type:      CreateTable,
			TableName: matches[1],
		}, nil
	}

	return nil, fmt.Errorf("unrecognized SQL statement: %s", query)
}
