package fsm

import (
	"FinKV/cluster/command"
	"FinKV/cluster/raft"
	"FinKV/database"
	"encoding/json"
	"fmt"
)

type FSM struct {
	db *database.FincasDB
}

func New(db *database.FincasDB) *FSM {
	return &FSM{db: db}
}

func (f *FSM) Apply(log *raft.LogEntry) interface{} {
	var cmd command.BaseCmd
	if err := json.Unmarshal(log.Command, &cmd); err != nil {
		return fmt.Errorf("failed to unmarshal command: %w", err)
	}

	c := command.New(cmd.GetType(), cmd.GetMethod(), cmd.Args)
	if err := c.Apply(f.db); err != nil {
		return fmt.Errorf("failed to apply command: %w", err)
	}

	return nil
}
