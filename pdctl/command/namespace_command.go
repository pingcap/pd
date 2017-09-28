// Copyright 2017 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package command

import (
	"fmt"
	"net/http"
	"strconv"

	"github.com/spf13/cobra"
)

const (
	namespacePrefix    = "pd/api/v1/namespaces"
	namespaceRelPrefix = "pd/api/v1/namespaces/rel"
)

// NewNamespaceCommand return a namespace sub-command of rootCmd
func NewNamespaceCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "namespace [create|append|remove]",
		Short: "show the namespace information",
		Run:   showNamespaceCommandFunc,
	}
	s.AddCommand(NewCreateNamespaceCommand())
	s.AddCommand(NewAppendTableIDCommand())
	s.AddCommand(NewRemoveTableIDCommand())
	return s
}

// NewCreateNamespaceCommand returns a create sub-command of namespaceCmd
func NewCreateNamespaceCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "create <namespace>",
		Short: "create namespace",
		Run:   createNamespaceCommandFunc,
	}
	return d
}

// NewAppendTableIDCommand returns a add sub-command of namespaceCmd
func NewAppendTableIDCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "append <name> <table_id>",
		Short: "add table id to namespace",
		Run:   appendTableCommandFunc,
	}
	return c
}

// NewRemoveTableIDCommand returns a remove sub-command of namespaceCmd
func NewRemoveTableIDCommand() *cobra.Command {
	c := &cobra.Command{
		Use:   "remove <name> <table_id>",
		Short: "remove table id from namespace",
		Run:   removeTableCommandFunc,
	}
	return c
}

func showNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, namespacePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the namespace information: %s\n", err)
		return
	}
	fmt.Println(r)
}

func createNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println("Usage: namespace create <name>")
		return
	}

	input := make(map[string]interface{})
	input["namespace"] = args[0]

	postJSON(cmd, namespacePrefix, input)
}

func appendTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace append <name> <table_id>")
		return
	}
	if _, err := strconv.Atoi(args[1]); err != nil {
		fmt.Println("table_id shoud be a number")
		return
	}

	input := make(map[string]interface{})
	input["namespace"] = args[0]
	input["table_id"] = args[1]
	input["action"] = "append"

	postJSON(cmd, namespaceRelPrefix, input)
}

func removeTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println("Usage: namespace remove <name> <table_id>")
		return
	}
	if _, err := strconv.Atoi(args[1]); err != nil {
		fmt.Println("table_id shoud be a number")
		return
	}

	input := make(map[string]interface{})
	input["namespace"] = args[0]
	input["table_id"] = args[1]
	input["action"] = "remove"

	postJSON(cmd, namespaceRelPrefix, input)
}
