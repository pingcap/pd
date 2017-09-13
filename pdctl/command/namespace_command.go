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
	"github.com/spf13/cobra"
	"net/http"
	"strconv"
)

const namespacePrefix = "pd/api/v1/namespaces"

// NewNamespaceCommand return a namespace sub-command of rootCmd
func NewNamespaceCommand() *cobra.Command {
	s := &cobra.Command{
		Use:   "namespace",
		Short: "show the namespace information",
		Run:   showNamespaceCommandFunc,
	}
	s.AddCommand(NewAddNamespaceCommand())
	return s
}

// NewAddNamespaceCommand return a create sub-command of namespaceCmd
func NewAddNamespaceCommand() *cobra.Command {
	d := &cobra.Command{
		Use:   "create <namespace> <table_id>",
		Short: "create namespace",
		Run:   addNamespaceCommandFunc,
	}
	return d
}

func showNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	r, err := doRequest(cmd, namespacePrefix, http.MethodGet)
	if err != nil {
		fmt.Printf("Failed to get the namespace information: %s\n", err)
		return
	}
	fmt.Println(r)
}

func addNamespaceCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		fmt.Println(cmd.Usage())
	}

	tableID, err := strconv.ParseUint(args[1], 10, 64)
	if err != nil {
		fmt.Println(err)
		return
	}

	input := make(map[string]interface{})
	input["namespace"] = args[0]
	input["table_id"] = tableID

	postJSON(cmd, namespacePrefix, input)
}
