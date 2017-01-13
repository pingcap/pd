// Copyright 2016 PingCAP, Inc.
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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strconv"

	"github.com/pingcap/kvproto/pkg/metapb"
	"github.com/spf13/cobra"
)

var (
	regionsPrefix = "pd/api/v1/regions"
	regionPrefix  = "pd/api/v1/region/%s"
)

type regionInfos struct {
	Regions *metapb.Region `json:"regions"`
	Leader  *metapb.Peer   `json:"leader"`
}

// NewRegionCommand return a region subcommand of rootCmd
func NewRegionCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "region <region_id>",
		Short: "show the region status",
		Run:   showRegionCommandFunc,
	}
	r.AddCommand(NewRegionWithKeyCommand())
	return r
}

func showRegionCommandFunc(cmd *cobra.Command, args []string) {
	var prefix string
	prefix = regionsPrefix
	if len(args) == 1 {
		if _, err := strconv.Atoi(args[0]); err != nil {
			fmt.Println("region_id should be a number")
			return
		}
		prefix = fmt.Sprintf(regionPrefix, args[0])
	}
	url := getAddressFromCmd(cmd, prefix)
	r, err := http.Get(url)
	if err != nil {
		fmt.Printf("Failed to get region:[%s]\n", err)
		return
	}
	defer r.Body.Close()
	if r.StatusCode != http.StatusOK {
		printResponseError(r)
		return
	}

	io.Copy(os.Stdout, r.Body)
}

// NewRegionWithKeyCommand return a region with key subcommand of regionCmd
func NewRegionWithKeyCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "key <key>",
		Short: "show the regions with key",
		Run:   showRegionWithTableCommandFunc,
	}
	return r
}

func showRegionWithTableCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		fmt.Println(cmd.UsageString())
		return
	}
	key := args[0]
	client, err := getClient()
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	regions, peers, err := client.GetRegion([]byte(key))
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}

	r := &regionInfos{
		Regions: regions,
		Leader:  peers,
	}
	infos, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		fmt.Println("Error: ", err)
		return
	}
	fmt.Println(string(infos))
}
