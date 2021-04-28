// Copyright 2016 TiKV Project Authors.
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

package pdctl

import (
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/chzyer/readline"
	"github.com/mattn/go-shellwords"
	"github.com/spf13/cobra"
	"github.com/tikv/pd/server"
	"github.com/tikv/pd/tools/pd-ctl/pdctl/command"
)

var (
	readlineCompleter *readline.PrefixCompleter
)

func init() {
	cobra.EnablePrefixMatching = true
}

func GetRootCmd() *cobra.Command {
	rootCmd := &cobra.Command{
		Use:   "pd-ctl",
		Short: "Placement Driver control",
	}

	rootCmd.PersistentFlags().StringP("pd", "u", "http://127.0.0.1:2379", "address of pd")
	rootCmd.PersistentFlags().String("cacert", "", "path of file that contains list of trusted SSL CAs")
	rootCmd.PersistentFlags().String("cert", "", "path of file that contains X509 certificate in PEM format")
	rootCmd.PersistentFlags().String("key", "", "path of file that contains X509 key in PEM format")
	rootCmd.PersistentFlags().BoolP("help", "h", false, "help message")

	rootCmd.AddCommand(
		command.NewConfigCommand(),
		command.NewRegionCommand(),
		command.NewStoreCommand(),
		command.NewStoresCommand(),
		command.NewMemberCommand(),
		command.NewExitCommand(),
		command.NewLabelCommand(),
		command.NewPingCommand(),
		command.NewOperatorCommand(),
		command.NewSchedulerCommand(),
		command.NewTSOCommand(),
		command.NewHotSpotCommand(),
		command.NewClusterCommand(),
		command.NewHealthCommand(),
		command.NewLogCommand(),
		command.NewPluginCommand(),
		command.NewServiceGCSafepointCommand(),
		command.NewCompletionCommand(),
	)

	rootCmd.Flags().ParseErrorsWhitelist.UnknownFlags = true
	rootCmd.SilenceErrors = true

	rootCmd.PersistentPreRunE = func(cmd *cobra.Command, args []string) error {
		CAPath, err := cmd.Flags().GetString("cacert")
		if err == nil && len(CAPath) != 0 {
			certPath, err := cmd.Flags().GetString("cert")
			if err != nil {
				return err
			}

			keyPath, err := cmd.Flags().GetString("key")
			if err != nil {
				return err
			}

			if err := command.InitHTTPSClient(CAPath, certPath, keyPath); err != nil {
				rootCmd.Println(err)
				return err
			}
		}
		return nil
	}

	return rootCmd
}

// MainStart start main command
func MainStart(args []string) {
	rootCmd := GetRootCmd()

	rootCmd.Flags().BoolP("interact", "i", false, "Run pdctl with readline.")
	rootCmd.Flags().BoolP("version", "V", false, "Print version information and exit.")
	// TODO: deprecated
	rootCmd.Flags().BoolP("detach", "d", true, "Run pdctl without readline.")

	rootCmd.Run = func(cmd *cobra.Command, args []string) {
		if v, err := cmd.Flags().GetBool("version"); err == nil && v {
			server.PrintPDInfo()
			return
		}
		if v, err := cmd.Flags().GetBool("interact"); err == nil && v {
			loop()
		}
	}

	rootCmd.SetArgs(args)
	rootCmd.ParseFlags(args)
	rootCmd.SetOutput(os.Stdout)

	readlineCompleter = readline.NewPrefixCompleter(genCompleter(rootCmd)...)

	if err := rootCmd.Execute(); err != nil {
		rootCmd.Println(err)
		os.Exit(1)
	}
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		AutoComplete:      readlineCompleter,
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

	rootCmd := GetRootCmd()
	rootCmd.SetOutput(os.Stdout)

	for {
		line, err := l.Readline()
		if err != nil {
			if err == readline.ErrInterrupt {
				break
			} else if err == io.EOF {
				break
			}
			continue
		}
		if line == "exit" {
			os.Exit(0)
		}
		args, err := shellwords.Parse(line)
		if err != nil {
			fmt.Printf("parse command err: %v\n", err)
			continue
		}

		rootCmd.SetArgs(args)
		rootCmd.ParseFlags(args)

		if err := rootCmd.Execute(); err != nil {
			rootCmd.Println(err)
			os.Exit(1)
		}
	}
}

func genCompleter(cmd *cobra.Command) []readline.PrefixCompleterInterface {
	pc := []readline.PrefixCompleterInterface{}

	for _, v := range cmd.Commands() {
		if v.HasFlags() {
			flagsPc := []readline.PrefixCompleterInterface{}
			flagUsages := strings.Split(strings.Trim(v.Flags().FlagUsages(), " "), "\n")
			for i := 0; i < len(flagUsages)-1; i++ {
				flagsPc = append(flagsPc, readline.PcItem(strings.Split(strings.Trim(flagUsages[i], " "), " ")[0]))
			}
			flagsPc = append(flagsPc, genCompleter(v)...)
			pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], flagsPc...))
		} else {
			pc = append(pc, readline.PcItem(strings.Split(v.Use, " ")[0], genCompleter(v)...))
		}
	}
	return pc
}
