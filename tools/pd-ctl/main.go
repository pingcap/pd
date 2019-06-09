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

package main

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/chzyer/readline"
	shellwords "github.com/mattn/go-shellwords"
	"github.com/pingcap/pd/server"
	"github.com/pingcap/pd/tools/pd-ctl/pdctl"
	flag "github.com/spf13/pflag"
)

var (
	url      string
	detach   bool
	interact bool
	version  bool
	caPath   string
	certPath string
	keyPath  string
)

func init() {
	flag.StringVarP(&url, "pd", "u", "http://127.0.0.1:2379", "The pd address")
	flag.BoolVarP(&detach, "detach", "d", true, "Run pdctl without readline")
	flag.BoolVarP(&interact, "interact", "i", false, "Run pdctl with readline")
	flag.BoolVarP(&version, "version", "V", false, "print version information and exit")
	flag.StringVar(&caPath, "cacert", "", "path of file that contains list of trusted SSL CAs.")
	flag.StringVar(&certPath, "cert", "", "path of file that contains X509 certificate in PEM format.")
	flag.StringVar(&keyPath, "key", "", "path of file that contains X509 key in PEM format.")
}

func main() {
	pdAddr := os.Getenv("PD_ADDR")
	if pdAddr != "" {
		os.Args = append(os.Args, "-u", pdAddr)
	}
	flag.CommandLine.ParseErrorsWhitelist.UnknownFlags = true
	flag.Parse()

	if version {
		server.PrintPDInfo()
		os.Exit(0)
	}

	sc := make(chan os.Signal, 1)
	signal.Notify(sc,
		syscall.SIGHUP,
		syscall.SIGINT,
		syscall.SIGTERM,
		syscall.SIGQUIT)

	go func() {
		sig := <-sc
		fmt.Printf("\nGot signal [%v] to exit.\n", sig)
		switch sig {
		case syscall.SIGTERM:
			os.Exit(0)
		default:
			os.Exit(1)
		}
	}()
	var input []string
	stat, _ := os.Stdin.Stat()
	if (stat.Mode() & os.ModeCharDevice) == 0 {
		detach = true
		b, err := ioutil.ReadAll(os.Stdin)
		if err != nil {
			fmt.Println(err)
			return
		}
		input = strings.Split(strings.TrimSpace(string(b[:])), " ")
	}
	if interact {
		loop()
		return
	}
	pdctl.Start(append(os.Args[1:], input...))
}

func loop() {
	l, err := readline.NewEx(&readline.Config{
		Prompt:            "\033[31m»\033[0m ",
		HistoryFile:       "/tmp/readline.tmp",
		InterruptPrompt:   "^C",
		EOFPrompt:         "^D",
		HistorySearchFold: true,
	})
	if err != nil {
		panic(err)
	}
	defer l.Close()

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
		args = append(args, "-u", url)
		if caPath != "" && certPath != "" && keyPath != "" {
			args = append(args, "--cacert", caPath, "--cert", certPath, "--key", keyPath)
		}
		pdctl.Start(args)
	}
}
