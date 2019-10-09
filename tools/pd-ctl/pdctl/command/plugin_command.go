package command

import (
	"github.com/spf13/cobra"
)

var (
	pluginPrefix = "pd/api/v1/plugin"
	loadPrefix   = "pd/api/v1/plugin/load"
	updatePrefix = "pd/api/v1/plugin/update"
	unloadPrefix = "pd/api/v1/plugin/unload"
)

// NewPluginCommand a set subcommand of plugin command
func NewPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "plugin <subcommand>",
		Short: "plugin commands",
	}
	r.AddCommand(NewLoadPluginCommand())
	r.AddCommand(NewUpdatePluginCommand())
	r.AddCommand(NewUnloadPluginCommand())
	return r
}

// NewLoadPluginCommand return a load subcommand of plugin command
func NewLoadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "load <plugin_path> <config_path>",
		Short: "load a plugin with a user defined config file",
		Run:   loadPluginCommandFunc,
	}
	return r
}

// NewUpdatePluginCommand return a update subcommand of plugin command
func NewUpdatePluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "update <plugin_path> <config_path>",
		Short: "update plugin with a user defined config file",
		Run:   updatePluginCommandFunc,
	}
	return r
}

// NewUnloadPluginCommand return a unload subcommand of plugin command
func NewUnloadPluginCommand() *cobra.Command {
	r := &cobra.Command{
		Use:   "unload <plugin_path>",
		Short: "unload a plugin",
		Run:   unloadPluginCommandFunc,
	}
	return r
}

func loadPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
		"config-path": args[1],
	}
	postJSON(cmd, loadPrefix, input)
}

func updatePluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 2 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
		"config-path": args[1],
	}
	postJSON(cmd, updatePrefix, input)
}

func unloadPluginCommandFunc(cmd *cobra.Command, args []string) {
	if len(args) != 1 {
		cmd.Println(cmd.UsageString())
		return
	}
	input := map[string]interface{}{
		"plugin-path": args[0],
	}
	postJSON(cmd, unloadPrefix, input)
}
