package main

import (
	"fmt"
	"os"
	"strings"

	prompt "github.com/c-bata/go-prompt"
)

// check this out for package free cli
// https://dev.to/tidalcloud/interactive-cli-prompts-in-go-3bj9

func completer(d prompt.Document) []prompt.Suggest {
	suggestions := []prompt.Suggest{}
	return prompt.FilterHasPrefix(suggestions, d.GetWordBeforeCursor(), true)
}

func getExecutor(file string) func(string) {
	return func(s string) {
		s = strings.TrimSpace(s)
		s = strings.ToLower(s)

		switch s {
		case "":
			return
		case ".quit", ".exit":
			fmt.Print("Goodbye!\n")
			os.Exit(0)
		default:
			execute_sql(s)
		}

	}
}

func StartPromt(file string) {
	p := prompt.New(getExecutor(file), completer)
	p.Run()
}
