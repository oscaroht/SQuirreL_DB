package main

import (
	"fmt"
	"os"
	"strings"

	"log/slog"

	prompt "github.com/c-bata/go-prompt"
)

// check this out for package free cli
// https://dev.to/tidalcloud/interactive-cli-prompts-in-go-3bj9

func completer(d prompt.Document) []prompt.Suggest {
	suggestions := []prompt.Suggest{}
	return prompt.FilterHasPrefix(suggestions, d.GetWordBeforeCursor(), true)
}

func printFormattedResponse(result *QueryResult) {
	println("*********************************************\n")
	fmt.Printf("%v\n\n", result.sql)
	// println("-----------------------------------------")
	if result.table != nil && result.columns != nil {
		for _, col := range result.columns {
			fmt.Printf("|%v", col)
		}
		print("|\n")
		// fmt.Printf("|%v|\n", cols[0].ColumnName)
		println("-----------------------------------------")
		for rowIdx := range len(result.table[0]) {
			for _, col := range result.table {
				fmt.Printf("| %v ", col[rowIdx])
			}
			fmt.Printf("\n")
		}
	}
	println("-----------------------------------------")
	print("Message: ")
	println(result.message)
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
		case ".new":
			err := os.WriteFile("squirrel.db", []byte(""), 0755)
			if err != nil {
				fmt.Printf("unable to write file: %w", err)
			}
		case ".flush":
			bm.flush()
		case ".setloglevel 1":
			slog.SetLogLoggerLevel(slog.LevelDebug)
			slog.Debug("Log level set to", "loglevel", 1)
		case ".setloglevel 2":
			slog.SetLogLoggerLevel(slog.LevelInfo)
			slog.Info("Log level set to", "loglevel", 2)
		case ".setloglevel 3":
			slog.SetLogLoggerLevel(slog.LevelWarn)
			slog.Warn("Log level set to", "loglevel", 3)
		default:
			result, error := execute_sql(s)
			if error != nil {
				slog.Error(error.Error())
			} else {
				printFormattedResponse(result)
			}

		}

	}
}

func StartPromt(file string) {
	p := prompt.New(getExecutor(file), completer)
	p.Run()
}
