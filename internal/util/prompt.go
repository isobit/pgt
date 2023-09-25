package util

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strings"

	"github.com/muesli/cancelreader"
)

func Prompt(ctx context.Context, prompt string) (string, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	r, err := cancelreader.NewReader(os.Stdin)
	if err != nil {
		return "", err
	}
	go func() {
		<-ctx.Done()
		r.Cancel()
	}()

	fmt.Fprintf(os.Stderr, prompt+" ")

	scanner := bufio.NewScanner(r)
	if scanner.Scan() {
		ans := scanner.Text()
		if ans != "" {
			return ans, nil
		}
	}

	err = scanner.Err()
	if err == cancelreader.ErrCanceled {
		return "", ctx.Err()
	}
	return "", err
}

func PromptYesNo(ctx context.Context, question string) (bool, error) {
	for {
		ans, err := Prompt(ctx, question+" [y/N]")
		if err != nil {
			return false, err
		}
		switch strings.TrimSpace(ans) {
		case "y", "Y":
			return true, nil
		case "n", "N":
			return false, nil
		}
	}
}
