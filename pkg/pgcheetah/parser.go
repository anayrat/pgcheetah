package pgcheetah

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

// State structure is used for the state machine. Statedesc is the current state.
// Xact count transactions. XactInProgress is used in the state machine to identify
// transactions.
type State struct {
	Statedesc      string
	Xact           int
	XactInProgress bool
}

type txtLineTyp struct {
	strings.Builder
}

// newState function is a state machine used to identify new transactions or
// queries (when there are multi line string). It is determined according to
// previous states.
func (s *State) newState(action string) (int, error) {
	var err error
	switch s.Statedesc {
	case "init":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.XactInProgress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "query":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.XactInProgress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
			s.Xact++
		case "CommitRollback":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "multi line query":
		switch action {
		case "Begin":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			if s.XactInProgress {
				s.Statedesc = "Xact in progress"
			} else {
				s.Statedesc = "query"
			}
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "new Xact":
		switch action {
		case "Begin":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			s.Statedesc = "Xact in progress"
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			s.Statedesc = "end Xact"
			s.XactInProgress = false
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "Xact in progress":
		switch action {
		case "Begin":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			s.Statedesc = "Xact in progress"
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			s.Statedesc = "end Xact"
			s.XactInProgress = false
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "end Xact":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.XactInProgress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			err = fmt.Errorf("unknown action ")
			s.Statedesc = "error"
		}
	case "error":
		err = fmt.Errorf("action %s bring from state %s to error state ", action, s.Statedesc)
		s.Statedesc = "error"
	default:
		err = fmt.Errorf("Unknown state ")
		s.Statedesc = "error"
	}
	return s.Xact, err
}

var (
	begin, _             = regexp.Compile(`(?i)^ *BEGIN`)
	commit, _            = regexp.Compile(`(?i)^ *COMMIT`)
	rollback, _          = regexp.Compile(`(?i)^ *ROLLBACK`)
	rollbackSavePoint, _ = regexp.Compile(`(?i)^ *ROLLBACK TO SAVEPOINT`)
	query, _             = regexp.Compile(`.*;[ ]*(--.*)?$`)
	blanckLine, _        = regexp.Compile(`^ *$`)
)

// evaluateLine identify if the input string is a begin, commit or rollback,
// singleline query or a part of a multiline query.
// It do not use a real parser, but several regex, it is a naive implementation.

func (t *txtLineTyp) evaluateLine() (string, bool) {

	if begin.MatchString(t.String()) {
		return "Begin", true
	}
	if rollbackSavePoint.MatchString(t.String()) {
		return "Query", true
	}
	if commit.MatchString(t.String()) {
		return "CommitRollback", true
	}
	if rollback.MatchString(t.String()) {
		return "CommitRollback", true
	}
	// Now we've catched everything except query
	if query.MatchString(t.String()) {
		return "Query", true
	}
	// Blank lines are ignored
	if blanckLine.MatchString(t.String()) {
		return "BlankLine", false
	}
	return "MultilineQuery", true
}

// ParseXact read a queryFile line by line and load all transaction in a data map.
// It returns the number of transactions processed.
// Note : it can handle very long lines which used to be longer than buffer.
func ParseXact(data map[int][]string, queryFile *string, s *State, debug *bool) (int, error) {

	var txtLine txtLineTyp
	var prevState string
	var xact int

	file, err := os.Open(*queryFile)
	if err != nil {
		return 0, err
	}

	reader := bufio.NewReader(file)

	for {
		line, isPrefix, err := reader.ReadLine()

		txtLine.WriteString(string(line))

		// Here is a trick to concatenane line which are too big for the buffer.
		// We loop and concatenate string util we reach the end of the file line.
		if !isPrefix {
			// We reached end of line or the line fitted in the buffer.
			if *debug {
				log.Println("Query:", string(line))
			}
			stmtype, isvalid := txtLine.evaluateLine()
			if isvalid {
				xact, err = s.newState(stmtype)
				if err != nil {
					return xact, err
				}

				// concatenate line when we have multi line query
				if prevState == "multi line query" {
					data[xact][len(data[xact])-1] += " " + txtLine.String()

				} else {
					data[xact] = append(data[xact], txtLine.String())
				}
				prevState = s.Statedesc
			}
			txtLine.Reset()
		}
		if err == io.EOF {
			break
		}
	}

	err = file.Close()
	return xact, err

}
