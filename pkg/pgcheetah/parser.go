package pgcheetah

import (
	"bufio"
	"io"
	"log"
	"os"
	"regexp"
	"strings"
)

type State struct {
	Statedesc      string
	Xact           int
	Xactinprogress bool
}

type Txtlinetyp struct {
	strings.Builder
}

// state machine to identify new transactions
func (s *State) NewState(action string) int {
	switch s.Statedesc {
	case "init":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.Xactinprogress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "query":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.Xactinprogress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
			s.Xact++
		case "CommitRollback":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "multi line query":
		switch action {
		case "Begin":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			if s.Xactinprogress {
				s.Statedesc = "Xact in progress"
			} else {
				s.Statedesc = "query"
			}
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "new Xact":
		switch action {
		case "Begin":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			s.Statedesc = "Xact in progress"
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			s.Statedesc = "end Xact"
			s.Xactinprogress = false
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "Xact in progress":
		switch action {
		case "Begin":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		case "Query":
			s.Statedesc = "Xact in progress"
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			s.Statedesc = "end Xact"
			s.Xactinprogress = false
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "end Xact":
		switch action {
		case "Begin":
			s.Statedesc = "new Xact"
			s.Xact++
			s.Xactinprogress = true
		case "Query":
			s.Statedesc = "query"
			s.Xact++
		case "MultilineQuery":
			s.Statedesc = "multi line query"
		case "CommitRollback":
			log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
			s.Statedesc = "error"
		default:
			log.Fatalf("Unkown action")
			s.Statedesc = "error"
		}
	case "error":
		log.Fatalf("Action %s bring from state %s to error state", action, s.Statedesc)
		s.Statedesc = "error"
	default:
		log.Fatalf("Unkown state")
		s.Statedesc = "error"
	}
	return s.Xact
}

// Method which identify statement type. return statement type and a boolean
// at false to ignore the line

var (
	begin, _             = regexp.Compile(`(?i)^ *BEGIN`)
	commit, _            = regexp.Compile(`(?i)^ *COMMIT`)
	rollback, _          = regexp.Compile(`(?i)^ *ROLLBACK`)
	rollbackSavePoint, _ = regexp.Compile(`(?i)^ *ROLLBACK TO SAVEPOINT`)
	query, _             = regexp.Compile(`.*;[ ]*(--.*)?$`)
	blanckLine, _        = regexp.Compile(`^ *$`)
)

func (t *Txtlinetyp) EvaluateLine() (string, bool) {

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

func ParseXact(data map[int][]string, queryfile *string, s *State, debug *bool) int {

	var txtline Txtlinetyp
	var prevstate string = ""
	var xact int

	file, err := os.Open(*queryfile)
	if err != nil {
		log.Fatalf("failed opening file: %s", err)
	}

	reader := bufio.NewReader(file)

	for {
		line, isPrefix, err := reader.ReadLine()

		txtline.WriteString(string(line))

		// Here is a trick to concatenane line which are too big for the buffer.
		// We loop and concatenate string util we reach the end of the file line.
		if !isPrefix {
			// We reached end of line or the line fitted in the buffer.
			if *debug {
				log.Println("Query:", string(line))
			}
			stmtype, isvalid := txtline.EvaluateLine()
			if isvalid {
				xact = s.NewState(stmtype)

				// concatenate line when we have multi line query
				if prevstate == "multi line query" {
					data[xact][len(data[xact])-1] += " " + txtline.String()

				} else {
					data[xact] = append(data[xact], txtline.String())
				}
				prevstate = s.Statedesc
			}
			txtline.Reset()
		}
		if err == io.EOF {
			break
		}
	}

	file.Close()
	return xact

}
