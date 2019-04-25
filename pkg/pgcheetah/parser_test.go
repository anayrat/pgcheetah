package pgcheetah

import "testing"
import "flag"

var queryfile = flag.String("queryfile", "", "file containing queries to play")

func TestNewstate(t *testing.T) {
	var tests = []struct {
		stateIn  State
		action   string
		stateExp State
	}{

		{State{"init", 0, false}, "Begin", State{"new Xact", 1, true}},
		{State{"init", 0, false}, "Query", State{"query", 1, false}},
		{State{"init", 0, false}, "MultilineQuery", State{"multi line query", 0, false}},
		{State{"init", 0, false}, "CommitRollback", State{"error", 0, false}},
		{State{"init", 0, false}, "other", State{"error", 0, false}},

		{State{"query", 0, false}, "Begin", State{"new Xact", 1, true}},
		{State{"query", 0, false}, "Query", State{"query", 1, false}},
		{State{"query", 0, false}, "MultilineQuery", State{"multi line query", 1, false}},
		{State{"query", 0, false}, "CommitRollback", State{"error", 0, false}},
		{State{"query", 0, false}, "other", State{"error", 0, false}},

		{State{"multi line query", 0, false}, "Begin", State{"error", 0, false}},
		{State{"multi line query", 0, true}, "Query", State{"Xact in progress", 0, true}},
		{State{"multi line query", 0, false}, "Query", State{"query", 0, false}},
		{State{"multi line query", 0, false}, "MultilineQuery", State{"multi line query", 0, false}},
		{State{"multi line query", 0, false}, "CommitRollback", State{"error", 0, false}},
		{State{"multi line query", 0, false}, "other", State{"error", 0, false}},

		{State{"new Xact", 0, false}, "Begin", State{"error", 0, false}},
		{State{"new Xact", 0, false}, "Query", State{"Xact in progress", 0, false}},
		{State{"new Xact", 0, false}, "MultilineQuery", State{"multi line query", 0, false}},
		{State{"new Xact", 0, false}, "CommitRollback", State{"end Xact", 0, false}},
		{State{"new Xact", 0, false}, "other", State{"error", 0, false}},

		{State{"Xact in progress", 0, false}, "Begin", State{"error", 0, false}},
		{State{"Xact in progress", 0, false}, "Query", State{"Xact in progress", 0, false}},
		{State{"Xact in progress", 0, false}, "MultilineQuery", State{"multi line query", 0, false}},
		{State{"Xact in progress", 0, false}, "CommitRollback", State{"end Xact", 0, false}},
		{State{"Xact in progress", 0, false}, "other", State{"error", 0, false}},

		{State{"end Xact", 0, false}, "Begin", State{"new Xact", 1, true}},
		{State{"end Xact", 0, false}, "Query", State{"query", 1, false}},
		{State{"end Xact", 0, false}, "MultilineQuery", State{"multi line query", 0, false}},
		{State{"end Xact", 0, false}, "CommitRollback", State{"error", 0, false}},
		{State{"end Xact", 0, false}, "other", State{"error", 0, false}},

		{State{"error", 0, false}, "other", State{"error", 0, false}},

		{State{"ewrong state", 0, false}, "other", State{"error", 0, false}},
	}

	for i, test := range tests {
		_, err := test.stateIn.newState(test.action)
		if err != nil {
			t.Error("Error during parsing ", err)
		}
		if test.stateExp != test.stateIn {
			t.Error("Test TestNewstate #", i, "Expected state ", test.stateExp, " got ", test.stateIn)
		}

	}

}

func TestEvaluateLine(t *testing.T) {

	var tests = []struct {
		in       string
		expected string
	}{
		{" Begin", "Begin"},
		{" Select 1;", "Query"},
		{" ROLLBACK TO SAVEPOINT", "Query"},
		{" Commit", "CommitRollback"},
		{" Rollback", "CommitRollback"},
		{"  ", "BlankLine"},
		{" FROM multiline", "MultilineQuery"},
	}

	for i, test := range tests {
		var line txtLineTyp
		line.WriteString(test.in)

		if v, _ := line.evaluateLine(); test.expected != v {
			t.Error("Test TestEvaluateLine #", i, "Expected state ", test.expected, " got ", test.in)
		}
	}
}

func TestParseXact(t *testing.T) {

	var data = make(map[int][]string, 100000)
	s := State{Statedesc: "init", Xact: 0, XactInProgress: false}
	debug := true
	if *queryfile == "" {
		t.Error("Provide query file with -queryfile option")
	}
	_, err := ParseXact(data, queryfile, &s, &debug)

	if err != nil {
		t.Error("Error during parsing ", err)
	}

}
