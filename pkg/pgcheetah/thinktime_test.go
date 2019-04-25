package pgcheetah

import "testing"

func TestThinkTimer(t *testing.T) {

	test := ThinkTimer(ThinkTime{Distribution: "uniform", Min: 2, Max: 5})
	if test < 2 || test > 5 {
		t.Error("Expected value between 2 and 5, got", test)
	}

	test = ThinkTimer(ThinkTime{Distribution: "wrong", Min: 2, Max: 5})
	if test != 0 {
		t.Error("Expected 0, got", test)
	}
}
