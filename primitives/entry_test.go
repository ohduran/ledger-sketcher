package primitives

import (
	"reflect"
	"testing"
)

func TestParseEntries(t *testing.T) {
	balancedEntries := []*Entry{
		&Entry{
			Direction: Credit,
			Status:    Pending,
			Value: Money{
				Amount:   10,
				Currency: "USD",
				Exponent: 1,
			},
		},
		&Entry{
			Direction: Debit,
			Status:    Pending,
			Value: Money{
				Amount:   10,
				Currency: "USD",
				Exponent: 1,
			},
		},
	}

	entries, err := ParseEntries(balancedEntries)

	if err != nil {
		t.Errorf("there was an error: %v", err)
	}

	if entries == nil {
		t.Errorf("entries are nil")
	}
	if !reflect.DeepEqual(entries, balancedEntries) {
		t.Errorf("entries don't match")
	}
}
