package primitives

import "fmt"

type EntryStatus int

const (
	Pending EntryStatus = iota
	Posted
	Archived
)

type EntryDirection int

const (
	Credit EntryDirection = iota
	Debit
)

type Entry struct {
	Direction EntryDirection
	Status    EntryStatus
	Value     Money
}

type BalancedEntries []*Entry

// Verify that entries' cumulative value (by currency) is the same for credit and debit entries in the entry list
func ParseEntries(entries []*Entry) ([]*Entry, error) {

	entriesByCurrency := make(map[Currency]map[EntryDirection]uint64)

	for _, entry := range entries {
		mm, ok := entriesByCurrency[entry.Value.Currency]
		if !ok {
			mm = make(map[EntryDirection]uint64)
			entriesByCurrency[entry.Value.Currency] = mm
		}
		mm[entry.Direction] += entry.Value.Amount
	}

	for _, currency := range entriesByCurrency {
		if currency[Credit] != currency[Debit] {
			return nil, fmt.Errorf("entries are not balanced")
		}
	}

	return entries, nil
}
