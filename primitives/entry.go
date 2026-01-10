package primitives

import (
	"fmt"
	"reflect"
)

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
	Value     MoneyWithPositiveAmount
}

type BalancedEntries []*Entry
type EntriesWithSameCurrency []*Entry

// Verify that entries' cumulative value (by currency) is the same for credit and debit entries in the entry list
func ParseBalanceEntries(entries []*Entry) (BalancedEntries, error) {
	type balance struct {
		credit uint64
		debit  uint64
	}

	balances := make(map[Currency]balance)

	for _, entry := range entries {
		b := balances[entry.Value.Currency]
		if entry.Direction == Credit {
			b.credit += entry.Value.Amount
		} else {
			b.debit += entry.Value.Amount
		}
		balances[entry.Value.Currency] = b
	}

	for _, b := range balances {
		if b.credit != b.debit {
			return nil, fmt.Errorf("entries are not balanced")
		}
	}

	return BalancedEntries(entries), nil
}

func ParseEntriesWithSameCurrency(entries []*Entry) (EntriesWithSameCurrency, error) {
	var currency Currency = entries[0].Value.Currency
	for _, entry := range entries {
		if !reflect.DeepEqual(entry.Value.Currency, currency) {
			return nil, fmt.Errorf("entries don't have the same currency")
		}
	}

	return EntriesWithSameCurrency(entries), nil
}
