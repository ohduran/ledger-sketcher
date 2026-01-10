package primitives

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestParseBalanceEntries(t *testing.T) {
	f := func(x uint64) bool {
		entries := []*Entry{
			&Entry{
				Direction: Credit,
				Status:    Pending,
				Value: MoneyWithPositiveAmount{
					Amount: x,
					Currency: Currency{
						Code:     "USD",
						Exponent: 2,
					},
				},
			},
			&Entry{
				Direction: Debit,
				Status:    Pending,
				Value: MoneyWithPositiveAmount{
					Amount: x,
					Currency: Currency{
						Code:     "USD",
						Exponent: 2,
					},
				},
			},
		}
		balancedEntries, err := ParseBalanceEntries(entries)
		return reflect.DeepEqual(BalancedEntries(entries), balancedEntries) && err == nil
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}

func TestPostedBalanceCanBeNegative(t *testing.T) {
	// Test that an Asset account (NormalDebit) can have a negative balance
	// when credits exceed debits
	usd := Currency{Code: "USD", Exponent: 2}

	entries := EntriesWithSameCurrency([]*Entry{
		&Entry{
			Direction: Credit,
			Status:    Posted,
			Value: MoneyWithPositiveAmount{
				Amount:   1000,
				Currency: usd,
			},
		},
		&Entry{
			Direction: Debit,
			Status:    Posted,
			Value: MoneyWithPositiveAmount{
				Amount:   500,
				Currency: usd,
			},
		},
	})

	account := Account[AccountKind]{
		Name:     "Cash",
		Entries:  entries,
		Currency: usd,
		Kind:     Asset,
	}

	balance := account.PostedBalance()

	// For an Asset account (NormalDebit), balance = debits - credits
	// balance = 500 - 1000 = -500
	if balance.Amount != -500 {
		t.Errorf("Expected balance of -500, got %d", balance.Amount)
	}
}

func TestPostedBalanceWithPositiveValues(t *testing.T) {
	// Test that Entry amounts are always positive (uint64)
	// but PostedBalance can calculate negative balances (int64)
	usd := Currency{Code: "USD", Exponent: 2}

	entries := EntriesWithSameCurrency([]*Entry{
		&Entry{
			Direction: Debit,
			Status:    Posted,
			Value: MoneyWithPositiveAmount{
				Amount:   1000,
				Currency: usd,
			},
		},
		&Entry{
			Direction: Credit,
			Status:    Posted,
			Value: MoneyWithPositiveAmount{
				Amount:   300,
				Currency: usd,
			},
		},
	})

	account := Account[AccountKind]{
		Name:     "Cash",
		Entries:  entries,
		Currency: usd,
		Kind:     Asset,
	}

	balance := account.PostedBalance()

	// For an Asset account (NormalDebit), balance = debits - credits
	// balance = 1000 - 300 = 700
	if balance.Amount != 700 {
		t.Errorf("Expected balance of 700, got %d", balance.Amount)
	}
}
