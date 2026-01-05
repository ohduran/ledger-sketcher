package primitives

import (
	"reflect"
	"testing"
	"testing/quick"
)

func TestParseEntries(t *testing.T) {
	f := func(x uint64) bool {
		entries := []*Entry{
			&Entry{
				Direction: Credit,
				Status:    Pending,
				Value: Money{
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
				Value: Money{
					Amount: x,
					Currency: Currency{
						Code:     "USD",
						Exponent: 2,
					},
				},
			},
		}
		balancedEntries, err := ParseEntries(entries)
		return reflect.DeepEqual(BalancedEntries(entries), balancedEntries) && err == nil
	}

	if err := quick.Check(f, nil); err != nil {
		t.Error(err)
	}
}
