package primitives

type Currency string

type Money struct {
	Amount   uint64
	Currency Currency
	Exponent uint64
}
