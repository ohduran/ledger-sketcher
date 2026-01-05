package primitives

type Currency struct {
	Code     string // ISO 4217 Currency Code
	Exponent uint64 // The number of digits after the decimal separator
}

type Money struct {
	Amount   uint64
	Currency Currency
}
