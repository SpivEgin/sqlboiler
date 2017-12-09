package bdb

type TableConversion struct {
	Column []string
	Row []string
	Type []string
	Value []string
	Convert []interface{}
}

type TableConverter interface {

}