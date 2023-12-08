package functions

import (
	"fmt"

	"github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

type ArgTypes int
type Reader func(arg any) error

const (
	STRING ArgTypes = iota
	STRINGVALUE
	NUMBER
	NUMBERVALUE
	BOOL
	ANY
)

func CheckSingnature(args []any, argTypes []ArgTypes, readers []Reader) error {
	if len(args)^len(argTypes)^len(readers) != len(args) {
		return sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("expected %d arguments and readers but received %d argument and %d readers", len(argTypes), len(args), len(readers)))
	}
	for index, item := range args {
		switch argTypes[index] {
		case STRING:
			{
				str, ok := item.(string)
				if !ok {
					return sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("expected string but received %T", item))
				}
				err := readers[index](str)
				if err != nil {
					return err
				}
			}
		case STRINGVALUE:
			{
				str, ok := item.(common.StringValue)
				if !ok {
					return sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("expected StringValue but received %T", item))
				}
				err := readers[index](str)
				if err != nil {
					return err
				}
			}
		case NUMBER:
			{
				number, ok := item.(float64)
				if !ok {
					return sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("expected float64 but received %T", item))
				}
				err := readers[index](number)
				if err != nil {
					return err
				}
			}
		case BOOL:
			{
				boolean, ok := item.(bool)
				if !ok {
					return sentinel.EXPECTATION_FAILED.Extend(fmt.Sprintf("expected boolean but received %T", item))
				}
				err := readers[index](boolean)
				if err != nil {
					return err
				}
			}
		default:
			{
				err := readers[index](item)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
