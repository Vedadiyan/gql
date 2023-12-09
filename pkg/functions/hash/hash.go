package hash

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	cmn "github.com/vedadiyan/gql/pkg/common"
	"github.com/vedadiyan/gql/pkg/functions"
	"github.com/vedadiyan/gql/pkg/functions/common"
	"github.com/vedadiyan/gql/pkg/sentinel"
)

func Hash(jo *[]any, row any, args []any) (any, error) {
	data, hashFunction, err := readArgs(args, row, jo)
	if err != nil {
		return nil, err
	}
	switch strings.ToLower(hashFunction) {
	case "sha256":
		{
			sha256 := sha256.New()
			_, err := sha256.Write(data)
			if err != nil {
				return nil, err
			}
			hash := sha256.Sum(nil)
			return common.ToStringValue(hex.EncodeToString(hash)), nil
		}
	case "md5":
		{
			md5 := md5.New()
			_, err := md5.Write(data)
			if err != nil {
				return nil, err
			}
			hash := md5.Sum(nil)
			return common.ToStringValue(hex.EncodeToString(hash)), nil
		}
	default:
		{
			return nil, sentinel.UNSUPPORTED_CASE.Extend(fmt.Sprintf("%s is not supported", hashFunction))
		}
	}
}

func readArgs(args []any, row any, jo *[]any) ([]byte, string, error) {
	var fnArg []byte
	var hashFunction string
	err := functions.CheckSingnature(
		args,
		[]functions.ArgTypes{
			functions.ANY,
			functions.STRINGVALUE,
		},
		[]functions.Reader{
			func(arg any) error {
				val, err := common.Select(arg, row)
				if err != nil {
					return err
				}
				bytes, err := json.Marshal(val)
				if err != nil {
					return err
				}
				fnArg = bytes
				return nil
			},
			func(arg any) error {
				hashFunction = string(arg.(cmn.StringValue))
				return nil
			},
		},
	)
	if err != nil {
		return nil, "", err
	}
	return fnArg, hashFunction, nil
}

func init() {
	cmn.RegisterFunction("hash", Hash)
}
