package aes

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/md5"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"

	cmn "github.com/vedadiyan/gql/pkg/common"
)

type (
	AES struct {
		encryptor cipher.BlockMode
		decryptor cipher.BlockMode
	}
)

func New(key []byte, iv []byte) (*AES, error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	encryptor := cipher.NewCBCEncrypter(block, iv)
	decryptor := cipher.NewCBCDecrypter(block, iv)
	aes := AES{
		encryptor: encryptor,
		decryptor: decryptor,
	}
	return &aes, nil
}
func (aes *AES) Encrypt(bytes []byte) []byte {
	initLen := len(bytes)
	remainder := 256 - (initLen % 256)
	if remainder == 0 {
		remainder = 256
	}
	len := initLen + (remainder)
	dest := make([]byte, len)
	copy := append(bytes, dest[initLen+1:]...)
	copy = append(copy, byte(remainder-1))
	aes.encryptor.CryptBlocks(dest, copy)
	return dest
}

func (aes *AES) Decrypt(bytes []byte) []byte {
	dest := make([]byte, len(bytes))
	aes.decryptor.CryptBlocks(dest, bytes)
	remainder := int(dest[len(dest)-1])
	return dest[:len(dest)-remainder-1]
}

func AESFN(jo *[]any, row any, args []any) (any, error) {
	if len(args) != 2 {
		return nil, fmt.Errorf("invalid arguments")
	}
	obj := args[0]
	secret, ok := args[1].(string)

	if !ok {
		return nil, fmt.Errorf("invalid arguments")
	}

	md5 := md5.New()
	_, err := md5.Write([]byte(secret))
	if err != nil {
		return nil, err
	}
	iv := md5.Sum(nil)

	sha256 := sha256.New()
	_, err = sha256.Write([]byte(secret))
	if err != nil {
		return nil, err
	}
	key := sha256.Sum(nil)

	aes, err := New(key, iv)
	if err != nil {
		return nil, err
	}
	json, err := json.Marshal(obj)
	if err != nil {
		return nil, err
	}
	encrypted := aes.Encrypt(json)
	return base64.StdEncoding.EncodeToString(encrypted), nil
}

func init() {
	cmn.RegisterFunction("aes", AESFN)
}
