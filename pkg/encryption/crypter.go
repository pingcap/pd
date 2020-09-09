// Copyright 2020 TiKV Project Authors.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package encryption

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"encoding/binary"
	"io"
	"time"

	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/pkg/errors"
)

const (
	ivLengthCTR = 16
	ivLengthGCM = 12
)

// CheckEncryptionMethodSupported check whether the encryption method is currently supported.
// This is to handle future extension to encryption methods on kvproto side.
func CheckEncryptionMethodSupported(method encryptionpb.EncryptionMethod) error {
	switch method {
	case encryptionpb.EncryptionMethod_AES128_CTR:
		return nil
	case encryptionpb.EncryptionMethod_AES192_CTR:
		return nil
	case encryptionpb.EncryptionMethod_AES256_CTR:
		return nil
	default:
		name, ok := encryptionpb.EncryptionMethod_name[int32(method)]
		if ok {
			return errors.Errorf("invalid encryption method %s", name)
		}
		return errors.Errorf("invalid encryption method %d", int32(method))
	}
}

// KeyLength return the encryption key lenght for supported encryption methods.
func KeyLength(method encryptionpb.EncryptionMethod) int {
	switch method {
	case encryptionpb.EncryptionMethod_AES128_CTR:
		return 16
	case encryptionpb.EncryptionMethod_AES192_CTR:
		return 24
	case encryptionpb.EncryptionMethod_AES256_CTR:
		return 32
	default:
		panic("unsupported encryption method")
	}
}

// IvCtr represent IV bytes for CTR mode.
type IvCtr []byte

// IvGcm represent IV bytes for GCM mode.
type IvGcm []byte

func newIV(ivLength int) ([]byte, error) {
	iv := make([]byte, ivLength)
	n, err := io.ReadFull(rand.Reader, iv)
	if err != nil {
		return nil, errors.Wrap(err, "fail to generate iv")
	}
	if n != ivLength {
		return nil, errors.New("no enough random bytes to generate iv")
	}
	return iv, nil
}

// NewIvCtr randomly generate an IV for CTR mode.
func NewIvCtr() (IvCtr, error) {
	return newIV(ivLengthCTR)
}

// NewIvCtr randomly generate an IV for GCM mode.
func NewIvGcm() (IvGcm, error) {
	return newIV(ivLengthGCM)
}

// NewDataKey randomly generate a new data key.
func NewDataKey(
	method encryptionpb.EncryptionMethod,
) (keyId uint64, key *encryptionpb.DataKey, err error) {
	err = CheckEncryptionMethodSupported(method)
	if err != nil {
		return
	}
	keyIdBuf := make([]byte, 8)
	n, err := io.ReadFull(rand.Reader, keyIdBuf)
	if err != nil {
		err = errors.Wrap(err, "fail to generate data key id")
		return
	}
	if n != 8 {
		err = errors.New("no enough random bytes to generate data key id")
		return
	}
	keyId = binary.BigEndian.Uint64(keyIdBuf)
	keyLength := KeyLength(method)
	keyBuf := make([]byte, keyLength)
	n, err = io.ReadFull(rand.Reader, keyBuf)
	if err != nil {
		err = errors.Wrap(err, "fail to generate data key")
		return
	}
	if n != keyLength {
		err = errors.New("no enough random bytes to generate data key")
		return
	}
	key = &encryptionpb.DataKey{
		Key:          keyBuf,
		Method:       method,
		CreationTime: uint64(time.Now().Unix()),
		WasExposed:   false,
	}
	return
}

func aesGcmEncryptImpl(
	key []byte,
	plaintext []byte,
	iv IvGcm,
) (ciphertext []byte, err error) {
	block, err := aes.NewCipher(key)
	if err != nil {
		err = errors.Wrap(err, "fail to create aes cipher")
		return
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		err = errors.Wrap(err, "fail to create aes-gcm cipher")
		return
	}
	ciphertext = aesgcm.Seal(nil, iv, plaintext, nil)
	return
}

// AesGcmEncrypt encrypt given plaintext with given key using aes256-gcm.
// The method is used to encrypt data keys.
func AesGcmEncrypt(
	key []byte,
	plaintext []byte,
) (ciphertext []byte, iv IvGcm, err error) {
	iv, err = NewIvGcm()
	if err != nil {
		return
	}
	ciphertext, err = aesGcmEncryptImpl(key, plaintext, iv)
	return
}

// AesGcmEncrypt encrypt given plaintext with given key using aes256-gcm.
// The method is used to decrypt data keys.
func AesGcmDecrypt(
	key []byte,
	ciphertext []byte,
	iv IvGcm,
) (plaintext []byte, err error) {
	if len(iv) != ivLengthGCM {
		err = errors.Errorf("unexpected gcm iv length %d", len(iv))
		return
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		err = errors.Wrap(err, "fail to create aes cipher")
		return
	}
	aesgcm, err := cipher.NewGCM(block)
	if err != nil {
		err = errors.Wrap(err, "fail to create aes-gcm cipher")
		return
	}
	plaintext, err = aesgcm.Open(nil, iv, ciphertext, nil)
	if err != nil {
		err = errors.Wrap(err, "authentication fail")
		return
	}
	return
}
