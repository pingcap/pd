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
	"encoding/hex"
	"io/ioutil"
	"strings"

	"github.com/pingcap/errors"
	"github.com/pingcap/kvproto/pkg/encryptionpb"
	"github.com/tikv/pd/pkg/errs"
)

const (
	// Master key is of fixed 256 bits (32 bytes).
	masterKeyLength = 32 // in bytes
)

// MasterKey is used to encrypt and decrypt encryption metadata (i.e. data encryption keys).
type MasterKey struct {
	// Master key config. Used to compare if two master key is the same.
	Config *encryptionpb.MasterKey
	// Encryption key in plaintext. If it is nil, encryption is no-op.
	// Never output it to info log or persist it on disk.
	key []byte
}

// NewMasterKey obtains a master key from backend specified by given config.
// The config may be altered to fill in metadata generated when initializing the master key.
func NewMasterKey(config *encryptionpb.MasterKey) (*MasterKey, error) {
	if config == nil {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack("master key config is empty")
	}
	if plaintext := config.GetPlaintext(); plaintext != nil {
		return &MasterKey{
			Config: config,
			key:    nil,
		}, nil
	}
	if file := config.GetFile(); file != nil {
		key, err := newMasterKeyFromFile(file)
		if err != nil {
			return nil, err
		}
		return &MasterKey{
			Config: config,
			key:    key,
		}, nil
	}
	return nil, errors.New("unrecognized master key type")
}

// Encrypt encrypts given plaintext using the master key.
// IV is randomly generated and included in the result. Caller is expected to pass the same IV back
// for decryption.
func (k *MasterKey) Encrypt(plaintext []byte) (ciphertext []byte, iv []byte, err error) {
	if k.key == nil {
		return plaintext, nil, nil
	}
	return AesGcmEncrypt(k.key, plaintext)
}

// Decrypt decrypts given ciphertext using the master key and IV.
func (k *MasterKey) Decrypt(
	ciphertext []byte,
	iv []byte,
) (plaintext []byte, err error) {
	if k.key == nil {
		return ciphertext, nil
	}
	return AesGcmDecrypt(k.key, ciphertext, iv)
}

// IsPlaintext checks if the master key is of plaintext type (i.e. no-op for encryption).
func (k *MasterKey) IsPlaintext() bool {
	return k.key == nil
}

// newMasterKeyFromFile reads a hex-string from file specified in the config, and construct a
// MasterKey object. The key must be of 256 bits (32 bytes). The file can contain leading and
// tailing spaces.
func newMasterKeyFromFile(config *encryptionpb.MasterKeyFile) ([]byte, error) {
	if config == nil {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack("missing master key file config")
	}
	path := config.Path
	if path == "" {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack("missing master key file path")
	}
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack(
			"fail to get encryption key from file %s", path)
	}
	key, err := hex.DecodeString(strings.TrimSpace(string(data)))
	if err != nil {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack(
			"failed to decode encryption key from file, the key must be in hex form")
	}
	if len(key) != masterKeyLength {
		return nil, errs.ErrEncryptionNewMasterKey.GenWithStack(
			"unexpected key length from master key file, expected %d vs actual %d",
			masterKeyLength, len(key))
	}
	return key, nil
}
