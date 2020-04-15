// Copyright 2020 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package component

import (
	"fmt"
	"sync"

	"github.com/pingcap/log"
	"go.uber.org/zap"
)

// Manager is used to manage components.
type Manager struct {
	sync.RWMutex
	// component -> addresses
	Addresses map[string][]string
}

// NewManager creates a new component manager.
func NewManager() *Manager {
	return &Manager{
		Addresses: make(map[string][]string),
	}
}

// GetComponentAddrs returns component addresses for a given component.
func (c *Manager) GetComponentAddrs(component string) []string {
	c.RLock()
	defer c.RUnlock()
	addresses := []string{}
	if ca, ok := c.Addresses[component]; ok {
		addresses = append(addresses, ca...)
	}
	return addresses
}

// GetAllComponentAddrs returns all components' addresses.
func (c *Manager) GetAllComponentAddrs() map[string][]string {
	c.RLock()
	defer c.RUnlock()
	n := make(map[string][]string)
	for k, v := range c.Addresses {
		b := make([]string, len(v))
		copy(b, v)
		n[k] = b
	}
	return n
}

// GetComponent returns the component from a given component ID.
func (c *Manager) GetComponent(addr string) string {
	c.RLock()
	defer c.RUnlock()
	for component, ca := range c.Addresses {
		if exist, _ := contains(ca, addr); exist {
			return component
		}
	}
	return ""
}

// Register is used for registering a component with an address to PD.
func (c *Manager) Register(component, addr string) error {
	c.Lock()
	defer c.Unlock()

	ca, ok := c.Addresses[component]
	if exist, _ := contains(ca, addr); ok && exist {
		log.Info("address has already been registered", zap.String("component", component), zap.String("address", addr))
		return fmt.Errorf("component %s address %s has already been registered", component, addr)
	}

	ca = append(ca, addr)
	c.Addresses[component] = ca
	log.Info("address registers successfully", zap.String("component", component), zap.String("address", addr))
	return nil
}

// UnRegister is used for unregistering a component with an address from PD.
func (c *Manager) UnRegister(component, addr string) error {
	c.Lock()
	defer c.Unlock()

	ca, ok := c.Addresses[component]
	if !ok {
		return fmt.Errorf("component %s not found", component)
	}

	if exist, idx := contains(ca, addr); exist {
		ca = append(ca[:idx], ca[idx+1:]...)
		log.Info("address has successfully been unregistered", zap.String("component", component), zap.String("address", addr))
		if len(ca) == 0 {
			delete(c.Addresses, component)
			return nil
		}

		c.Addresses[component] = ca
		return nil
	}

	return fmt.Errorf("address %s not found", addr)
}

func contains(slice []string, item string) (bool, int) {
	for i, s := range slice {
		if s == item {
			return true, i
		}
	}

	return false, 0
}
