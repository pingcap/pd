// Copyright 2020 TiKV Project Authors.
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

package placement

import (
	"bytes"
	"encoding/json"
)

// ruleConfig contains rule and rule group configurations.
type ruleConfig struct {
	rules  map[[2]string]*Rule   // {group, id} => Rule
	groups map[string]*RuleGroup // id => RuleGroup
}

func newRuleConfig() *ruleConfig {
	return &ruleConfig{
		rules:  make(map[[2]string]*Rule),
		groups: make(map[string]*RuleGroup),
	}
}

// adjust configs for `buildRuleList` and API use.
func (c *ruleConfig) adjust() {
	// remove all default group configurations.
	// if there are rules belong to the group, it will be re-add later.
	for id, g := range c.groups {
		if g.isDefault() {
			delete(c.groups, id)
		}
	}
	for _, r := range c.rules {
		g := c.groups[r.GroupID]
		if g == nil {
			// create default group configurations.
			g = &RuleGroup{ID: r.GroupID}
			c.groups[r.GroupID] = g
		}
		// setup group for `buildRuleList`
		r.group = g
	}
}

func (c *ruleConfig) getRule(key [2]string) *Rule {
	return c.rules[key]
}

func (c *ruleConfig) iterateRules(f func(*Rule)) {
	for _, r := range c.rules {
		f(r)
	}
}

func (c *ruleConfig) setRule(r *Rule) {
	c.rules[r.Key()] = r
}

func (c *ruleConfig) setGroup(g *RuleGroup) {
	c.groups[g.ID] = g
}

func (c *ruleConfig) beginPatch() *ruleConfigPatch {
	return &ruleConfigPatch{
		c:   c,
		mut: newRuleConfig(),
	}
}

// A helper data structure to update ruleConfig.
type ruleConfigPatch struct {
	c   *ruleConfig // original configuration to be updated
	mut *ruleConfig // record all to-commit rules and groups
}

func (p *ruleConfigPatch) getRule(key [2]string) *Rule {
	if r, ok := p.mut.rules[key]; ok {
		return r
	}
	return p.c.rules[key]
}

func (p *ruleConfigPatch) setRule(r *Rule) {
	if old := p.getRule(r.Key()); !jsonEquals(old, r) {
		p.mut.rules[r.Key()] = r
	}
}

func (p *ruleConfigPatch) deleteRule(group, id string) {
	key := [2]string{group, id}
	if old := p.getRule(key); old != nil {
		p.mut.rules[key] = nil
	}
}

func (p *ruleConfigPatch) getGroup(id string) *RuleGroup {
	if g, ok := p.mut.groups[id]; ok {
		return g
	}
	if g, ok := p.c.groups[id]; ok {
		return g
	}
	return &RuleGroup{ID: id}
}

func (p *ruleConfigPatch) setGroup(g *RuleGroup) {
	old := p.getGroup(g.ID)
	if !jsonEquals(old, g) {
		p.mut.groups[g.ID] = g
	}
}

func (p *ruleConfigPatch) deleteGroup(id string) {
	p.setGroup(&RuleGroup{ID: id})
}

func (p *ruleConfigPatch) iterateRules(f func(*Rule)) {
	for _, r := range p.mut.rules {
		if r != nil { // nil means delete.
			f(r)
		}
	}
	for _, r := range p.c.rules {
		if _, ok := p.mut.rules[r.Key()]; !ok { // ignore rules that has been overwritten.
			f(r)
		}
	}
}

func (p *ruleConfigPatch) adjust() {
	// setup rule.group for `buildRuleList` use.
	p.iterateRules(func(r *Rule) { r.group = p.getGroup(r.GroupID) })
}

// trim unnecessary updates. For example, remove a rule then insert the same rule.
func (p *ruleConfigPatch) trim() {
	for key, rule := range p.mut.rules {
		if jsonEquals(rule, p.c.getRule(key)) {
			delete(p.mut.rules, key)
		}
	}
	for id, group := range p.mut.groups {
		if jsonEquals(group, p.c.groups[id]) {
			delete(p.mut.groups, id)
		}
	}
}

// merge all mutations to ruleConfig.
func (p *ruleConfigPatch) commit() {
	for key, rule := range p.mut.rules {
		if rule == nil {
			delete(p.c.rules, key)
		} else {
			p.c.rules[key] = rule
		}
	}
	for id, group := range p.mut.groups {
		p.c.groups[id] = group
	}
	p.c.adjust()
}

func jsonEquals(a, b interface{}) bool {
	aa, _ := json.Marshal(a)
	bb, _ := json.Marshal(b)
	return bytes.Equal(aa, bb)
}
