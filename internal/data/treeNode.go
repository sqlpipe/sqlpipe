package data

import (
	"encoding/json"
	"fmt"
	"sync"
)

type SafeTreeNode struct {
	ID          string
	Name        string
	ContainsPII bool
	PIIType     string
	Children    []*SafeTreeNode
	Parent      *SafeTreeNode
	Mu          *sync.Mutex
}

func (n *SafeTreeNode) AddChild(id, name string) *SafeTreeNode {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	child := &SafeTreeNode{
		ID:          id,
		Name:        name,
		ContainsPII: false,
		Children:    []*SafeTreeNode{},
		Parent:      n,
		Mu:          &sync.Mutex{},
	}
	n.Children = append(n.Children, child)
	return child
}

func (n *SafeTreeNode) ChangeContainsPII(containsPII bool, PIIType string) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	n.ContainsPII = containsPII
	n.PIIType = PIIType
}

func (n *SafeTreeNode) PrettyPrint() {
	toPrint := fmt.Sprintf("- %s: Contains PII: %t", n.Name, n.ContainsPII)

	if n.PIIType != "" {
		toPrint = fmt.Sprintf("%v, PII Type: %s\n", toPrint, n.PIIType)
	} else {
		toPrint = fmt.Sprintf("%v\n", toPrint)
	}

	fmt.Println(toPrint)

	for _, child := range n.Children {
		child.PrettyPrint()
	}
}

func (n *SafeTreeNode) FindChildNodeByName(name string) (*SafeTreeNode, bool) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	for _, child := range n.Children {
		if child.Name == name {
			return child, true
		}
	}
	return nil, false
}

func (n *SafeTreeNode) RecursivelySearchForPII() bool {

	if n.ContainsPII {
		n.ContainsPII = true
	}

	for _, child := range n.Children {
		if child.RecursivelySearchForPII() {
			n.ContainsPII = true
		}
	}

	return n.ContainsPII
}

func (n *SafeTreeNode) CloneWithoutParents() *SafeTreeNode {

	clone := &SafeTreeNode{
		ID:          n.ID,
		Name:        n.Name,
		ContainsPII: n.ContainsPII,
		PIIType:     n.PIIType,
		Children:    []*SafeTreeNode{},
		Mu:          &sync.Mutex{},
	}

	for _, child := range n.Children {
		clone.Children = append(clone.Children, child.CloneWithoutParents())
	}

	return clone
}

func (n *SafeTreeNode) ToJSON() (string, error) {

	newTree := n.CloneWithoutParents()

	data, err := json.Marshal(newTree)
	if err != nil {
		return "", err
	}
	return string(data), nil
}

func (n *SafeTreeNode) FromJSON(data string) (*SafeTreeNode, error) {

	node := &SafeTreeNode{}
	err := json.Unmarshal([]byte(data), node)
	if err != nil {
		return nil, err
	}

	return node, nil
}
