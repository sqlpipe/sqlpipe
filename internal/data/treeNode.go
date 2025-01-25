package data

import (
	"fmt"
	"sync"
)

type SafeTreeNode struct {
	ID          string
	Name        string
	ContainsPII bool
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

func (n *SafeTreeNode) ChangeContainsPII(containsPII bool) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	n.ContainsPII = containsPII
}

func (n *SafeTreeNode) PrettyPrint(indent string) {

	n.Mu.Lock()
	defer n.Mu.Unlock()

	fmt.Printf("%s- %s (Contains PII: %t)\n", indent, n.Name, n.ContainsPII)
	for _, child := range n.Children {
		child.PrettyPrint(indent + "  ")
	}
}
