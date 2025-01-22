package data

import "fmt"

type SchemaTree struct {
	Name     string
	Children []*SchemaTree
}

func (n *SchemaTree) AddChild(name string) *SchemaTree {
	child := &SchemaTree{Name: name}
	n.Children = append(n.Children, child)
	return child
}

func (n *SchemaTree) PrettyPrint(indent string) {
	fmt.Println(indent + n.Name)
	for _, child := range n.Children {
		child.PrettyPrint(indent + "  ")
	}
}
