package main

type Item struct {
	key   string
	value interface{}
}

type Node struct {
	bucket   *Tree
	items    []*Item
	children []*Node
}

type Tree struct {
	root     *Node
	minItems int
	maxItems int
}

// more later
