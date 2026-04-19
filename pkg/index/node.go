package index

import (
	"encoding/binary"
	"fmt"

	"github.com/go-db/pkg/schema"
	"github.com/go-db/pkg/storage"
)

// Header layout:
// IsLeaf (1 bytes)
// NumKeys (2 bytes)
// ParentID (4 bytes)
const (
	OffsetIsLeaf   = 0
	OffsetNumKeys  = 1
	OffsetParentID = 3
)

// Internal Node specifics
// LeftmostChild (4 bytes)
const (
	OffsetLeftmostChild = 7
	InternalHeaderSize  = 11
	InternalEntrySize   = 8 // Key (uint32) + RightChildID (uint32)
)

// Leaf Node specifics
// NextLeafID (4 bytes)
const (
	OffsetNextLeafID = 7
	LeafHeaderSize   = 11
)

var (
	RecordSize        = schema.RecordSize()
	MaxKeysInLeaf     = uint16((storage.PageSize - LeafHeaderSize) / RecordSize)
	MaxKeysInInternal = uint16((storage.PageSize - InternalHeaderSize) / uint32(InternalEntrySize))
)

// BaseNode provides helper methods on a raw page
type BaseNode struct {
	Page *storage.Page
}

func (n *BaseNode) IsLeaf() bool {
	return n.Page[OffsetIsLeaf] == 1
}

func (n *BaseNode) SetIsLeaf(isLeaf bool) {
	if isLeaf {
		n.Page[OffsetIsLeaf] = 1
	} else {
		n.Page[OffsetIsLeaf] = 0
	}
}

func (n *BaseNode) NumKeys() uint16 {
	return binary.LittleEndian.Uint16(n.Page[OffsetNumKeys : OffsetNumKeys+2])
}

func (n *BaseNode) SetNumKeys(numKeys uint16) {
	binary.LittleEndian.PutUint16(n.Page[OffsetNumKeys:OffsetNumKeys+2], numKeys)
}

func (n *BaseNode) ParentID() storage.PageID {
	return storage.PageID(binary.LittleEndian.Uint32(n.Page[OffsetParentID : OffsetParentID+4]))
}

func (n *BaseNode) SetParentID(id storage.PageID) {
	binary.LittleEndian.PutUint32(n.Page[OffsetParentID:OffsetParentID+4], uint32(id))
}

// LeafNode wraps a Page and interprets it as a B+Tree leaf
type LeafNode struct {
	BaseNode
}

func NewLeafNode(page *storage.Page) *LeafNode {
	leaf := &LeafNode{BaseNode{page}}
	leaf.SetIsLeaf(true)
	leaf.SetNumKeys(0)
	leaf.SetNextLeafID(0)
	return leaf
}

func (n *LeafNode) NextLeafID() storage.PageID {
	return storage.PageID(binary.LittleEndian.Uint32(n.Page[OffsetNextLeafID : OffsetNextLeafID+4]))
}

func (n *LeafNode) SetNextLeafID(id storage.PageID) {
	binary.LittleEndian.PutUint32(n.Page[OffsetNextLeafID:OffsetNextLeafID+4], uint32(id))
}

func (n *LeafNode) GetRecord(index uint16) (*schema.Record, error) {
	if index >= n.NumKeys() {
		return nil, fmt.Errorf("index out of bounds")
	}
	offset := LeafHeaderSize + uint32(index)*RecordSize
	return schema.Deserialize(n.Page[offset : offset+RecordSize])
}

func (n *LeafNode) SetRecord(index uint16, record *schema.Record) error {
	data, err := record.Serialize()
	if err != nil {
		return err
	}
	offset := LeafHeaderSize + uint32(index)*RecordSize
	copy(n.Page[offset:offset+RecordSize], data)
	return nil
}

func (n *LeafNode) KeyAt(index uint16) uint32 {
	if index >= n.NumKeys() {
		return 0
	}
	offset := LeafHeaderSize + uint32(index)*RecordSize
	return binary.LittleEndian.Uint32(n.Page[offset : offset+4])
}

// InternalNode wraps a Page and interprets it as a B+Tree inner node
type InternalNode struct {
	BaseNode
}

func NewInternalNode(page *storage.Page) *InternalNode {
	inner := &InternalNode{BaseNode{page}}
	inner.SetIsLeaf(false)
	inner.SetNumKeys(0)
	return inner
}

func (n *InternalNode) LeftmostChild() storage.PageID {
	return storage.PageID(binary.LittleEndian.Uint32(n.Page[OffsetLeftmostChild : OffsetLeftmostChild+4]))
}

func (n *InternalNode) SetLeftmostChild(id storage.PageID) {
	binary.LittleEndian.PutUint32(n.Page[OffsetLeftmostChild:OffsetLeftmostChild+4], uint32(id))
}

func (n *InternalNode) KeyAt(index uint16) uint32 {
	if index >= n.NumKeys() {
		return 0
	}
	offset := InternalHeaderSize + uint32(index)*uint32(InternalEntrySize)
	return binary.LittleEndian.Uint32(n.Page[offset : offset+4])
}

func (n *InternalNode) RightChildAt(index uint16) storage.PageID {
	if index >= n.NumKeys() {
		return 0
	}
	offset := InternalHeaderSize + uint32(index)*uint32(InternalEntrySize) + 4
	return storage.PageID(binary.LittleEndian.Uint32(n.Page[offset : offset+4]))
}

func (n *InternalNode) SetKeyAndRightChild(index uint16, key uint32, childID storage.PageID) {
	offset := InternalHeaderSize + uint32(index)*uint32(InternalEntrySize)
	binary.LittleEndian.PutUint32(n.Page[offset:offset+4], key)
	binary.LittleEndian.PutUint32(n.Page[offset+4:offset+8], uint32(childID))
}

// ChildAt gives the i-th child (0 is leftmost, 1 is right of 0-th key)
func (n *InternalNode) ChildAt(index uint16) storage.PageID {
	if index == 0 {
		return n.LeftmostChild()
	}
	return n.RightChildAt(index - 1)
}
