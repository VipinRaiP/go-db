package index

import (
	"fmt"

	"github.com/go-db/pkg/buffer"
	"github.com/go-db/pkg/schema"
	"github.com/go-db/pkg/storage"
)

type BTree struct {
	bp         *buffer.BufferPool
	rootPageID storage.PageID
}

// NewBTree initializes a BTree structure on top of the buffer pool.
func NewBTree(bp *buffer.BufferPool, rootPageID storage.PageID) *BTree {
	return &BTree{
		bp:         bp,
		rootPageID: rootPageID,
	}
}

// InitEmpty creates an initial root leaf page.
func (bt *BTree) InitEmpty() error {
	frame, err := bt.bp.NewPage()
	if err != nil {
		return err
	}
	defer bt.bp.UnpinPage(frame.PageID, true)

	rootLeaf := NewLeafNode(frame.Page)
	rootLeaf.SetParentID(0)
	bt.rootPageID = frame.PageID
	return nil
}

func (bt *BTree) RootPageID() storage.PageID {
    return bt.rootPageID
}

// Find retrieves a record by ID
func (bt *BTree) Find(key uint32) (*schema.Record, error) {
	leafNode, frame, err := bt.findLeafNode(key)
	if err != nil {
		return nil, err
	}
	defer bt.bp.UnpinPage(frame.PageID, false)

	numKeys := leafNode.NumKeys()
	for i := uint16(0); i < numKeys; i++ {
		if leafNode.KeyAt(i) == key {
			return leafNode.GetRecord(i)
		}
	}
	return nil, fmt.Errorf("record %d not found", key)
}

func (bt *BTree) findLeafNode(key uint32) (*LeafNode, *buffer.Frame, error) {
	frame, err := bt.bp.FetchPage(bt.rootPageID)
	if err != nil {
		return nil, nil, err
	}

	node := &BaseNode{Page: frame.Page}

	for !node.IsLeaf() {
		inner := &InternalNode{BaseNode: *node}
		numKeys := inner.NumKeys()

		var nextChild storage.PageID
		found := false
		for i := uint16(0); i < numKeys; i++ {
			if key < inner.KeyAt(i) {
				nextChild = inner.ChildAt(i)
				found = true
				break
			}
		}
		if !found {
			nextChild = inner.ChildAt(numKeys)
		}

		bt.bp.UnpinPage(frame.PageID, false)

		frame, err = bt.bp.FetchPage(nextChild)
		if err != nil {
			return nil, nil, err
		}
		node = &BaseNode{Page: frame.Page}
	}

	return &LeafNode{BaseNode: *node}, frame, nil
}

// Insert adds a record to the B+ Tree
func (bt *BTree) Insert(record *schema.Record) error {
	leaf, frame, err := bt.findLeafNode(record.ID)
	if err != nil {
		return err
	}
	defer bt.bp.UnpinPage(frame.PageID, true)

	numKeys := leaf.NumKeys()

	insertIdx := numKeys
	for i := uint16(0); i < numKeys; i++ {
		if record.ID < leaf.KeyAt(i) {
			insertIdx = i
			break
		} else if record.ID == leaf.KeyAt(i) {
			return fmt.Errorf("duplicate key %d", record.ID)
		}
	}

	if numKeys >= MaxKeysInLeaf {
		return bt.splitLeafAndInsert(leaf, frame, insertIdx, record)
	}

	for i := numKeys; i > insertIdx; i-- {
		rec, _ := leaf.GetRecord(i - 1)
		leaf.SetRecord(i, rec)
	}

	err = leaf.SetRecord(insertIdx, record)
	if err != nil {
		return err
	}

	leaf.SetNumKeys(numKeys + 1)
	return nil
}

func (bt *BTree) splitLeafAndInsert(leaf *LeafNode, frame *buffer.Frame, insertIdx uint16, record *schema.Record) error {
	newFrame, err := bt.bp.NewPage()
	if err != nil {
		return err
	}
	defer bt.bp.UnpinPage(newFrame.PageID, true)

	newLeaf := NewLeafNode(newFrame.Page)
	newLeaf.SetParentID(leaf.ParentID())
	newLeaf.SetNextLeafID(leaf.NextLeafID())
	leaf.SetNextLeafID(newFrame.PageID)

	tempRecs := make([]*schema.Record, 0, MaxKeysInLeaf+1)

	for i := uint16(0); i < MaxKeysInLeaf; i++ {
		rec, _ := leaf.GetRecord(i)
		tempRecs = append(tempRecs, rec)
	}

	tempRecs = append(tempRecs[:insertIdx], append([]*schema.Record{record}, tempRecs[insertIdx:]...)...)

	splitIndex := (MaxKeysInLeaf + 1) / 2

	leaf.SetNumKeys(0)
	for i := uint16(0); i < splitIndex; i++ {
		leaf.SetRecord(i, tempRecs[i])
	}
	leaf.SetNumKeys(splitIndex)

	newLeaf.SetNumKeys(0)
	for i := splitIndex; i < uint16(len(tempRecs)); i++ {
		newLeaf.SetRecord(i-splitIndex, tempRecs[i])
	}
	newLeaf.SetNumKeys(uint16(len(tempRecs)) - splitIndex)

	return bt.insertIntoParent(frame.PageID, leaf.ParentID(), newLeaf.KeyAt(0), newFrame.PageID)
}

func (bt *BTree) insertIntoParent(leftID storage.PageID, parentID storage.PageID, key uint32, rightID storage.PageID) error {
	if parentID == 0 {
		return bt.createNewRoot(leftID, key, rightID)
	}

	parentFrame, err := bt.bp.FetchPage(parentID)
	if err != nil {
		return err
	}
	defer bt.bp.UnpinPage(parentFrame.PageID, true)

	parent := &InternalNode{BaseNode{Page: parentFrame.Page}}

	numKeys := parent.NumKeys()

	insertIdx := numKeys
	for i := uint16(0); i < numKeys; i++ {
		if key < parent.KeyAt(i) {
			insertIdx = i
			break
		}
	}

	if numKeys < MaxKeysInInternal {
		for i := numKeys; i > insertIdx; i-- {
			k := parent.KeyAt(i - 1)
			c := parent.RightChildAt(i - 1)
			parent.SetKeyAndRightChild(i, k, c)
		}
		parent.SetKeyAndRightChild(insertIdx, key, rightID)
		parent.SetNumKeys(numKeys + 1)
		return nil
	}

	return fmt.Errorf("internal node split not fully implemented for this milestone")
}

func (bt *BTree) createNewRoot(leftID storage.PageID, key uint32, rightID storage.PageID) error {
	newRootFrame, err := bt.bp.NewPage()
	if err != nil {
		return err
	}
	defer bt.bp.UnpinPage(newRootFrame.PageID, true)

	newRoot := NewInternalNode(newRootFrame.Page)
	newRoot.SetLeftmostChild(leftID)
	newRoot.SetKeyAndRightChild(0, key, rightID)
	newRoot.SetNumKeys(1)

	bt.updateParent(leftID, newRootFrame.PageID)
	bt.updateParent(rightID, newRootFrame.PageID)

	bt.rootPageID = newRootFrame.PageID
	return nil
}

func (bt *BTree) updateParent(childID storage.PageID, parentID storage.PageID) {
	childFrame, _ := bt.bp.FetchPage(childID)
	node := &BaseNode{Page: childFrame.Page}
	node.SetParentID(parentID)
	bt.bp.UnpinPage(childID, true)
}
