package buffer

import (
	"container/list"
	"fmt"
	"sync"
	"github.com/go-db/pkg/storage"
)

type FrameID uint32

// Frame represents a spot in memory that holds a page.
type Frame struct {
	Page     *storage.Page
	PageID   storage.PageID
	IsDirty  bool
	PinCount int
}

type BufferPool struct {
	mu          sync.RWMutex
	pager       *storage.Pager
	poolSize    int
	frames      []*Frame
	pageTable   map[storage.PageID]FrameID
	lruList     *list.List
	lruElements map[FrameID]*list.Element
	freeFrames  []FrameID
}

// NewBufferPool initializes an in-memory page LRU cache.
func NewBufferPool(pager *storage.Pager, poolSize int) *BufferPool {
	frames := make([]*Frame, poolSize)
	freeFrames := make([]FrameID, poolSize)
	for i := 0; i < poolSize; i++ {
		frames[i] = &Frame{
			Page: new(storage.Page),
		}
		freeFrames[i] = FrameID(i)
	}

	return &BufferPool{
		pager:       pager,
		poolSize:    poolSize,
		frames:      frames,
		pageTable:   make(map[storage.PageID]FrameID),
		lruList:     list.New(),
		lruElements: make(map[FrameID]*list.Element),
		freeFrames:  freeFrames,
	}
}

// FetchPage retrieves a page from the buffer pool or loads it from disk.
func (bp *BufferPool) FetchPage(pageID storage.PageID) (*Frame, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	// If page is already in the pool
	if frameID, found := bp.pageTable[pageID]; found {
		frame := bp.frames[frameID]
		frame.PinCount++
		// Remove from LRU list since it is pinned
		if element, ok := bp.lruElements[frameID]; ok {
			bp.lruList.Remove(element)
			delete(bp.lruElements, frameID)
		}
		return frame, nil
	}

	// Page not in pool, must load from disk
	frameID, err := bp.getAvailableFrame()
	if err != nil {
		return nil, err
	}

	frame := bp.frames[frameID]
	
	// Read from disk
	page, err := bp.pager.ReadPage(pageID)
	if err != nil {
		// Put frame back into free list if read fails
		bp.freeFrames = append(bp.freeFrames, frameID)
		return nil, err
	}

	// Copy data to frame
	copy(frame.Page[:], page[:])
	frame.PageID = pageID
	frame.IsDirty = false
	frame.PinCount = 1

	// Update page table
	bp.pageTable[pageID] = frameID

	return frame, nil
}

// UnpinPage indicates that the caller is done with the page.
func (bp *BufferPool) UnpinPage(pageID storage.PageID, isDirty bool) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameID, found := bp.pageTable[pageID]
	if !found {
		return fmt.Errorf("page ID %d not found in buffer pool", pageID)
	}

	frame := bp.frames[frameID]
	if frame.PinCount <= 0 {
		return fmt.Errorf("page ID %d pin count became negative", pageID)
	}

	frame.PinCount--
	if isDirty {
		frame.IsDirty = true
	}

	if frame.PinCount == 0 {
		// Add to LRU
		element := bp.lruList.PushFront(frameID)
		bp.lruElements[frameID] = element
	}

	return nil
}

// FlushPage forces a page to disk.
func (bp *BufferPool) FlushPage(pageID storage.PageID) error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameID, found := bp.pageTable[pageID]
	if !found {
		return nil // Not in pool, assuming it's on disk
	}

	frame := bp.frames[frameID]
	if frame.IsDirty {
		err := bp.pager.WritePage(frame.PageID, frame.Page)
		if err != nil {
			return err
		}
		frame.IsDirty = false
	}
	return nil
}

func (bp *BufferPool) FlushAllPages() error {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	for _, frameID := range bp.pageTable {
		frame := bp.frames[frameID]
		if frame.IsDirty {
			err := bp.pager.WritePage(frame.PageID, frame.Page)
			if err != nil {
				return err
			}
			frame.IsDirty = false
		}
	}
	return nil
}

// NewPage allocates a new page on disk and brings it into the pool.
func (bp *BufferPool) NewPage() (*Frame, error) {
	bp.mu.Lock()
	defer bp.mu.Unlock()

	frameID, err := bp.getAvailableFrame()
	if err != nil {
		return nil, err
	}

	// Allocate in pager. This doesn't write to disk immediately.
	pageID := bp.pager.AllocatePage()

	frame := bp.frames[frameID]
	// Zero out page memory
	*frame.Page = storage.Page{}
	frame.PageID = pageID
	frame.IsDirty = true
	frame.PinCount = 1

	bp.pageTable[pageID] = frameID

	return frame, nil
}

// getAvailableFrame returns a free frame or evicts a page using LRU.
func (bp *BufferPool) getAvailableFrame() (FrameID, error) {
	if len(bp.freeFrames) > 0 {
		frameID := bp.freeFrames[len(bp.freeFrames)-1]
		bp.freeFrames = bp.freeFrames[:len(bp.freeFrames)-1]
		return frameID, nil
	}

	// Evict using LRU
	element := bp.lruList.Back()
	if element == nil {
		return 0, fmt.Errorf("all frames are pinned. buffer pool is full")
	}

	frameID := element.Value.(FrameID)
	frame := bp.frames[frameID]

	// Remove from LRU list
	bp.lruList.Remove(element)
	delete(bp.lruElements, frameID)

	// Flush to disk if dirty
	if frame.IsDirty {
		err := bp.pager.WritePage(frame.PageID, frame.Page)
		if err != nil {
			// Put it back in LRU if write fails
			bp.lruElements[frameID] = bp.lruList.PushBack(frameID)
			return 0, fmt.Errorf("failed to flush page %d: %w", frame.PageID, err)
		}
	}

	// Delete from page table
	delete(bp.pageTable, frame.PageID)

	return frameID, nil
}
