package storage

import (
	"fmt"
	"os"
)

const PageSize = 4096

// PageID is a 32-bit unsigned integer (can address 16TB with 4KB pages)
type PageID uint32

type Page [PageSize]byte

type Pager struct {
	file     *os.File
	numPages PageID
}

func NewPager(filename string) (*Pager, error) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE, 0666)
	if err != nil {
		return nil, err
	}

	fileInfo, err := file.Stat()
	if err != nil {
		return nil, err
	}

	size := fileInfo.Size()
	var numPages PageID
	if size%PageSize != 0 {
		return nil, fmt.Errorf("file size %d is not a multiple of PageSize", size)
	}
	numPages = PageID(size / PageSize)

	return &Pager{
		file:     file,
		numPages: numPages,
	}, nil
}

func (p *Pager) ReadPage(pageID PageID) (*Page, error) {
	if pageID >= p.numPages {
		return nil, fmt.Errorf("page ID %d out of bounds (numPages: %d)", pageID, p.numPages)
	}

	var page Page
	offset := int64(pageID) * PageSize
	_, err := p.file.ReadAt(page[:], offset)
	if err != nil {
		return nil, err
	}

	return &page, nil
}

func (p *Pager) WritePage(pageID PageID, page *Page) error {
	offset := int64(pageID) * PageSize
	_, err := p.file.WriteAt(page[:], offset)
	if err != nil {
		return err
	}

	if pageID >= p.numPages {
		p.numPages = pageID + 1
	}

	return nil
}

func (p *Pager) AllocatePage() PageID {
	pageID := p.numPages
	p.numPages++
	return pageID
}

func (p *Pager) FileSize() int64 {
	return int64(p.numPages) * PageSize
}

func (p *Pager) NumPages() uint32 {
	return uint32(p.numPages)
}

func (p *Pager) Sync() error {
	return p.file.Sync()
}

func (p *Pager) Close() error {
	if err := p.file.Sync(); err != nil {
		return err
	}
	return p.file.Close()
}
