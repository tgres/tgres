package main

import (
	"encoding/binary"
	"fmt"
	"io"
	"os"
)

type metadata struct {
	CF    uint32  // CF: 0:UNK 1:AVG 2:SUM 3:LAST 4:MAX 5:MIN
	_     uint32  // max retention
	_     float32 // xff
	Count uint32
}

type archiveInfo struct {
	Offset uint32 // The byte offset of the archive within the database
	Step   uint32 // seconds per point
	Size   uint32 // The number of data points
}

type header struct {
	metadata               // General metadata about the database
	archives []archiveInfo // Information about each of the archives in the database, in order of precision
}

type point struct {
	TimeStamp uint32  // Timestamp in seconds past the epoch
	Value     float64 // Data point value
}

type archive []point

func (a archive) Len() int           { return len(a) }
func (a archive) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a archive) Less(i, j int) bool { return a[i].TimeStamp < a[j].TimeStamp }

type whisper struct {
	*header
	file io.ReadWriteSeeker
}

func readHeader(buf io.ReadSeeker) (*header, error) {
	var hdr header

	if err := binary.Read(buf, binary.BigEndian, &hdr.metadata); err != nil {
		return nil, err
	}

	hdr.archives = make([]archiveInfo, hdr.metadata.Count)
	for i := 0; i < len(hdr.archives); i++ {
		if err := binary.Read(buf, binary.BigEndian, &hdr.archives[i]); err != nil {
			return nil, err
		}
	}

	return &hdr, nil
}

func newWhisper(path string) (*whisper, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	header, err := readHeader(f)
	if err != nil {
		return nil, err
	}
	return &whisper{header: header, file: f}, nil
}

func (w *whisper) dumpArchive(n int) ([]point, error) {
	if n >= len(w.header.archives) {
		return nil, fmt.Errorf("database contains only %d archives", len(w.header.archives))
	} else if n < 0 {
		return nil, fmt.Errorf("archive index must be greater than 0")
	}

	info := w.header.archives[n]
	points := make([]point, int(info.Size))
	err := w.readPoints(info.Offset, points)
	return points, err
}

func (w *whisper) readPoints(offset uint32, points []point) error {
	if _, err := w.file.Seek(int64(offset), 0); err != nil {
		return err
	}
	return binary.Read(w.file, binary.BigEndian, points)
}
