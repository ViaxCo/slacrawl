package importer

import (
	"archive/zip"
	"errors"
	"io"
	"math"
	"sort"
)

// headerReader freezes only metadata requested by File.DataOffset. Payload
// reads still use the retained source and archive/zip's decompressor/checksum.
type headerReader struct {
	source   io.ReaderAt
	ranges   []headerRange
	captured map[int64]byte
}
type headerRange struct {
	offset int64
	data   []byte
}

func (r *headerReader) ReadAt(p []byte, offset int64) (int, error) {
	if offset < 0 || int64(len(p)) > math.MaxInt64-offset {
		return 0, errors.New("invalid ZIP read range")
	}
	if r.captured != nil {
		return r.captureReadAt(p, offset)
	}
	total := 0
	for total < len(p) {
		current := offset + int64(total)
		index := sort.Search(len(r.ranges), func(i int) bool { return r.ranges[i].offset+int64(len(r.ranges[i].data)) > current })
		if index < len(r.ranges) && r.ranges[index].offset <= current {
			cached := r.ranges[index]
			total += copy(p[total:], cached.data[current-cached.offset:])
			continue
		}
		length := len(p) - total
		if index < len(r.ranges) {
			length = min(length, int(r.ranges[index].offset-current))
		}
		n, err := r.source.ReadAt(p[total:total+length], current)
		total += n
		if err != nil && (err != io.EOF || n != length) {
			return total, err
		}
		if n != length {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

// Capture by absolute byte position, with first-read bytes winning overlaps.
// Sort once after capture so reverse central-directory order cannot cause
// quadratic insertion work. Only DataOffset metadata enters this map.
func (r *headerReader) captureReadAt(p []byte, offset int64) (int, error) {
	total := 0
	for total < len(p) {
		if value, ok := r.captured[offset+int64(total)]; ok {
			p[total] = value
			total++
			continue
		}
		end := total + 1
		for end < len(p) {
			if _, ok := r.captured[offset+int64(end)]; ok {
				break
			}
			end++
		}
		n, err := r.source.ReadAt(p[total:end], offset+int64(total))
		for i := 0; i < n; i++ {
			r.captured[offset+int64(total+i)] = p[total+i]
		}
		total += n
		if err != nil && (err != io.EOF || total != end) {
			return total, err
		}
		if total != end {
			return total, io.ErrUnexpectedEOF
		}
	}
	return total, nil
}

func (r *headerReader) freeze() {
	offsets := make([]int64, 0, len(r.captured))
	for offset := range r.captured {
		offsets = append(offsets, offset)
	}
	sort.Slice(offsets, func(i, j int) bool { return offsets[i] < offsets[j] })
	for _, offset := range offsets {
		last := len(r.ranges) - 1
		if last < 0 || r.ranges[last].offset+int64(len(r.ranges[last].data)) != offset {
			r.ranges = append(r.ranges, headerRange{offset: offset})
			last++
		}
		r.ranges[last].data = append(r.ranges[last].data, r.captured[offset])
	}
	r.captured = nil
}

func openZIP(source io.ReaderAt, size int64, closer io.Closer) (*Export, error) {
	headers := &headerReader{source: source}
	reader, err := zip.NewReader(headers, size)
	if err != nil {
		_ = closer.Close()
		return nil, err
	}
	files, err := indexZIP(reader.File)
	if err != nil {
		_ = closer.Close()
		return nil, err
	}
	headers.captured = map[int64]byte{}
	for _, file := range reader.File {
		if _, err := file.DataOffset(); err != nil {
			_ = closer.Close()
			return nil, errors.New("invalid export ZIP local header")
		}
	}
	headers.freeze()
	return &Export{fs: reader, closer: closer, zipFiles: files}, nil
}
