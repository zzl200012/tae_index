package common

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
	"tae/mock"
)

type VirtualIndexFile struct {
	host *mock.Resource
	meta *IndexMeta
	stat *fileStat
}

func MakeVirtualIndexFile(segment *mock.Resource, meta *IndexMeta) *VirtualIndexFile {
	file := &VirtualIndexFile{
		host: segment,
		meta: meta,
		stat: &fileStat{
			size:  int64(meta.Size),
			osize: int64(meta.RawSize),
			algo: uint8(meta.CompType),
		},
	}
	file.Ref()
	return file
}

func (file *VirtualIndexFile) Read(data []byte) (n int, err error) {
	if uint32(len(data)) != file.meta.Size {
		panic("unexpected error: buffer length mismatch")
	}
	part, err := file.host.FetchPart(file.meta.PartOffset)
	if err != nil {
		return 0, err
	}
	return part.ReadAt(data, int64(file.meta.StartOffset))
}

func (file *VirtualIndexFile) Ref() {
	// no-op
}

func (file *VirtualIndexFile) Unref() {
	// no-op
}

func (file *VirtualIndexFile) RefCount() int64 {
	// no-op
	return 0
}

func (file *VirtualIndexFile) Stat() common.FileInfo {
	return file.stat
}

func (file *VirtualIndexFile) GetFileType() common.FileType {
	return common.DiskFile
}

type fileStat struct {
	size  int64
	osize int64
	name  string
	algo  uint8
}

func (info *fileStat) Size() int64 {
	return info.size
}

func (info *fileStat) OriginSize() int64 {
	return info.osize
}

func (info *fileStat) Name() string {
	return info.name
}

func (info *fileStat) CompressAlgo() int {
	return int(info.algo)
}