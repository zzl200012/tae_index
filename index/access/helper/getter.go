package helper

import (
	"tae/index/access"
	"tae/index/access/access_iface"
	"tae/index/io/io_iface"
)

func GetZoneMapReader(holder access_iface.INonAppendableSegmentIndexHolder) io_iface.ISegmentZoneMapIndexReader {
	return holder.(*access.NonAppendableSegmentIndexHolder).GetZoneMapReader()
}

func GetFilterReaders(holder access_iface.INonAppendableSegmentIndexHolder) []io_iface.IStaticFilterIndexReader {
	return holder.(*access.NonAppendableSegmentIndexHolder).GetFilterReaders()
}


