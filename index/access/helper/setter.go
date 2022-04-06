package helper

import (
	"tae/index/access"
	"tae/index/access/access_iface"
	"tae/index/io/io_iface"
)

func SetFilterReaders(holder access_iface.INonAppendableSegmentIndexHolder, readers []io_iface.IStaticFilterIndexReader) {
	holder.(*access.NonAppendableSegmentIndexHolder).SetFilterReaders(readers)
}

func SetZoneMapReader(holder access_iface.INonAppendableSegmentIndexHolder, reader io_iface.ISegmentZoneMapIndexReader) {
	holder.(*access.NonAppendableSegmentIndexHolder).SetZoneMapReader(reader)
}
