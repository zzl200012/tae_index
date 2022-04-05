package io

import (
	"tae/index/io"
)

func GetSegmentZoneMapIndexWriter() *io.SegmentZoneMapIndexWriter {
	return &io.SegmentZoneMapIndexWriter{}
}

func GetStaticFilterIndexWriter() *io.StaticFilterIndexWriter {
	return &io.StaticFilterIndexWriter{}
}
