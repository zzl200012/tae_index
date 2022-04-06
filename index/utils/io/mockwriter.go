package io

import (
	"tae/index/io"
)

func GetBlockZoneMapIndexWriter() *io.BlockZoneMapIndexWriter {
	return &io.BlockZoneMapIndexWriter{}
}

func GetSegmentZoneMapIndexWriter() *io.SegmentZoneMapIndexWriter {
	return &io.SegmentZoneMapIndexWriter{}
}

func GetStaticFilterIndexWriter() *io.StaticFilterIndexWriter {
	return &io.StaticFilterIndexWriter{}
}
