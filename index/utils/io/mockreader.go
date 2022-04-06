package io

import "tae/index/io"

func GetBlockZoneMapIndexReader() *io.BlockZoneMapIndexReader {
	return &io.BlockZoneMapIndexReader{}
}

func GetSegmentZoneMapIndexReader() *io.SegmentZoneMapIndexReader {
	return &io.SegmentZoneMapIndexReader{}
}

func GetStaticFilterIndexReader() *io.StaticFilterIndexReader {
	return &io.StaticFilterIndexReader{}
}
