package io

import "tae/index/io"

func GetSegmentZoneMapIndexReader() *io.SegmentZoneMapIndexReader {
	return &io.SegmentZoneMapIndexReader{}
}

func GetStaticFilterIndexReader() *io.StaticFilterIndexReader {
	return &io.StaticFilterIndexReader{}
}
