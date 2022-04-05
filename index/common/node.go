package common

import (
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/buffer/manager/iface"
	"github.com/matrixorigin/matrixone/pkg/vm/engine/aoe/storage/common"
)

type IndexBufferNode struct {
	common.RefHelper
	inner *manager.Node
}

func NewIndexBufferNode(mgr iface.IBufferManager, vf common.IVFile, useCompress bool, constructor buf.MemoryNodeConstructor) *IndexBufferNode {
	node := new(IndexBufferNode)
	node.inner = mgr.CreateNode(vf, useCompress, constructor).(*manager.Node)
	node.Ref()
	node.OnZeroCB = node.close
	return node
}

func (node *IndexBufferNode) GetInnerNode() iface.MangaedNode {
	return node.inner.GetManagedNode()
}

func (node *IndexBufferNode) close() {
	if node.inner != nil {
		err := node.inner.Close()
		if err != nil {
			panic(err)
		}
	}
}