package smt

import (
	"context"
	"fmt"
	"sync"

	"github.com/dgravesa/go-parallel/parallel"
	"github.com/ledgerwatch/erigon/smt/pkg/utils"
	"github.com/ledgerwatch/erigon/zk"
)

type InsertBatchConfig struct {
	ctx                 context.Context
	logPrefix           string
	shouldPrintProgress bool
}

func NewInsertBatchConfig(ctx context.Context, logPrefix string, shouldPrintProgress bool) InsertBatchConfig {
	return InsertBatchConfig{
		ctx:                 ctx,
		logPrefix:           logPrefix,
		shouldPrintProgress: shouldPrintProgress,
	}
}

func getProgressPrinterPre(logPrefix string, progressType string, size uint64, shouldPrintProgress bool) (progressChanPre *chan uint64, stopProgressPrinterPre func()) {
	var newChan chan uint64
	if shouldPrintProgress {
		newChan, stopProgressPrinterPre = zk.ProgressPrinter(fmt.Sprintf("[%s] SMT incremental progress (%s)", logPrefix, progressType), size, false)
	} else {
		newChan = make(chan uint64, size)
		var once sync.Once

		stopProgressPrinterPre = func() {
			once.Do(func() { close(newChan) })
		}
	}

	return &newChan, stopProgressPrinterPre
}

func (s *SMT) InsertBatch(cfg InsertBatchConfig, nodeKeys []*utils.NodeKey, nodeValues []*utils.NodeValue8, nodeValuesHashes []*[4]uint64, rootNodeHash *utils.NodeKey) (r *SMTResponse, err error) {
	s.clearUpMutex.Lock()
	defer s.clearUpMutex.Unlock()

	var (
		maxInsertingNodePathLevel = 0
		size                      = len(nodeKeys)
		smtBatchNodeRoot          *smtBatchNode
		nodeHashesForDelete       = make(map[uint64]map[uint64]map[uint64]map[uint64]*utils.NodeKey)
	)

	//BE CAREFUL: modifies the arrays
	if err := s.preprocessBatchedNodeValues(
		cfg.logPrefix,
		cfg.shouldPrintProgress,
		&nodeKeys,
		&nodeValues,
		&nodeValuesHashes,
		&rootNodeHash,
	); err != nil {
		return nil, fmt.Errorf("preprocessBatchedNodeValues: %w", err)
	}

	//DO NOT MOVE ABOVE PREPROCESS
	size = len(nodeKeys)

	progressChan, stopProgressPrinter := getProgressPrinterPre(cfg.logPrefix, "process", uint64(size), cfg.shouldPrintProgress)
	defer stopProgressPrinter()

	for i := 0; i < size; i++ {
		select {
		case <-cfg.ctx.Done():
			return nil, fmt.Errorf("context done")
		case *progressChan <- uint64(1):
		default:
		}

		insertingNodeKey := nodeKeys[i]
		insertingNodeValue := nodeValues[i]
		insertingNodeValueHash := nodeValuesHashes[i]
		insertingNodePath := insertingNodeKey.GetPath()
		insertingNodePathLevel, insertingPointerToSmtBatchNode, visitedNodeHashes, err := s.findInsertingPoint(insertingNodePath, rootNodeHash, &smtBatchNodeRoot, insertingNodeValue.IsZero())
		if err != nil {
			return nil, err
		}
		updateNodeHashesForDelete(nodeHashesForDelete, visitedNodeHashes)

		// special case if root does not exists yet
		if (*insertingPointerToSmtBatchNode) == nil {
			if !insertingNodeValue.IsZero() {
				*insertingPointerToSmtBatchNode = newSmtBatchNodeLeaf(insertingNodeKey, (*utils.NodeKey)(insertingNodeValueHash), nil)
			}
			// else branch would be for deleting a value but the root does not exists => there is nothing to delete
			continue
		}

		insertingRemainingKey := utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel)
		if !insertingNodeValue.IsZero() {
			if !((*insertingPointerToSmtBatchNode).isLeaf()) {
				if insertingPointerToSmtBatchNode, err = (*insertingPointerToSmtBatchNode).createALeafInEmptyDirection(insertingNodePath, insertingNodePathLevel, insertingNodeKey); err != nil {
					return nil, err
				}
				insertingRemainingKey = *((*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey)
				insertingNodePathLevel++
			}

			if !((*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey.IsEqualTo(insertingRemainingKey)) {
				currentTreeNodePath := utils.JoinKey(insertingNodePath[:insertingNodePathLevel], *(*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey).GetPath()
				for insertingNodePath[insertingNodePathLevel] == currentTreeNodePath[insertingNodePathLevel] {
					insertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).expandLeafByAddingALeafInDirection(insertingNodePath, insertingNodePathLevel)
					insertingNodePathLevel++
				}

				(*insertingPointerToSmtBatchNode).expandLeafByAddingALeafInDirection(currentTreeNodePath, insertingNodePathLevel)

				if insertingPointerToSmtBatchNode, err = (*insertingPointerToSmtBatchNode).createALeafInEmptyDirection(insertingNodePath, insertingNodePathLevel, insertingNodeKey); err != nil {
					return nil, err
				}
				// EXPLAIN THE LINE BELOW: there is no need to update insertingRemainingKey because it is not needed anymore therefore its value is incorrect if used after this line
				// insertingRemainingKey = *((*insertingPointerToSmtBatchNode).nodeLeftKeyOrRemainingKey)
				insertingNodePathLevel++
			}

			// EXPLAIN THE LINE BELOW: cannot delete the old values because it might be used as a value of an another node
			// updateNodeHashesForDelete(nodeHashesForDelete, []*utils.NodeKey{(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash})
			(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash = (*utils.NodeKey)(insertingNodeValueHash)
		} else {
			if (*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey.IsEqualTo(insertingRemainingKey) {
				// EXPLAIN THE LINE BELOW: cannot delete the old values because it might be used as a value of an another node
				// updateNodeHashesForDelete(nodeHashesForDelete, []*utils.NodeKey{(*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash})

				parentAfterDelete := &((*insertingPointerToSmtBatchNode).parentNode)
				*insertingPointerToSmtBatchNode = nil
				insertingPointerToSmtBatchNode = parentAfterDelete
				if (*insertingPointerToSmtBatchNode) != nil {
					(*insertingPointerToSmtBatchNode).updateHashesAfterDelete()
				}
				insertingNodePathLevel--
				// EXPLAIN THE LINE BELOW: there is no need to update insertingRemainingKey because it is not needed anymore therefore its value is incorrect if used after this line
				// insertingRemainingKey = utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel)
			}

			for {
				// the root node has been deleted so we can safely break
				if *insertingPointerToSmtBatchNode == nil {
					break
				}

				// a leaf (with mismatching remaining key) => nothing to collapse
				if (*insertingPointerToSmtBatchNode).isLeaf() {
					break
				}

				// does not have a single leaf => nothing to collapse
				theSingleNodeLeaf, theSingleNodeLeafDirection := (*insertingPointerToSmtBatchNode).getTheSingleLeafAndDirectionIfAny()
				if theSingleNodeLeaf == nil {
					break
				}

				insertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).collapseLeafByRemovingTheSingleLeaf(insertingNodePath, insertingNodePathLevel, theSingleNodeLeaf, theSingleNodeLeafDirection)
				insertingNodePathLevel--
			}
		}

		if maxInsertingNodePathLevel < insertingNodePathLevel {
			maxInsertingNodePathLevel = insertingNodePathLevel
		}
	}
	select {
	case *progressChan <- uint64(1):
	default:
	}
	stopProgressPrinter()

	if err := s.updateDepth(maxInsertingNodePathLevel); err != nil {
		return nil, fmt.Errorf("updateDepth: %w", err)
	}

	if err := s.deleteBatchedNodeValues(cfg.logPrefix, nodeHashesForDelete); err != nil {
		return nil, fmt.Errorf("deleteBatchedNodeValues: %w", err)
	}

	if err := s.saveBatchedNodeValues(cfg.logPrefix, nodeValues, nodeValuesHashes); err != nil {
		return nil, fmt.Errorf("saveBatchedNodeValues: %w", err)
	}

	if smtBatchNodeRoot == nil {
		rootNodeHash = &utils.NodeKey{0, 0, 0, 0}
	} else {
		sdh := newSmtDfsHelper(s)

		go func() {
			defer sdh.destroy()

			calculateAndSaveHashesDfs(sdh, smtBatchNodeRoot, make([]int, 256), 0)
			rootNodeHash = (*utils.NodeKey)(smtBatchNodeRoot.hash)
		}()

		if !s.noSaveOnInsert {
			if err = sdh.startConsumersLoop(s); err != nil {
				return nil, fmt.Errorf("saving smt hashes dfs: %w", err)
			}
		}
		sdh.wg.Wait()
	}
	if err := s.setLastRoot(*rootNodeHash); err != nil {
		return nil, err
	}

	return &SMTResponse{
		Mode:          "batch insert",
		NewRootScalar: rootNodeHash,
	}, nil
}

// returns the new size of the values batch after removing duplicate entries
func (s *SMT) preprocessBatchedNodeValues(
	logPrefix string,
	shouldPrintProgress bool,
	nodeKeys *[]*utils.NodeKey,
	nodeValues *[]*utils.NodeValue8,
	nodeValuesHashes *[]*[4]uint64,
	rootNodeHash **utils.NodeKey,
) error {
	progressChanPre, stopProgressPrinterPre := getProgressPrinterPre(logPrefix, "pre-process", 4, shouldPrintProgress)
	defer stopProgressPrinterPre()

	if err := validateDataLengths(*nodeKeys, *nodeValues, nodeValuesHashes); err != nil {
		return fmt.Errorf("validateDataLengths: %w", err)
	}
	*progressChanPre <- uint64(1)

	if err := removeDuplicateEntriesByKeys(nodeKeys, nodeValues, nodeValuesHashes); err != nil {
		return fmt.Errorf("removeDuplicateEntriesByKeys: %w", err)
	}
	*progressChanPre <- uint64(1)

	if err := calculateNodeValueHashesIfMissing(*nodeValues, nodeValuesHashes); err != nil {
		return fmt.Errorf("calculateNodeValueHashesIfMissing: %w", err)
	}
	*progressChanPre <- uint64(1)

	if err := calculateRootNodeHashIfNil(s, rootNodeHash); err != nil {
		return fmt.Errorf("calculateRootNodeHashIfNil: %w", err)
	}
	*progressChanPre <- uint64(1)
	stopProgressPrinterPre()

	return nil
}

func (s *SMT) deleteBatchedNodeValues(
	logPrefix string,
	nodeHashesForDelete map[uint64]map[uint64]map[uint64]map[uint64]*utils.NodeKey,
) error {
	progressChanDel, stopProgressPrinterDel := getProgressPrinterPre(logPrefix, "deletes", uint64(len(nodeHashesForDelete)), false)
	defer stopProgressPrinterDel()

	for _, mapLevel0 := range nodeHashesForDelete {
		*progressChanDel <- uint64(1)
		for _, mapLevel1 := range mapLevel0 {
			for _, mapLevel2 := range mapLevel1 {
				for _, nodeHash := range mapLevel2 {
					if err := s.Db.DeleteByNodeKey(*nodeHash); err != nil {
						return fmt.Errorf("DeleteByNodeKey: %w", err)
					}
					if err := s.Db.DeleteHashKey(*nodeHash); err != nil {
						return fmt.Errorf("DeleteHashKey: %w", err)
					}
				}
			}
		}
	}
	stopProgressPrinterDel()

	return nil
}

func (s *SMT) saveBatchedNodeValues(
	logPrefix string,
	nodeValues []*utils.NodeValue8,
	nodeValuesHashes []*[4]uint64,
) error {
	progressChanFin, stopProgressPrinterFin := getProgressPrinterPre(logPrefix, "finalize", uint64(len(nodeValues)), false)
	defer stopProgressPrinterFin()

	for i, nodeValue := range nodeValues {
		select {
		case *progressChanFin <- uint64(1):
		default:
		}
		if !nodeValue.IsZero() {
			if err := s.hashSaveByPointers(nodeValue.ToUintArrayByPointer(), &utils.BranchCapacity, nodeValuesHashes[i]); err != nil {
				return err
			}
		}
	}
	stopProgressPrinterFin()
	return nil
}

func validateDataLengths(
	nodeKeys []*utils.NodeKey,
	nodeValues []*utils.NodeValue8,
	nodeValuesHashes *[]*[4]uint64,
) error {
	var size int = len(nodeKeys)

	if len(nodeValues) != size {
		return fmt.Errorf("mismatch nodeValues length, expected %d but got %d", size, len(nodeValues))
	}

	if (*nodeValuesHashes) == nil {
		(*nodeValuesHashes) = make([]*[4]uint64, size)
	}
	if len(*nodeValuesHashes) != size {
		return fmt.Errorf("mismatch nodeValuesHashes length, expected %d but got %d", size, len(*nodeValuesHashes))
	}

	return nil
}

func removeDuplicateEntriesByKeys(
	nodeKeys *[]*utils.NodeKey,
	nodeValues *[]*utils.NodeValue8,
	nodeValuesHashes *[]*[4]uint64,
) error {
	size := len(*nodeKeys)
	storage := make(map[uint64]map[uint64]map[uint64]map[uint64]int)

	resultNodeKeys := make([]*utils.NodeKey, 0, size)
	resultNodeValues := make([]*utils.NodeValue8, 0, size)
	resultNodeValuesHashes := make([]*[4]uint64, 0, size)

	for i, nodeKey := range *nodeKeys {
		setNodeKeyMapValue(storage, nodeKey, i)
	}

	for i, nodeKey := range *nodeKeys {
		latestIndex, found := getNodeKeyMapValue(storage, nodeKey)
		if !found {
			return fmt.Errorf("key not found")
		}

		if latestIndex == i {
			resultNodeKeys = append(resultNodeKeys, nodeKey)
			resultNodeValues = append(resultNodeValues, (*nodeValues)[i])
			resultNodeValuesHashes = append(resultNodeValuesHashes, (*nodeValuesHashes)[i])
		}
	}

	*nodeKeys = resultNodeKeys
	*nodeValues = resultNodeValues
	*nodeValuesHashes = resultNodeValuesHashes

	return nil
}

func calculateNodeValueHashesIfMissing(
	nodeValues []*utils.NodeValue8,
	nodeValuesHashes *[]*[4]uint64,
) error {
	var globalError error
	size := len(nodeValues)
	cpuNum := parallel.DefaultNumGoroutines()

	if size > (cpuNum << 2) {
		var wg sync.WaitGroup
		itemsPerCpu := (size + cpuNum - 1) / cpuNum

		wg.Add(cpuNum)

		for cpuIndex := 0; cpuIndex < cpuNum; cpuIndex++ {
			go func(cpuIndexInThread int) {
				defer wg.Done()
				startIndex := cpuIndexInThread * itemsPerCpu
				endIndex := startIndex + itemsPerCpu
				if endIndex > size {
					endIndex = size
				}
				err := calculateNodeValueHashesIfMissingInInterval(nodeValues, nodeValuesHashes, startIndex, endIndex)
				if err != nil {
					globalError = err
				}
			}(cpuIndex)
		}

		wg.Wait()
	} else {
		globalError = calculateNodeValueHashesIfMissingInInterval(nodeValues, nodeValuesHashes, 0, len(nodeValues))
	}

	return globalError
}

func calculateNodeValueHashesIfMissingInInterval(
	nodeValues []*utils.NodeValue8,
	nodeValuesHashes *[]*[4]uint64,
	startIndex,
	endIndex int,
) error {
	for i := startIndex; i < endIndex; i++ {
		if (*nodeValuesHashes)[i] != nil {
			continue
		}

		nodeValueHashObj := utils.Hash(nodeValues[i].ToUintArray(), utils.BranchCapacity)
		(*nodeValuesHashes)[i] = &nodeValueHashObj
	}

	return nil
}

func calculateRootNodeHashIfNil(s *SMT, root **utils.NodeKey) error {
	if (*root) == nil {
		oldRootObj, err := s.getLastRoot()
		if err != nil {
			return err
		}
		(*root) = &oldRootObj
	}
	return nil
}

func (s *SMT) findInsertingPoint(
	insertingNodePath []int,
	insertingPointerNodeHash *utils.NodeKey,
	insertingPointerToSmtBatchNode **smtBatchNode,
	fetchDirectSiblings bool,
) (
	insertingNodePathLevel int,
	nextInsertingPointerToSmtBatchNode **smtBatchNode,
	visitedNodeHashes []*utils.NodeKey,
	err error,
) {
	insertingNodePathLevel = -1
	visitedNodeHashes = make([]*utils.NodeKey, 0, 256)

	var (
		insertingPointerToSmtBatchNodeParent *smtBatchNode
		nextInsertingPointerNodeHash         *utils.NodeKey
	)

	for {
		if (*insertingPointerToSmtBatchNode) == nil { // update in-memory structure from db
			if !insertingPointerNodeHash.IsZero() {
				*insertingPointerToSmtBatchNode, err = s.fetchNodeDataFromDb(insertingPointerNodeHash, insertingPointerToSmtBatchNodeParent)
				if err != nil {
					return -2, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, insertingPointerNodeHash)
			} else {
				if insertingNodePathLevel != -1 {
					return -2, insertingPointerToSmtBatchNode, visitedNodeHashes, fmt.Errorf("nodekey is zero at non-root level")
				}
			}
		}

		if (*insertingPointerToSmtBatchNode) == nil {
			if insertingNodePathLevel != -1 {
				return -2, insertingPointerToSmtBatchNode, visitedNodeHashes, fmt.Errorf("working smt pointer is nil at non-root level")
			}
			break
		}

		insertingNodePathLevel++

		if (*insertingPointerToSmtBatchNode).isLeaf() {
			break
		}

		if fetchDirectSiblings {
			// load direct siblings of a non-leaf from the DB
			if (*insertingPointerToSmtBatchNode).leftNode == nil {
				(*insertingPointerToSmtBatchNode).leftNode, err = s.fetchNodeDataFromDb((*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey, (*insertingPointerToSmtBatchNode))
				if err != nil {
					return -2, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, (*insertingPointerToSmtBatchNode).nodeLeftHashOrRemainingKey)
			}
			if (*insertingPointerToSmtBatchNode).rightNode == nil {
				(*insertingPointerToSmtBatchNode).rightNode, err = s.fetchNodeDataFromDb((*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash, (*insertingPointerToSmtBatchNode))
				if err != nil {
					return -2, insertingPointerToSmtBatchNode, visitedNodeHashes, err
				}
				visitedNodeHashes = append(visitedNodeHashes, (*insertingPointerToSmtBatchNode).nodeRightHashOrValueHash)
			}
		}

		insertDirection := insertingNodePath[insertingNodePathLevel]
		nextInsertingPointerNodeHash = (*insertingPointerToSmtBatchNode).getNextNodeHashInDirection(insertDirection)
		nextInsertingPointerToSmtBatchNode = (*insertingPointerToSmtBatchNode).getChildInDirection(insertDirection)
		if nextInsertingPointerNodeHash.IsZero() && (*nextInsertingPointerToSmtBatchNode) == nil {
			break
		}

		insertingPointerNodeHash = nextInsertingPointerNodeHash
		insertingPointerToSmtBatchNodeParent = *insertingPointerToSmtBatchNode
		insertingPointerToSmtBatchNode = nextInsertingPointerToSmtBatchNode
	}

	return insertingNodePathLevel, insertingPointerToSmtBatchNode, visitedNodeHashes, nil
}

func updateNodeHashesForDelete(
	nodeHashesForDelete map[uint64]map[uint64]map[uint64]map[uint64]*utils.NodeKey,
	visitedNodeHashes []*utils.NodeKey,
) {
	for _, visitedNodeHash := range visitedNodeHashes {
		if visitedNodeHash == nil {
			continue
		}

		setNodeKeyMapValue(nodeHashesForDelete, visitedNodeHash, visitedNodeHash)
	}
}

// no point to parallelize this function because db consumer is slower than this producer
func calculateAndSaveHashesDfs(
	sdh *smtDfsHelper,
	smtBatchNode *smtBatchNode,
	path []int,
	level int,
) {
	if smtBatchNode.isLeaf() {
		hashObj, hashValue := utils.HashKeyAndValueByPointers(utils.ConcatArrays4ByPointers(smtBatchNode.nodeLeftHashOrRemainingKey.AsUint64Pointer(), smtBatchNode.nodeRightHashOrValueHash.AsUint64Pointer()), &utils.LeafCapacity)
		smtBatchNode.hash = hashObj
		if !sdh.s.noSaveOnInsert {
			sdh.dataChan <- newSmtDfsHelperDataStruct(hashObj, hashValue)

			nodeKey := utils.JoinKey(path[:level], *smtBatchNode.nodeLeftHashOrRemainingKey)
			sdh.dataChan <- newSmtDfsHelperDataStruct(hashObj, nodeKey)
		}
		return
	}

	var totalHash utils.NodeValue8

	if smtBatchNode.leftNode != nil {
		path[level] = 0
		calculateAndSaveHashesDfs(sdh, smtBatchNode.leftNode, path, level+1)
		totalHash.SetHalfValue(*smtBatchNode.leftNode.hash, 0) // no point to check for error because we used hardcoded 0 which ensures that no error will be returned
	} else {
		totalHash.SetHalfValue(*smtBatchNode.nodeLeftHashOrRemainingKey, 0) // no point to check for error because we used hardcoded 0 which ensures that no error will be returned
	}

	if smtBatchNode.rightNode != nil {
		path[level] = 1
		calculateAndSaveHashesDfs(sdh, smtBatchNode.rightNode, path, level+1)
		totalHash.SetHalfValue(*smtBatchNode.rightNode.hash, 1) // no point to check for error because we used hardcoded 1 which ensures that no error will be returned
	} else {
		totalHash.SetHalfValue(*smtBatchNode.nodeRightHashOrValueHash, 1) // no point to check for error because we used hardcoded 1 which ensures that no error will be returned
	}

	hashObj, hashValue := utils.HashKeyAndValueByPointers(totalHash.ToUintArrayByPointer(), &utils.BranchCapacity)
	if !sdh.s.noSaveOnInsert {
		sdh.dataChan <- newSmtDfsHelperDataStruct(hashObj, hashValue)
	}

	smtBatchNode.hash = hashObj
}

type smtBatchNode struct {
	nodeLeftHashOrRemainingKey *utils.NodeKey
	nodeRightHashOrValueHash   *utils.NodeKey
	leaf                       bool
	parentNode                 *smtBatchNode // if nil => this is the root node
	leftNode                   *smtBatchNode
	rightNode                  *smtBatchNode
	hash                       *[4]uint64
}

func newSmtBatchNodeLeaf(
	nodeLeftHashOrRemainingKey,
	nodeRightHashOrValueHash *utils.NodeKey,
	parentNode *smtBatchNode,
) *smtBatchNode {
	return &smtBatchNode{
		nodeLeftHashOrRemainingKey: nodeLeftHashOrRemainingKey,
		nodeRightHashOrValueHash:   nodeRightHashOrValueHash,
		leaf:                       true,
		parentNode:                 parentNode,
		leftNode:                   nil,
		rightNode:                  nil,
		hash:                       nil,
	}
}

func (s *SMT) fetchNodeDataFromDb(nodeHash *utils.NodeKey, parentNode *smtBatchNode) (*smtBatchNode, error) {
	if nodeHash.IsZero() {
		return nil, nil
	}

	dbNodeValue, err := s.Db.Get(*nodeHash)
	if err != nil {
		return nil, err
	}

	nodeLeftHashOrRemainingKey := utils.NodeKeyFromBigIntArray(dbNodeValue[0:4])
	nodeRightHashOrValueHash := utils.NodeKeyFromBigIntArray(dbNodeValue[4:8])
	return &smtBatchNode{
		parentNode:                 parentNode,
		nodeLeftHashOrRemainingKey: &nodeLeftHashOrRemainingKey,
		nodeRightHashOrValueHash:   &nodeRightHashOrValueHash,
		leaf:                       dbNodeValue.IsFinalNode(),
	}, nil
}

func (sbn *smtBatchNode) isLeaf() bool {
	return sbn.leaf
}

func (sbn *smtBatchNode) getTheSingleLeafAndDirectionIfAny() (*smtBatchNode, int) {
	if sbn.leftNode != nil && sbn.rightNode == nil && sbn.leftNode.isLeaf() {
		return sbn.leftNode, 0
	}
	if sbn.leftNode == nil && sbn.rightNode != nil && sbn.rightNode.isLeaf() {
		return sbn.rightNode, 1
	}
	return nil, -1
}

func (sbn *smtBatchNode) getNextNodeHashInDirection(direction int) *utils.NodeKey {
	if direction == 0 {
		return sbn.nodeLeftHashOrRemainingKey
	} else {
		return sbn.nodeRightHashOrValueHash
	}
}

func (sbn *smtBatchNode) getChildInDirection(direction int) **smtBatchNode {
	if direction == 0 {
		return &sbn.leftNode
	} else {
		return &sbn.rightNode
	}
}

func (sbn *smtBatchNode) updateHashesAfterDelete() {
	if sbn.leftNode == nil {
		sbn.nodeLeftHashOrRemainingKey = &utils.NodeKey{0, 0, 0, 0}
	}
	if sbn.rightNode == nil {
		sbn.nodeRightHashOrValueHash = &utils.NodeKey{0, 0, 0, 0}
	}
}

func (sbn *smtBatchNode) createALeafInEmptyDirection(
	insertingNodePath []int,
	insertingNodePathLevel int,
	insertingNodeKey *utils.NodeKey,
) (**smtBatchNode, error) {
	direction := insertingNodePath[insertingNodePathLevel]
	childPointer := sbn.getChildInDirection(direction)
	if (*childPointer) != nil {
		return nil, fmt.Errorf("branch has already been taken")
	}
	remainingKey := utils.RemoveKeyBits(*insertingNodeKey, insertingNodePathLevel+1)
	*childPointer = newSmtBatchNodeLeaf(&remainingKey, nil, sbn)
	return childPointer, nil
}

func (sbn *smtBatchNode) expandLeafByAddingALeafInDirection(
	insertingNodeKey []int,
	insertingNodeKeyLevel int,
) **smtBatchNode {
	direction := insertingNodeKey[insertingNodeKeyLevel]
	insertingNodeKeyUpToLevel := insertingNodeKey[:insertingNodeKeyLevel]

	childPointer := sbn.getChildInDirection(direction)

	nodeKey := utils.JoinKey(insertingNodeKeyUpToLevel, *sbn.nodeLeftHashOrRemainingKey)
	remainingKey := utils.RemoveKeyBits(*nodeKey, insertingNodeKeyLevel+1)

	*childPointer = newSmtBatchNodeLeaf(&remainingKey, sbn.nodeRightHashOrValueHash, sbn)
	sbn.nodeLeftHashOrRemainingKey = &utils.NodeKey{0, 0, 0, 0}
	sbn.nodeRightHashOrValueHash = &utils.NodeKey{0, 0, 0, 0}
	sbn.leaf = false

	return childPointer
}

func (sbn *smtBatchNode) collapseLeafByRemovingTheSingleLeaf(
	insertingNodeKey []int,
	insertingNodeKeyLevel int,
	theSingleLeaf *smtBatchNode,
	theSingleNodeLeafDirection int,
) **smtBatchNode {
	insertingNodeKeyUpToLevel := insertingNodeKey[:insertingNodeKeyLevel+1]
	insertingNodeKeyUpToLevel[insertingNodeKeyLevel] = theSingleNodeLeafDirection
	nodeKey := utils.JoinKey(insertingNodeKeyUpToLevel, *theSingleLeaf.nodeLeftHashOrRemainingKey)
	remainingKey := utils.RemoveKeyBits(*nodeKey, insertingNodeKeyLevel)

	sbn.nodeLeftHashOrRemainingKey = &remainingKey
	sbn.nodeRightHashOrValueHash = theSingleLeaf.nodeRightHashOrValueHash
	sbn.leaf = true
	sbn.leftNode = nil
	sbn.rightNode = nil

	return &sbn.parentNode
}

type smtDfsHelperDataStruct struct {
	key   *[4]uint64
	value interface{}
}

func newSmtDfsHelperDataStruct(key *[4]uint64, value interface{}) *smtDfsHelperDataStruct {
	return &smtDfsHelperDataStruct{
		key:   key,
		value: value,
	}
}

type smtDfsHelper struct {
	s        *SMT
	dataChan chan *smtDfsHelperDataStruct
	wg       *sync.WaitGroup
	once     *sync.Once
}

func newSmtDfsHelper(s *SMT) *smtDfsHelper {
	sdh := &smtDfsHelper{
		s:        s,
		dataChan: make(chan *smtDfsHelperDataStruct, 1<<16),
		wg:       &sync.WaitGroup{},
		once:     &sync.Once{},
	}

	sdh.wg.Add(1)

	return sdh
}

func (sdh *smtDfsHelper) destroy() {
	sdh.once.Do(func() {
		close(sdh.dataChan)
		sdh.wg.Done()
	})
}

func (sdh *smtDfsHelper) startConsumersLoop(s *SMT) error {
	for {
		dataStruct, ok := <-sdh.dataChan
		if !ok {
			return nil
		}

		switch castedDataStruct := dataStruct.value.(type) {
		case *utils.NodeKey:
			if err := s.Db.InsertHashKey(*dataStruct.key, *castedDataStruct); err != nil {
				return fmt.Errorf("calculating and saving hashes dfs: %w", err)
			}
		case *utils.NodeValue12:
			if err := s.Db.Insert(*dataStruct.key, *castedDataStruct); err != nil {
				return fmt.Errorf("calculating and saving hashes dfs: %w", err)
			}
		}
	}
}

func setNodeKeyMapValue[T int | *utils.NodeKey](
	nodeKeyMap map[uint64]map[uint64]map[uint64]map[uint64]T,
	nodeKey *utils.NodeKey,
	value T,
) {
	mapLevel0, found := nodeKeyMap[nodeKey[0]]
	if !found {
		mapLevel0 = make(map[uint64]map[uint64]map[uint64]T)
		nodeKeyMap[nodeKey[0]] = mapLevel0
	}

	mapLevel1, found := mapLevel0[nodeKey[1]]
	if !found {
		mapLevel1 = make(map[uint64]map[uint64]T)
		mapLevel0[nodeKey[1]] = mapLevel1
	}

	mapLevel2, found := mapLevel1[nodeKey[2]]
	if !found {
		mapLevel2 = make(map[uint64]T)
		mapLevel1[nodeKey[2]] = mapLevel2
	}

	mapLevel2[nodeKey[3]] = value
}

func getNodeKeyMapValue[T int | *utils.NodeKey](
	nodeKeyMap map[uint64]map[uint64]map[uint64]map[uint64]T,
	nodeKey *utils.NodeKey,
) (T, bool) {
	var notExistingValue T

	mapLevel0, found := nodeKeyMap[nodeKey[0]]
	if !found {
		return notExistingValue, false
	}

	mapLevel1, found := mapLevel0[nodeKey[1]]
	if !found {
		return notExistingValue, false
	}

	mapLevel2, found := mapLevel1[nodeKey[2]]
	if !found {
		return notExistingValue, false
	}

	value, found := mapLevel2[nodeKey[3]]
	return value, found
}
