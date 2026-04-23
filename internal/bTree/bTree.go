package btree

import (
	"encoding/binary"
	"fmt"

	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

type BTree struct {
	pm pagemanager.PageManager
}

func NewBTree(pm pagemanager.PageManager) *BTree {
	return &BTree{pm: pm}
}

// RecordKey extracts just the key from a raw record without decoding the value.
// Used during traversal for comparisons, avoids full decode overhead.
func RecordKey(record []byte) uint64 {
	return binary.BigEndian.Uint64(record[:8])
}

// searchLeaf finds specific key within a leaf page, returning the record bytes and whether it was found.
func searchLeaf(key uint64, page *pagemanager.Page) ([]byte, bool) {
	rowCount := page.GetRowCount()

	// Binary search for the key within the leaf page
	low, high := 0, int(rowCount)-1
	for low <= high {
		mid := (low + high) / 2

		record, found := page.GetRecord(mid)
		if !found {
			return nil, false
		}

		recordKey := RecordKey(record)

		if recordKey == key {
			return record, true
		}

		if recordKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	// Key not found in this leaf page
	return nil, false
}

// searchInternal finds the child page ID to follow for a given key within an internal page.
func searchInternal(key uint64, page *pagemanager.Page) uint32 {
	rowCount := page.GetRowCount()

	// Lower-bound binary search: find first slot where key <= recordKey
	low, high := 0, int(rowCount)-1
	result := int(rowCount)
	for low <= high {
		mid := (low + high) / 2

		record, found := page.GetRecord(mid)
		if !found {
			return pagemanager.InvalidPageID
		}

		recordKey := RecordKey(record)
		if key <= recordKey {
			result = mid
			high = mid - 1
		} else {
			low = mid + 1
		}
	}

	if result == int(rowCount) {
		return page.GetRightMostChild()
	}
	record, _ := page.GetRecord(result)
	_, childID, _ := DecodeInternalRecord(record)
	return childID
}

// findLeaf traverses the B-Tree to find the leaf page that should contain the given key.
func (bt *BTree) findLeaf(key uint64, page *pagemanager.Page) (*pagemanager.Page, error) {
	for page.GetPageType() != pagemanager.PageTypeLeaf {
		childId := searchInternal(key, page)
		var err error
		page, err = bt.pm.ReadPage(childId)
		if err != nil {
			return nil, err
		}
	}
	return page, nil
}

// findLeafWithPath is a modified findLeaf that tracks the traversal path.
// The returned path contains the IDs of every internal node visited (ancestors
// only), with the direct parent of the returned leaf at the end. It does NOT
// include the leaf's own ID, so callers can pass it straight to insertIntoParent.
func (bt *BTree) findLeafWithPath(key uint64, page *pagemanager.Page) (*pagemanager.Page, []uint32, error) {
	path := []uint32{}

	for page.GetPageType() != pagemanager.PageTypeLeaf {
		path = append(path, page.GetPageId())
		childId := searchInternal(key, page)
		var err error
		page, err = bt.pm.ReadPage(childId)
		if err != nil {
			return nil, nil, err
		}
	}

	return page, path, nil
}

// findInsertPosition binary searches the leaf page to find the correct position to insert a new record with the given key.
func findInsertPosition(key uint64, page *pagemanager.Page) int {
	rowCount := page.GetRowCount()

	low, high := 0, int(rowCount)-1
	for low <= high {
		mid := (low + high) / 2

		record, found := page.GetRecord(mid)
		if !found {
			return low // if we can't read the record, treat it as if the key is greater (insert at end)
		}

		recordKey := RecordKey(record)
		if recordKey < key {
			low = mid + 1
		} else {
			high = mid - 1
		}
	}

	return low
}

// splitLeaf splits a full leaf page into two, redistributing records and returning the new page, a separator key, and any error.
func (bt *BTree) splitLeaf(page *pagemanager.Page, newRecord []byte) (*pagemanager.Page, uint64, error) {
	insertPos := findInsertPosition(RecordKey(newRecord), page)
	newPage, err := bt.pm.AllocatePage()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	// set new page type to leaf and set all field
	oldRightSiblingId := page.GetRightSibling()

	*newPage = *pagemanager.NewLeafPage(newPage.GetPageId(), page.GetPageId(), oldRightSiblingId)

	// Update old right sibling to point left to newPage
	if oldRightSiblingId != pagemanager.InvalidPageID {
		oldRightSibling, err := bt.pm.ReadPage(oldRightSiblingId)
		if err != nil {
			return nil, 0, fmt.Errorf("failed to read right sibling: %w", err)
		}
		oldRightSibling.UpdateSiblings(newPage.GetPageId(), oldRightSibling.GetRightSibling())

		if err := bt.pm.WritePage(oldRightSibling); err != nil {
			return nil, 0, fmt.Errorf("failed to write updated right sibling: %w", err)
		}
	}

	page.UpdateSiblings(page.GetLeftSibling(), newPage.GetPageId())

	rowCount := int(page.GetRowCount())
	totalRecords := rowCount + 1
	splitPoint := totalRecords / 2

	// Determine which page receives the new record and build the right-half slice.
	// The old page is full, so we must free space before inserting into it.
	var rightHalf [][]byte

	if insertPos < splitPoint {
		// New record stays in old page.
		// Right half of the combined sequence = original slots [splitPoint-1..rowCount-1].
		rightHalf = make([][]byte, rowCount-splitPoint+1)
		for i := range rightHalf {
			raw, _ := page.GetRecord(splitPoint - 1 + i)
			rightHalf[i] = append([]byte(nil), raw...)
		}
		for i := splitPoint - 1; i < rowCount; i++ {
			page.DeleteRecord(i)
		}
		page.CompactPage()
		if !page.InsertRecordAt(insertPos, newRecord) {
			return nil, 0, fmt.Errorf("InsertRecordAt failed after compaction (page %d)", page.GetPageId())
		}
	} else {
		// New record goes to new page.
		// Right half of the combined sequence = original slots [splitPoint..rowCount-1] + newRecord.
		origRight := make([][]byte, rowCount-splitPoint)
		for i := range origRight {
			raw, _ := page.GetRecord(splitPoint + i)
			origRight[i] = append([]byte(nil), raw...)
		}
		for i := splitPoint; i < rowCount; i++ {
			page.DeleteRecord(i)
		}
		page.CompactPage()
		// Splice newRecord into its position within the right half.
		pos := insertPos - splitPoint
		rightHalf = make([][]byte, len(origRight)+1)
		copy(rightHalf, origRight[:pos])
		rightHalf[pos] = newRecord
		copy(rightHalf[pos+1:], origRight[pos:])
	}

	for _, r := range rightHalf {
		newPage.InsertRecord(r)
	}

	if err := bt.pm.WritePage(page); err != nil {
		return nil, 0, fmt.Errorf("failed to write split left page: %w", err)
	}
	if err := bt.pm.WritePage(newPage); err != nil {
		return nil, 0, fmt.Errorf("failed to write split right page: %w", err)
	}

	// Use max(leftPage) as separator: with key<=recordKey routing, the record
	// (k, leftPage) sends keys <=k to leftPage, so k must be the largest key
	// actually stored there.
	lastLeftRecord, _ := page.GetRecord(int(page.GetRowCount()) - 1)
	separatorKey := RecordKey(lastLeftRecord)
	return newPage, separatorKey, nil
}

// splitInternal splits a full internal page into two, redistributing records and returning the new page, a separator key, and any error.
func (bt *BTree) splitInternal(page *pagemanager.Page, newKey uint64, newChildId uint32) (*pagemanager.Page, uint64, error) {
	insertPos := findInsertPosition(newKey, page)
	newPage, err := bt.pm.AllocatePage()
	if err != nil {
		return nil, 0, fmt.Errorf("failed to allocate new page: %w", err)
	}

	oldRightMostChildId := page.GetRightMostChild()

	*newPage = *pagemanager.NewInternalPage(newPage.GetPageId(), page.GetLevel(), oldRightMostChildId)

	rowCount := int(page.GetRowCount())
	totalRecords := rowCount + 1
	splitPoint := totalRecords / 2

	// Determine which page receives the new key and build the right-half slice.
	var rightHalf [][]byte
	if insertPos < splitPoint {
		// New key stays in old page.
		// Right half of the combined sequence = original slots [splitPoint-1..rowCount-1].
		rightHalf = make([][]byte, rowCount-splitPoint+1)
		for i := range rightHalf {
			raw, _ := page.GetRecord(splitPoint - 1 + i)
			rightHalf[i] = append([]byte(nil), raw...)
		}
		for i := splitPoint - 1; i < rowCount; i++ {
			page.DeleteRecord(i)
		}
		page.CompactPage()

		if !page.InsertRecordAt(insertPos, EncodeInternalRecord(newKey, newChildId)) {
			return nil, 0, fmt.Errorf("InsertRecordAt failed after compaction (page %d)", page.GetPageId())
		}
	} else {
		// New key goes to new page.
		// Right half of the combined sequence = original slots [splitPoint..rowCount-1] + newKey.
		origRight := make([][]byte, rowCount-splitPoint)
		for i := range origRight {
			raw, _ := page.GetRecord(splitPoint + i)
			origRight[i] = append([]byte(nil), raw...)
		}
		for i := splitPoint; i < rowCount; i++ {
			page.DeleteRecord(i)
		}
		page.CompactPage()
		pos := insertPos - splitPoint
		rightHalf = make([][]byte, len(origRight)+1)
		copy(rightHalf, origRight[:pos])
		rightHalf[pos] = EncodeInternalRecord(newKey, newChildId)
		copy(rightHalf[pos+1:], origRight[pos:])
	}

	// rightHalf[0] is the separator: its key is pushed up to the parent and its
	// child pointer becomes the left page's new rightmost child.
	separatorKey, separatorChildId, err := DecodeInternalRecord(rightHalf[0])
	if err != nil {
		return nil, 0, fmt.Errorf("failed to decode separator record: %w", err)
	}
	page.SetRightMostChild(separatorChildId)

	for _, r := range rightHalf[1:] {
		newPage.InsertRecord(r)
	}

	if err := bt.pm.WritePage(page); err != nil {
		return nil, 0, fmt.Errorf("failed to write split left page: %w", err)
	}

	if err := bt.pm.WritePage(newPage); err != nil {
		return nil, 0, fmt.Errorf("failed to write split right page: %w", err)
	}

	return newPage, separatorKey, nil
}

// insertIntoParent handles inserting a new key and child pointer into the parent page after a split, recursively splitting the parent if necessary.
func (bt *BTree) insertIntoParent(leftPage *pagemanager.Page, separatorKey uint64, rightPage *pagemanager.Page, path []uint32) error {
	if len(path) == 0 {
		// We split the root, so we need to create a new root page.
		newRoot, err := bt.pm.AllocatePage()
		if err != nil {
			return fmt.Errorf("failed to allocate new root page: %w", err)
		}

		var newRootLevel uint16
		if leftPage.GetPageType() == pagemanager.PageTypeLeaf {
			newRootLevel = 1
		} else {
			newRootLevel = leftPage.GetLevel() + 1
		}
		*newRoot = *pagemanager.NewInternalPage(newRoot.GetPageId(), newRootLevel, rightPage.GetPageId())
		_, insertSuccess := newRoot.InsertRecord(EncodeInternalRecord(separatorKey, leftPage.GetPageId()))
		if !insertSuccess {
			return fmt.Errorf("failed to insert record into new root page")
		}

		if err := bt.pm.WritePage(newRoot); err != nil {
			return fmt.Errorf("failed to write new root page: %w", err)
		}
		bt.pm.SetRootPageId(newRoot.GetPageId())
		return nil
	}

	parentId := path[len(path)-1]
	path = path[:len(path)-1]
	parentPage, err := bt.pm.ReadPage(parentId)
	if err != nil {
		return fmt.Errorf("failed to read parent page: %w", err)
	}

	// The existing parent entry that routed searches to leftPage must now route
	// them to rightPage (rightPage holds the upper half of leftPage's old range).
	if parentPage.GetRightMostChild() == leftPage.GetPageId() {
		parentPage.SetRightMostChild(rightPage.GetPageId())
	} else {
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			recKey, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == leftPage.GetPageId() {
				parentPage.DeleteRecord(i)
				parentPage.CompactPage()
				parentPage.InsertRecordAt(findInsertPosition(recKey, parentPage), EncodeInternalRecord(recKey, rightPage.GetPageId()))
				break
			}
		}
	}

	separatorRecord := EncodeInternalRecord(separatorKey, leftPage.GetPageId())
	if parentPage.CanAccommodate(len(separatorRecord)) {
		insertSuccess := parentPage.InsertRecordAt(findInsertPosition(separatorKey, parentPage), separatorRecord)
		if !insertSuccess {
			return fmt.Errorf("failed to insert record into parent page")
		}
		if err := bt.pm.WritePage(parentPage); err != nil {
			return fmt.Errorf("failed to write updated parent page: %w", err)
		}
		return nil
	}

	// Otherwise, we need to split the parent as well and recursively insert into its parent.
	// splitInternal already persists both parentPage and newSibling.
	newSibling, newSeparatorKey, err := bt.splitInternal(parentPage, separatorKey, leftPage.GetPageId())
	if err != nil {
		return fmt.Errorf("failed to split internal page: %w", err)
	}

	return bt.insertIntoParent(parentPage, newSeparatorKey, newSibling, path)
}

// Search looks for the given key in the B-Tree, returning the record bytes and whether it was found.
func (bt *BTree) Search(key uint64) ([]Field, bool, error) {
	rootId := bt.pm.GetRootPageId()
	if rootId == pagemanager.InvalidPageID {
		return nil, false, nil
	}

	rootPage, err := bt.pm.ReadPage(rootId)
	if err != nil {
		return nil, false, fmt.Errorf("failed to read root page: %w", err)
	}

	leafPage, err := bt.findLeaf(key, rootPage)
	if err != nil {
		return nil, false, fmt.Errorf("failed to find leaf page: %w", err)
	}

	record, found := searchLeaf(key, leafPage)
	if !found {
		return nil, false, nil //key not found
	}

	_, fields, err := DecodeLeafRecord(record)
	if err != nil {
		return nil, false, fmt.Errorf("failed to decode leaf record: %w", err)
	}

	return fields, true, nil
}

// Insert inerts a new record with given key and value fields into the BTree, splitting pages and updating parent nodes as necessary.
func (bt *BTree) Insert(key uint64, fields []Field) error {
	encodedRecord, err := EncodeLeafRecord(key, fields)
	if err != nil {
		return fmt.Errorf("failed to encode record: %w", err)
	}

	rootId := bt.pm.GetRootPageId()
	if rootId == pagemanager.InvalidPageID {
		// Tree is empty, so create a new leaf page as the root.
		newRoot, err := bt.pm.AllocatePage()
		if err != nil {
			return fmt.Errorf("failed to allocate new root page: %w", err)
		}

		*newRoot = *pagemanager.NewLeafPage(newRoot.GetPageId(), pagemanager.InvalidPageID, pagemanager.InvalidPageID)
		_, insertSuccess := newRoot.InsertRecord(encodedRecord)
		if !insertSuccess {
			return fmt.Errorf("failed to insert record into new root page")
		}

		if err := bt.pm.WritePage(newRoot); err != nil {
			return fmt.Errorf("failed to write new root page: %w", err)
		}

		bt.pm.SetRootPageId(newRoot.GetPageId())
		return nil
	}

	rootPage, err := bt.pm.ReadPage(rootId)
	if err != nil {
		return fmt.Errorf("failed to read root page: %w", err)
	}

	leafPage, path, err := bt.findLeafWithPath(key, rootPage)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	// check if the key already exists in the leaf page
	if _, found := searchLeaf(key, leafPage); found {
		insertPos := findInsertPosition(key, leafPage)
		leafPage.DeleteRecord(insertPos)
		leafPage.CompactPage()
		insertPos = findInsertPosition(key, leafPage) //recalculate insert position after deletion and compaction

		if leafPage.InsertRecordAt(insertPos, encodedRecord) {
			if err := bt.pm.WritePage(leafPage); err != nil {
				return fmt.Errorf("failed to write updated leaf page: %w", err)
			}
			return nil
		}

		// New value is larger than old + free space; split and propagate.
		newSibling, separatorKey, err := bt.splitLeaf(leafPage, encodedRecord)
		if err != nil {
			return fmt.Errorf("failed to split leaf page during update: %w", err)
		}
		return bt.insertIntoParent(leafPage, separatorKey, newSibling, path)
	}

	//find the correct position to insert the new record in the leaf page
	insertPos := findInsertPosition(key, leafPage)

	// Try to insert the new record into the leaf page. If it fits, we're done.
	if leafPage.CanAccommodate(len(encodedRecord)) {
		insertSuccess := leafPage.InsertRecordAt(insertPos, encodedRecord)
		if !insertSuccess {
			return fmt.Errorf("failed to insert record into leaf page")
		}
		if err := bt.pm.WritePage(leafPage); err != nil {
			return fmt.Errorf("failed to write updated leaf page: %w", err)
		}
		return nil
	}

	// Otherwise, we need to split the leaf page and then insert a new separator key into the parent.
	newSibling, separatorKey, err := bt.splitLeaf(leafPage, encodedRecord)
	if err != nil {
		return fmt.Errorf("failed to split leaf page: %w", err)
	}

	return bt.insertIntoParent(leafPage, separatorKey, newSibling, path)
}
