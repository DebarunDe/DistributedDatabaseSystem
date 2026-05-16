package btree

import (
	"encoding/binary"
	"fmt"
	"sync"

	pagemanager "github.com/your-username/DistributedDatabaseSystem/internal/pageManager"
)

const (
	minPageFreeSpace = 2048 // Minimum free space threshold for underflow handling
)

type BTree struct {
	mu sync.Mutex
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
		if err := bt.pm.SetRootPageId(newRoot.GetPageId()); err != nil {
			return fmt.Errorf("failed to set root page ID: %w", err)
		}
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

// redistributeLeaf redistributes records between two sibling leaf pages and updates the parent separator key accordingly. This is to balance the two pages after a deletion
func (bt *BTree) redistributeLeaf(leftPage, rightPage, parentPage *pagemanager.Page) error {
	// Get all records from both pages
	leftRecords := make([][]byte, leftPage.GetRowCount())
	for i := range leftRecords {
		raw, _ := leftPage.GetRecord(i)
		leftRecords[i] = append([]byte(nil), raw...)
	}
	rightRecords := make([][]byte, rightPage.GetRowCount())
	for i := range rightRecords {
		raw, _ := rightPage.GetRecord(i)
		rightRecords[i] = append([]byte(nil), raw...)
	}

	// Both pages are already sorted and all left keys < all right keys,
	// so the concatenation is already in order.
	allRecords := append(leftRecords, rightRecords...)

	// Redistribute records evenly
	mid := len(allRecords) / 2
	leftPage.ClearRecords()
	for i := 0; i < mid; i++ {
		leftPage.InsertRecord(allRecords[i])
	}
	rightPage.ClearRecords()
	for i := mid; i < len(allRecords); i++ {
		rightPage.InsertRecord(allRecords[i])
	}

	// Update parent separator key to be the max key in the left page
	newSeparatorKey := RecordKey(allRecords[mid-1])

	for i := 0; i < int(parentPage.GetRowCount()); i++ {
		rec, ok := parentPage.GetRecord(i)
		if !ok {
			continue
		}
		_, childId, err := DecodeInternalRecord(rec)
		if err != nil {
			continue
		}
		if childId == leftPage.GetPageId() {
			parentPage.DeleteRecord(i)
			parentPage.CompactPage()
			parentPage.InsertRecordAt(findInsertPosition(newSeparatorKey, parentPage), EncodeInternalRecord(newSeparatorKey, leftPage.GetPageId()))
			break
		}
	}

	if err := bt.pm.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write left page during redistribution: %w", err)
	}
	if err := bt.pm.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right page during redistribution: %w", err)
	}
	if err := bt.pm.WritePage(parentPage); err != nil {
		return fmt.Errorf("failed to write parent page during redistribution: %w", err)
	}

	return nil
}

// redistributeInternal redistributes records between two sibling internal pages and updates the parent separator key accordingly to balance the two pages after a deletion
func (bt *BTree) redistributeInternal(leftPage, rightPage, parentPage *pagemanager.Page) error {
	// For internal pages, the parent separator key is NOT stored in either sibling's
	// slot array (unlike leaves, where the separator equals max(leftPage)). Instead, it
	// was pushed up to the parent during a prior splitInternal. We must pull it back
	// down and include it as a bridge entry in the combined sequence, then promote a
	// new boundary entry back to the parent after redistribution.
	var parentSepKey uint64
	parentSepFound := false
	for i := 0; i < int(parentPage.GetRowCount()); i++ {
		rec, ok := parentPage.GetRecord(i)
		if !ok {
			continue
		}
		k, cid, err := DecodeInternalRecord(rec)
		if err != nil {
			continue
		}
		if cid == leftPage.GetPageId() {
			parentSepKey = k
			parentSepFound = true
			break
		}
	}
	if !parentSepFound {
		return fmt.Errorf("redistributeInternal: no parent separator found for leftPage %d", leftPage.GetPageId())
	}

	// Collect slot-array records from both pages.
	leftRecords := make([][]byte, leftPage.GetRowCount())
	for i := range leftRecords {
		raw, _ := leftPage.GetRecord(i)
		leftRecords[i] = append([]byte(nil), raw...)
	}
	rightRecords := make([][]byte, rightPage.GetRowCount())
	for i := range rightRecords {
		raw, _ := rightPage.GetRecord(i)
		rightRecords[i] = append([]byte(nil), raw...)
	}

	// Build the full combined sequence by inserting the pulled-down bridge entry
	// between the two pages' slot arrays:
	//   leftRecords + [(parentSepKey, leftPage.RightMostChild)] + rightRecords
	// The entry at position mid is promoted back to the parent (its key becomes the
	// new separator; its child pointer becomes leftPage's new rightmost child).
	bridgeRecord := EncodeInternalRecord(parentSepKey, leftPage.GetRightMostChild())
	allRecords := make([][]byte, 0, len(leftRecords)+1+len(rightRecords))
	allRecords = append(allRecords, leftRecords...)
	allRecords = append(allRecords, bridgeRecord)
	allRecords = append(allRecords, rightRecords...)

	mid := len(allRecords) / 2

	leftPage.ClearRecords()
	for i := 0; i < mid; i++ {
		leftPage.InsertRecord(allRecords[i])
	}

	// Promote allRecords[mid]: its key rises to the parent as the new separator,
	// and its child pointer becomes leftPage's new rightmost child.
	newSeparatorKey, newRightMostChild, err := DecodeInternalRecord(allRecords[mid])
	if err != nil {
		return fmt.Errorf("redistributeInternal: failed to decode boundary record: %w", err)
	}
	leftPage.SetRightMostChild(newRightMostChild)

	rightPage.ClearRecords()
	for i := mid + 1; i < len(allRecords); i++ {
		rightPage.InsertRecord(allRecords[i])
	}
	// rightPage.RightMostChild is unchanged.

	for i := 0; i < int(parentPage.GetRowCount()); i++ {
		rec, ok := parentPage.GetRecord(i)
		if !ok {
			continue
		}
		_, childId, err := DecodeInternalRecord(rec)
		if err != nil {
			continue
		}
		if childId == leftPage.GetPageId() {
			parentPage.DeleteRecord(i)
			parentPage.CompactPage()
			parentPage.InsertRecordAt(findInsertPosition(newSeparatorKey, parentPage), EncodeInternalRecord(newSeparatorKey, leftPage.GetPageId()))
			break
		}
	}

	if err := bt.pm.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write left page during redistribution: %w", err)
	}
	if err := bt.pm.WritePage(rightPage); err != nil {
		return fmt.Errorf("failed to write right page during redistribution: %w", err)
	}
	if err := bt.pm.WritePage(parentPage); err != nil {
		return fmt.Errorf("failed to write parent page during redistribution: %w", err)
	}

	return nil
}

// mergeLeaf combines two sibling leaf pages into one, frees the empty page, and removes the separator key from the parent. This is used to handle underflow after a deletion.
func (bt *BTree) mergeLeaf(leftPage, rightPage, parentPage *pagemanager.Page) error {
	// Move all records from rightPage into leftPage.
	for i := 0; i < int(rightPage.GetRowCount()); i++ {
		raw, _ := rightPage.GetRecord(i)
		leftPage.InsertRecord(raw)
	}

	// Update siblings to bypass rightPage, which will be freed.
	leftPage.UpdateSiblings(leftPage.GetLeftSibling(), rightPage.GetRightSibling())
	if rightPage.GetRightSibling() != pagemanager.InvalidPageID {
		rightSibling, err := bt.pm.ReadPage(rightPage.GetRightSibling())
		if err != nil {
			return fmt.Errorf("failed to read right sibling during merge: %w", err)
		}
		rightSibling.UpdateSiblings(leftPage.GetPageId(), rightSibling.GetRightSibling())
		if err := bt.pm.WritePage(rightSibling); err != nil {
			return fmt.Errorf("failed to write updated right sibling during merge: %w", err)
		}
	}

	// Remove rightPage from the parent and extend leftPage's separator to cover
	// the combined range.
	//
	// Case A – rightPage is the rightMostChild:
	//   The last slot entry that points to leftPage is no longer needed as a
	//   boundary; remove it and promote leftPage to rightMostChild.
	//
	// Case B – rightPage has its own slot entry (k_R, rightPage):
	//   Repoint that entry to leftPage (key k_R stays – it is now max of the
	//   merged page), then remove leftPage's old, lower-key entry (k_L, leftPage).
	if parentPage.GetRightMostChild() == rightPage.GetPageId() {
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			_, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == leftPage.GetPageId() {
				parentPage.DeleteRecord(i)
				parentPage.CompactPage()
				break
			}
		}
		parentPage.SetRightMostChild(leftPage.GetPageId())
	} else {
		var rightSepKey uint64
		rightSepIdx := -1
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			k, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == rightPage.GetPageId() {
				rightSepKey = k
				rightSepIdx = i
				break
			}
		}
		if rightSepIdx == -1 {
			return fmt.Errorf("mergeLeaf: rightPage %d not found in parent", rightPage.GetPageId())
		}
		// Repoint rightPage's entry to leftPage (raises leftPage's separator to k_R).
		parentPage.DeleteRecord(rightSepIdx)
		parentPage.CompactPage()
		parentPage.InsertRecordAt(findInsertPosition(rightSepKey, parentPage), EncodeInternalRecord(rightSepKey, leftPage.GetPageId()))
		// Remove leftPage's old lower-keyed entry.
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			k, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == leftPage.GetPageId() && k != rightSepKey {
				parentPage.DeleteRecord(i)
				parentPage.CompactPage()
				break
			}
		}
	}

	if err := bt.pm.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write merged page: %w", err)
	}

	if err := bt.pm.FreePage(rightPage.GetPageId()); err != nil {
		return fmt.Errorf("failed to free right page during merge: %w", err)
	}

	if err := bt.pm.WritePage(parentPage); err != nil {
		return fmt.Errorf("failed to write parent page during merge: %w", err)
	}

	return nil
}

// mergeInternal is the internal page version of mergeLeaf, combining two sibling internal pages into one, freeing the empty page, and removing the separator key from the parent to handle underflow after a deletion.
func (bt *BTree) mergeInternal(leftPage, rightPage, parentPage *pagemanager.Page) error {
	// Similar to mergeLeaf, but we must also handle the parent separator key that bridges the two internal pages.
	// Find the parent separator key that bridges leftPage and rightPage.
	var parentSepKey uint64
	parentSepFound := false
	for i := 0; i < int(parentPage.GetRowCount()); i++ {
		rec, ok := parentPage.GetRecord(i)
		if !ok {
			continue
		}
		k, cid, err := DecodeInternalRecord(rec)
		if err != nil {
			continue
		}
		if cid == leftPage.GetPageId() {
			parentSepKey = k
			parentSepFound = true
			break
		}
	}

	if !parentSepFound {
		return fmt.Errorf("mergeInternal: no parent separator found for leftPage %d", leftPage.GetPageId())
	}

	// Move all records from rightPage into leftPage, with the parent separator bridging them.
	bridgeRecord := EncodeInternalRecord(parentSepKey, leftPage.GetRightMostChild())
	leftPage.InsertRecord(bridgeRecord)
	for i := 0; i < int(rightPage.GetRowCount()); i++ {
		raw, _ := rightPage.GetRecord(i)
		leftPage.InsertRecord(raw)
	}
	// rightPage's RMC covers the key range above all of rightPage's slots; the
	// merged page must adopt it so that range remains reachable.
	leftPage.SetRightMostChild(rightPage.GetRightMostChild())

	// Remove rightPage from the parent and update leftPage's separator to cover the combined range.
	// if RightMostChild is rightPage, just promote leftPage to RightMostChild. Otherwise, repoint rightPage's entry to leftPage and remove leftPage's old entry.
	if parentPage.GetRightMostChild() == rightPage.GetPageId() {
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			_, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == leftPage.GetPageId() {
				parentPage.DeleteRecord(i)
				parentPage.CompactPage()
				break
			}
		}
		parentPage.SetRightMostChild(leftPage.GetPageId())
	} else {
		var rightSepKey uint64
		rightSepIdx := -1
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			k, cid, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if cid == rightPage.GetPageId() {
				rightSepKey = k
				rightSepIdx = i
				break
			}
		}

		if rightSepIdx == -1 {
			return fmt.Errorf("mergeInternal: rightPage %d not found in parent", rightPage.GetPageId())
		}
		// Repoint rightPage's entry to leftPage (raises leftPage's separator to k_R).
		parentPage.DeleteRecord(rightSepIdx)
		parentPage.CompactPage()
		parentPage.InsertRecordAt(findInsertPosition(rightSepKey, parentPage), EncodeInternalRecord(rightSepKey, leftPage.GetPageId()))
		// Remove leftPage's old lower-keyed entry.
		for i := 0; i < int(parentPage.GetRowCount()); i++ {
			rec, ok := parentPage.GetRecord(i)
			if !ok {
				continue
			}
			k, childId, err := DecodeInternalRecord(rec)
			if err != nil {
				continue
			}
			if childId == leftPage.GetPageId() && k != rightSepKey {
				parentPage.DeleteRecord(i)
				parentPage.CompactPage()
				break
			}
		}
	}

	if err := bt.pm.WritePage(leftPage); err != nil {
		return fmt.Errorf("failed to write merged page: %w", err)
	}
	if err := bt.pm.FreePage(rightPage.GetPageId()); err != nil {
		return fmt.Errorf("failed to free right page during merge: %w", err)
	}
	if err := bt.pm.WritePage(parentPage); err != nil {
		return fmt.Errorf("failed to write parent page during merge: %w", err)
	}

	return nil
}

// handleUnderflow checks if a page is under minimum occupancy and redistributes the pages or merges them as necessary
func (bt *BTree) handleUnderflow(page *pagemanager.Page, path []uint32) error {
	currPageSize := page.GetFreeSpace()

	if currPageSize > minPageFreeSpace {
		// page is below minimum occupancy, so we need to either redistribute with a sibling or merge with a sibling
		if len(path) == 0 {
			// page is root, so we don't need to worry about underflow
			return nil
		}

		parentId := path[len(path)-1]
		parentPage, err := bt.pm.ReadPage(parentId)
		if err != nil {
			return fmt.Errorf("failed to read parent page during underflow handling: %w", err)
		}

		var siblingId uint32
		var isLeftSibling bool
		if parentPage.GetRightMostChild() == page.GetPageId() {
			// page is rightmost child, so sibling is to the left, get the sibling id by searching for the last record in the parent page
			lastRecord, _ := parentPage.GetRecord(int(parentPage.GetRowCount()) - 1)
			_, siblingId, err = DecodeInternalRecord(lastRecord)
			if err != nil {
				return fmt.Errorf("failed to decode last record in parent page during underflow handling: %w", err)
			}
			isLeftSibling = true
		} else {
			// page is not rightmost child, so sibling is to the right, get the sibling id by searching for the record in the parent page that points to the current page and taking the child pointer from the next record
			for i := 0; i < int(parentPage.GetRowCount()); i++ {
				rec, ok := parentPage.GetRecord(i)
				if !ok {
					continue
				}
				_, childId, err := DecodeInternalRecord(rec)
				if err != nil {
					continue
				}
				if childId == page.GetPageId() {
					// sibling id is the child pointer of the next record, if no next record exist, the sibling is the rightmost child
					if i+1 < int(parentPage.GetRowCount()) {
						nextRec, _ := parentPage.GetRecord(i + 1)
						_, siblingId, err = DecodeInternalRecord(nextRec)
						if err != nil {
							return fmt.Errorf("failed to decode next record in parent page during underflow handling: %w", err)
						}
					} else {
						siblingId = parentPage.GetRightMostChild()
					}

					isLeftSibling = false
					break
				}
			}
		}

		siblingPage, err := bt.pm.ReadPage(siblingId)
		if err != nil {
			return fmt.Errorf("failed to read sibling page during underflow handling: %w", err)
		}

		if siblingPage.GetFreeSpace() < minPageFreeSpace {
			// sibling is dense (has enough records to donate), call the appropriate redistribution function based on page type and parameters based on whether the sibling is to the left or right
			if page.GetPageType() == pagemanager.PageTypeLeaf {
				if isLeftSibling {
					return bt.redistributeLeaf(siblingPage, page, parentPage)
				} else {
					return bt.redistributeLeaf(page, siblingPage, parentPage)
				}
			} else {
				if isLeftSibling {
					return bt.redistributeInternal(siblingPage, page, parentPage)
				} else {
					return bt.redistributeInternal(page, siblingPage, parentPage)
				}
			}
		} else {
			// sibling is also sparse (both pages can fit into one), call the appropriate merge function based on page type and parameters based on whether the sibling is to the left or right
			var mergeErr error
			if page.GetPageType() == pagemanager.PageTypeLeaf {
				if isLeftSibling {
					mergeErr = bt.mergeLeaf(siblingPage, page, parentPage)
				} else {
					mergeErr = bt.mergeLeaf(page, siblingPage, parentPage)
				}
			} else {
				if isLeftSibling {
					mergeErr = bt.mergeInternal(siblingPage, page, parentPage)
				} else {
					mergeErr = bt.mergeInternal(page, siblingPage, parentPage)
				}
			}
			if mergeErr != nil {
				return fmt.Errorf("failed to merge pages during underflow handling: %w", mergeErr)
			}

			// The surviving page is the left operand of the merge call above.
			survivingPageId := page.GetPageId()
			if isLeftSibling {
				survivingPageId = siblingPage.GetPageId()
			}

			// After a merge the parent lost one child entry; check for two cases:
			//   (a) parent is the root and now has no entries, collapse the tree height
			//   (b) parent is a non-root page that now underflows, propagate up
			if parentPage.GetPageId() == bt.pm.GetRootPageId() && parentPage.GetRowCount() == 0 {
				if err := bt.pm.SetRootPageId(survivingPageId); err != nil {
					return fmt.Errorf("failed to update root page ID during underflow handling: %w", err)
				}
				if err := bt.pm.FreePage(parentPage.GetPageId()); err != nil {
					return fmt.Errorf("failed to free old root page during underflow handling: %w", err)
				}
				return nil
			}

			if parentPage.GetPageId() != bt.pm.GetRootPageId() && parentPage.GetFreeSpace() > minPageFreeSpace {
				// parent is non-root and may be underflowed, so we need to recursively handle underflow on the parent page
				return bt.handleUnderflow(parentPage, path[:len(path)-1])
			}
		}
	}

	//Page is above minimum occupancy, so we don't need to do anything
	return nil
}

// Public API
// Search looks for the given key in the B-Tree, returning the record bytes and whether it was found.
func (bt *BTree) Search(key uint64) ([]Field, bool, error) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

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
	bt.mu.Lock()
	defer bt.mu.Unlock()

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

		if err := bt.pm.SetRootPageId(newRoot.GetPageId()); err != nil {
			return fmt.Errorf("failed to set root page ID: %w", err)
		}
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

// Delete removes the record with the given key from the B-Tree, merging or redistributing pages and updating parent nodes as necessary to maintain B-Tree properties.
func (bt *BTree) Delete(key uint64) error {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.pm.GetRootPageId() == pagemanager.InvalidPageID {
		// Tree is empty, nothing to delete.
		return nil
	}

	rootPage, err := bt.pm.ReadPage(bt.pm.GetRootPageId())
	if err != nil {
		return fmt.Errorf("failed to read root page: %w", err)
	}

	leafPage, path, err := bt.findLeafWithPath(key, rootPage)
	if err != nil {
		return fmt.Errorf("failed to find leaf page: %w", err)
	}

	_, isFound := searchLeaf(key, leafPage) //find the position of the record to be deleted in the leaf page
	if !isFound {
		// Key not found, nothing to delete.
		return nil
	}

	// Delete the record from the leaf page.
	insertPos := findInsertPosition(key, leafPage)
	leafPage.DeleteRecord(insertPos)
	leafPage.CompactPage()

	// if leaf page is root, check if empty
	if leafPage.GetPageId() == bt.pm.GetRootPageId() {
		if leafPage.GetRowCount() == 0 {
			// Tree is now empty, so free the root page and reset root page ID.
			if err := bt.pm.FreePage(leafPage.GetPageId()); err != nil {
				return fmt.Errorf("failed to free root page: %w", err)
			}
			if err := bt.pm.SetRootPageId(pagemanager.InvalidPageID); err != nil {
				return fmt.Errorf("failed to reset root page ID: %w", err)
			}
		} else {
			// Just write the updated root page.
			if err := bt.pm.WritePage(leafPage); err != nil {
				return fmt.Errorf("failed to write updated root page: %w", err)
			}
		}
		return nil
	}

	// For non-root leaf, we need to check for underflow and handle it if necessary.
	if leafPage.GetFreeSpace() <= minPageFreeSpace {
		// No underflow, just write the updated leaf page.
		if err := bt.pm.WritePage(leafPage); err != nil {
			return fmt.Errorf("failed to write updated leaf page: %w", err)
		}
		return nil
	} else {
		// Handle underflow, which may involve merging or redistributing with siblings and recursively updating parent nodes.
		return bt.handleUnderflow(leafPage, path)
	}
}

// RangeScan returns all records with keys in the range [startKey, endKey], inclusive. It traverses the leaf pages starting from the leaf page containing startKey and continues until it has passed endKey, collecting records along the way.
func (bt *BTree) RangeScan(startKey, endKey uint64) ([]struct {
	Key    uint64
	Fields []Field
}, error) {
	bt.mu.Lock()
	defer bt.mu.Unlock()

	if bt.pm.GetRootPageId() == pagemanager.InvalidPageID {
		// Tree is empty, return empty result.
		return nil, nil
	}

	rootPage, err := bt.pm.ReadPage(bt.pm.GetRootPageId())
	if err != nil {
		return nil, fmt.Errorf("failed to read root page: %w", err)
	}

	leafPage, err := bt.findLeaf(startKey, rootPage)
	if err != nil {
		return nil, fmt.Errorf("failed to find starting leaf page: %w", err)
	}

	results := make([]struct {
		Key    uint64
		Fields []Field
	}, 0)

	for leafPage.GetPageId() != pagemanager.InvalidPageID {
		for i := 0; i < int(leafPage.GetRowCount()); i++ {
			raw, _ := leafPage.GetRecord(i)
			recKey, fields, err := DecodeLeafRecord(raw)
			if err != nil {
				return nil, fmt.Errorf("failed to decode leaf record: %w", err)
			}

			if recKey > endKey {
				return results, nil
			}

			if recKey >= startKey {
				results = append(results, struct {
					Key    uint64
					Fields []Field
				}{
					Key:    recKey,
					Fields: fields,
				})
			}
		}

		nextPageId := leafPage.GetRightSibling()
		if nextPageId == pagemanager.InvalidPageID {
			break
		}

		leafPage, err = bt.pm.ReadPage(nextPageId)
		if err != nil {
			return nil, fmt.Errorf("failed to read next leaf page: %w", err)
		}
	}

	return results, nil
}
