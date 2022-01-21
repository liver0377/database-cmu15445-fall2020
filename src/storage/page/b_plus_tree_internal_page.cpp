//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/page/b_plus_tree_internal_page.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

/**
 * 内部页不存储任何实际数据，而是存储有序的m个键条目(key entries)和m+1个子指针(又名page_id)。
 * 由于指针的数量不等于键的数量，因此将第一个键设置为无效，查找方法应始终从第二个键开始。
 * 在任何时候，每个内部页面至少是半满的。在删除过程中，可以将两个半满的页面合并成一个合法的页面，也可以重新分配以避免合并，
 * 而在插入过程中，可以将一个完整的页面一分为二。
 * 总结：
 * 在插入删除的时候检查节点大小，如果溢出则进行拆分(Split)，不足则视兄弟节点情况进行合并或者重分配。
 * 在实现的时候注意维护好有序关系以及KV对应关系
 * 注意：
 * 内部页面的第一个key（即array[0]）是无效的，任何search/lookup都忽略第一个key
 */

#include <iostream>
#include <sstream>

#include "common/exception.h"
#include "storage/page/b_plus_tree_internal_page.h"

namespace bustub {
/*****************************************************************************
 * HELPER METHODS AND UTILITIES
 *****************************************************************************/
/*
 * Init method after creating a new internal page
 * Including set page type, set current size, set page id, set parent id and set
 * max page size
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Init(page_id_t page_id, page_id_t parent_id, int max_size) {
  // 用于初始化
  // 缺省：page_id_t parent_id = INVALID_PAGE_ID, int max_size = INTERNAL_PAGE_SIZE);
  // 注意这里并没有实际为array分配空间，因为Internal page使用的时候就是将Page经过reinterpret_cast转换得到
  // 所以实际上internal page的作用就是维护一些元数据，内部的真实tuple直接维持Page对象中的原样即可
  SetPageId(page_id);
  SetSize(0);
  SetPageType(IndexPageType::INTERNAL_PAGE);
  SetMaxSize(max_size);
  SetParentPageId(parent_id);
}
/*
 * Helper method to get/set the key associated with input "index"(a.k.a
 * array offset)
 */
INDEX_TEMPLATE_ARGUMENTS
KeyType B_PLUS_TREE_INTERNAL_PAGE_TYPE::KeyAt(int index) const {
  // replace with your own code
  // 用于返回index处的key
  return array_[index].first;
}

INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::SetKeyAt(int index, const KeyType &key) {
  // 用于设置index处的key
  array_[index].first = key;
}

/*
 * 找到value对应的下标
 * Helper method to find and return array index(or offset), so that its value
 * equals to input "value"
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueIndex(const ValueType &value) const {
  // 对于内部页面，key有序可以比较，但value无法比较，只能顺序查找
  // 这里的value是指page id
  for (int i = 0; i < this->GetSize(); i++) {
    if (array_[i].second == value) {
      return i;
    }
  }
  return -1;
}

/*
 * Helper method to get the value associated with input "index"(a.k.a array
 * offset)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::ValueAt(int index) const {
  // 返回index处的value
  return array_[index].second;
}

/*****************************************************************************
 * LOOKUP 查找key应该在哪个value指向的子树中
 *****************************************************************************/
/*
 * 查找internal page的array中第一个>key(注意不是>=)的下标，然后据其确定value
 * 注意：value指向的是子树，或者说指向的是当前内部结点的下一层某个结点
 * 假设arraty_[i]的子树中的所有key为subtree(value(i))，array_[i]的关键字为key(i)
 * 那么满足 key(i) <= subtree(value(i)) < key(i + 1)
 * 其实就是任意两个key之间的指针所指向的子树的key均位于它们之间
 * Find and return the child pointer(page_id) which points to the child page
 * that contains input "key"
 * Start the search from the second key(the first key should always be invalid)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::Lookup(const KeyType &key, const KeyComparator &comparator) const {
  // 查找内部节点中最后一个<=给定key的下标
  // 正常来说下标范围是[0,size-1]，但是0位置设为无效
  // 所以直接从1位置开始，作为下界，下标范围是[1,size-1]
  // 这里手写二分查找upper_bound，速度快于for循环的顺序查找
  // array类型为std::pair<KeyType, ValueType>
  // 正常来说下标范围是[0,size-1]，但是0位置设为无效
  // 所以直接从1位置开始，作为下界，下标范围是[1,size-1]
  // assert(GetSize() >= 1);  // 这里总是容易出现错误
  int left = 1;
  int right = GetSize() - 1;
  while (left <= right) {
    int mid = left + (right - left) / 2;
    if (comparator(KeyAt(mid), key) > 0) {  // 下标还需要减小
      right = mid - 1;
    } else {  // 下标还需要增大
      left = mid + 1;
    }
  }  // upper_bound
  int target_index = left;
  assert(target_index - 1 >= 0);
  // 注意，返回的value下标要减1，这样才能满足key(i-1) <= subtree(value(i)) < key(i)
  return ValueAt(target_index - 1);
}

/*****************************************************************************
 * INSERTION 将当前page重置为2个value+1个key（size=2）
 *****************************************************************************/
/*
 * Populate(填充) new root page with old_value + new_key & new_value
 * When the insertion cause overflow from leaf page all the way upto the root
 * page, you should create a new root page and populate its elements.
 * NOTE: This method is only called within InsertIntoParent()(b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::PopulateNewRoot(const ValueType &old_value, const KeyType &new_key,
                                                     const ValueType &new_value) {
  // 只有root节点能够使用该函数，即该函数就是用来填充自身的page的
  array_[0] = {KeyType(), old_value};
  array_[1] = {new_key, new_value};
  SetSize(2);
}
/*
 * Insert new_key & new_value pair right after the pair with its value ==
 * old_value
 * @return:  new size after insertion
 */
INDEX_TEMPLATE_ARGUMENTS
int B_PLUS_TREE_INTERNAL_PAGE_TYPE::InsertNodeAfter(const ValueType &old_value, const KeyType &new_key,
                                                    const ValueType &new_value) {
  // 在紧接着value = old_value的pair后面再插一个新的value
  // 情景1: 节点中存在old_value, 并且节点中还有剩余空间，插入成功
  // 情景2：节点中没有剩余空间，插入失败
  // 情景3：节点中没有old_value, 插入失败
  // 插入失败时，返回原size
  // 此时没有考虑分裂
  int insert_index = ValueIndex(old_value);  // 得到 =old_value 的下标
  // assert(insert_index != -1);                // 下标存在
  insert_index++;  // 插入位置在 =old_value的下标 的后面一个
  // 数组下标>=insert_index的元素整体后移1位
  // [insert_index, size - 1] --> [insert_index + 1, size]
  for (int i = GetSize(); i > insert_index; i--) {
    // 这里不考虑越界问题
    array_[i] = array_[i - 1];
  }
  array_[insert_index] = MappingType{new_key, new_value};  // insert pair
  IncreaseSize(1);
  return GetSize();
}

/*****************************************************************************
 * SPLIT
 * 上层调用：
 * this page是old_node，recipient page是new_node
 * old_node的右半部分array移动给new_node
 * 并且，将new_node（原old_node的右半部分）的所有孩子结点的父指针更新为指向new_node
 *****************************************************************************/
/*
 * Remove half of key & value pairs from this page to "recipient" page
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveHalfTo(BPlusTreeInternalPage *recipient,
                                                BufferPoolManager *buffer_pool_manager) {
  // 将当前节点右边的一半复制给新的节点, 即recipient
  // 虽说是复制，但是其实这些pair已经不属于自己了，所以是MoveHalfTo

  int start_index = GetMinSize();
  int size = GetSize() - start_index;
  recipient->CopyNFrom(array_ + start_index, size, buffer_pool_manager);
  // 记得改变size
  IncreaseSize(-size);
}

/*
 * 从items指向的位置开始，复制size个，到当前调用该函数的page的array尾部（本函数由recipient page调用）
 * 并且，找到调用该函数的page的array中每个value指向的孩子结点，其父指针更新为调用该函数的page id
 * Copy entries into me, starting from {items} and copy {size} entries.
 * Since it is an internal page, for all entries (pages) moved, their parents page now changes to me.
 * So I need to 'adopt' them by changing their parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyNFrom(MappingType *items, int size, BufferPoolManager *buffer_pool_manager) {
  // 该函数将items所指节点的pair对拷贝size个到当前node, 原来节点中的对应pair不需要删除，但是其孩子的父节点需要重新改变
  for (int i = GetSize(); i < GetSize() + size; i++) {
    array_[i] = items[i - GetSize()];
    auto child_page = buffer_pool_manager->FetchPage(array_[i].second);
    auto child_node = reinterpret_cast<BPlusTreeInternalPage *>(child_page);
    // 修正子节点的parent page
    child_node->SetParentPageId(GetPageId());
    buffer_pool_manager->UnpinPage(array_[i].second, true);
  }
  IncreaseSize(size);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Remove the key & value pair in internal page according to input index(a.k.a
 * array offset)
 * NOTE: store key&value pair continuously after deletion
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::Remove(int index) {
  // 删除给定index的pair
  if (index < 0 || index >= GetSize()) {
    return;
  }
  for (int i = index; i < GetSize(); i++) {
    array_[i] = array_[i + 1];
  }
  IncreaseSize(-1);
}

/*
 * Remove the only key & value pair in internal page and return the value
 * NOTE: only call this method within AdjustRoot()(in b_plus_tree.cpp)
 */
INDEX_TEMPLATE_ARGUMENTS
ValueType B_PLUS_TREE_INTERNAL_PAGE_TYPE::RemoveAndReturnOnlyChild() {
  auto ans = array_[0].second;
  Remove(0);
  return ans;
}
/*****************************************************************************
 * MERGE
 *****************************************************************************/
/*
 * Remove all of key & value pairs from this page to "recipient" page.
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveAllTo(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                               BufferPoolManager *buffer_pool_manager) {
  // 当前node的第一个key(即array[0].first)本是无效值(因为是内部结点)，但由于要移动当前node的整个array到recipient
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为index的middle_key
  SetKeyAt(0, middle_key);  // 将分隔key设置在0的位置
  recipient->CopyNFrom(array_, GetSize(), buffer_pool_manager);
  SetSize(0);
}

/*****************************************************************************
 * REDISTRIBUTE
 *****************************************************************************/
/*
 * Remove the first key & value pair from this page to tail of "recipient" page.
 *
 * The middle_key is the separation key you should get from the parent. You need
 * to make sure the middle key is added to the recipient to maintain the invariant.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those
 * pages that are moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveFirstToEndOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                      BufferPoolManager *buffer_pool_manager) {
  // 将当前节点的首部的pair移动到recipient
  // 当前node的第一个key本是无效值(因为是内部结点)，但由于要移动当前node的array[0]到recipient尾部
  // 那么必须在移动前将当前node的第一个key 赋值为 父结点中下标为1的middle_key
  SetKeyAt(0, middle_key);
  // first item (array[0]) of this page array copied to recipient page last
  // 此时的array_[0]是{middle_key, array_[0].second}
  recipient->CopyLastFrom(array_[0], buffer_pool_manager);
  // delete array[0]
  Remove(0);  // 函数复用
}

/* Append an entry at the end.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyLastFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 在当前节点的尾部新加一个条目
  array_[GetSize()] = pair;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(GetSize()));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
}

/*
 * Remove the last key & value pair from this page to head of "recipient" page.
 * You need to handle the original dummy key properly, e.g. updating recipient’s array to position the middle_key at the
 * right place.
 * You also need to use BufferPoolManager to persist changes to the parent page id for those pages that are
 * moved to the recipient
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::MoveLastToFrontOf(BPlusTreeInternalPage *recipient, const KeyType &middle_key,
                                                       BufferPoolManager *buffer_pool_manager) {
  recipient->SetKeyAt(0, middle_key);
  auto last_pair = array_[GetSize() - 1];
  recipient->CopyFirstFrom(last_pair, buffer_pool_manager);
  IncreaseSize(-1);
}

/* Append an entry at the beginning.
 * Since it is an internal page, the moved entry(page)'s parent needs to be updated.
 * So I need to 'adopt' it by changing its parent page id, which needs to be persisted with BufferPoolManger
 */
INDEX_TEMPLATE_ARGUMENTS
void B_PLUS_TREE_INTERNAL_PAGE_TYPE::CopyFirstFrom(const MappingType &pair, BufferPoolManager *buffer_pool_manager) {
  // 将pair拷贝到array_的首部
  // move array after index=0 to back by 1 size
  for (int i = GetSize(); i >= 0; i--) {
    array_[i + 1] = array_[i];
  }
  // insert item to array[0]
  array_[0] = pair;

  // update parent page id of child page
  Page *child_page = buffer_pool_manager->FetchPage(ValueAt(0));
  BPlusTreePage *child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());
  child_node->SetParentPageId(GetPageId());
  buffer_pool_manager->UnpinPage(child_page->GetPageId(), true);

  IncreaseSize(1);
}

// valuetype for internalNode should be page id_t
template class BPlusTreeInternalPage<GenericKey<4>, page_id_t, GenericComparator<4>>;
template class BPlusTreeInternalPage<GenericKey<8>, page_id_t, GenericComparator<8>>;
template class BPlusTreeInternalPage<GenericKey<16>, page_id_t, GenericComparator<16>>;
template class BPlusTreeInternalPage<GenericKey<32>, page_id_t, GenericComparator<32>>;
template class BPlusTreeInternalPage<GenericKey<64>, page_id_t, GenericComparator<64>>;
}  // namespace bustub
