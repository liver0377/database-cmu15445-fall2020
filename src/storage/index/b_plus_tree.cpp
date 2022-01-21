//===----------------------------------------------------------------------===//
//
//                         CMU-DB Project (15-445/645)
//                         ***DO NO SHARE PUBLICLY***
//
// Identification: src/index/b_plus_tree.cpp
//
// Copyright (c) 2018, Carnegie Mellon University Database Group
//
//===----------------------------------------------------------------------===//

#include <string>

#include "common/exception.h"
#include "common/rid.h"
#include "storage/index/b_plus_tree.h"
#include "storage/page/header_page.h"

namespace bustub {
INDEX_TEMPLATE_ARGUMENTS
BPLUSTREE_TYPE::BPlusTree(std::string name, BufferPoolManager *buffer_pool_manager, const KeyComparator &comparator,
                          int leaf_max_size, int internal_max_size)
    : index_name_(std::move(name)),
      root_page_id_(INVALID_PAGE_ID),
      buffer_pool_manager_(buffer_pool_manager),
      comparator_(comparator),
      leaf_max_size_(leaf_max_size),
      internal_max_size_(internal_max_size) {}

/*
 * Helper function to decide whether current b+tree is empty
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::IsEmpty() const { return root_page_id_ == INVALID_PAGE_ID; }
/*****************************************************************************
 * SEARCH 最终要实现的目标函数之一
 *****************************************************************************/
/*
 * project2 检查点1
 * B+树的点查询。《数据库系统概念》P431有伪代码
 * 功能：查询key在对应leaf page中的value，并将value存入result
 * Return the only value that associated with input key
 * This method is used for point query
 * @return : true means key exists
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::GetValue(const KeyType &key, std::vector<ValueType> *result, Transaction *transaction) {
  // 1 先找到leaf page，这里面会调用fetch page
  auto leaf_page = reinterpret_cast<LeafPage *>(FindLeafPageByOperation(key, Operation::FIND, transaction).first);
  // 为空说明fetch失败
  if (leaf_page == nullptr) {
    return false;
  }
  // 2 在leaf page里找这个key
  ValueType temp;
  bool ans = leaf_page->Lookup(key, &temp, comparator_);
  // 3 page用完后记得unpin page
  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), false);
  // 将得到的value添加到result中
  if (ans) {
    result->push_back(temp);
  }
  return ans;
}

/*****************************************************************************
 * INSERTION 最终要实现的目标函数之一
 *****************************************************************************/
/*
 * project2 检查点1
 * Insert constant key & value pair into b+ tree
 * if current tree is empty, start new tree, update root page id and insert
 * entry, otherwise insert into leaf page.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::Insert(const KeyType &key, const ValueType &value, Transaction *transaction) {
  // 将{key, value} 插入树中
  // 如果已经key已经存在， 那么返回false, 否则返回true
  if (IsEmpty()) {
    StartNewTree(key, value);
    return true;
  }
  return InsertIntoLeaf(key, value, transaction);
}
/*
 * 创建新树，即创建root page
 * Insert constant key & value pair into an empty tree
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then update b+
 * tree's root page id and insert entry directly into leaf page.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::StartNewTree(const KeyType &key, const ValueType &value) {
  LOG_INFO("StartNewTree() begin, new tree is set");
  // 创建一棵新树，将{key, value}插入
  // 1.向buffer pool申请一个page用做root page
  page_id_t root_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&root_page_id);
  if (new_page == nullptr) {
    std::runtime_error("out of memory");
  }
  // 2.更新root_page_id_
  root_page_id_ = root_page_id;
  //参数1代表在root page中插入pair而不是更新pair
  UpdateRootPageId(1);
  // 3.插入新pair
  auto leaf_page = reinterpret_cast<LeafPage *>(new_page);
  leaf_page->Init(root_page_id, INVALID_PAGE_ID, leaf_max_size_);
  leaf_page->Insert(key, value, comparator_);
  buffer_pool_manager_->UnpinPage(root_page_id, true);
}

/*
 * Insert constant key & value pair into leaf page
 * User needs to first find the right leaf page as insertion target, then look
 * through leaf page to see whether insert key exist or not. If exist, return
 * immdiately, otherwise insert entry. Remember to deal with split if necessary.
 * @return: since we only support unique key, if user try to insert duplicate
 * keys return false, otherwise return true.
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::InsertIntoLeaf(const KeyType &key, const ValueType &value, Transaction *transaction) {
  LOG_INFO("InsertIntoLeaf()  begin");
  // 专用于向叶子节点中插入的函数
  // 1.根据key找到待插入的叶子节点
  auto [leaf_page, root_is_latched] = FindLeafPageByOperation(key, Operation::FIND, transaction);
  LeafPage *leaf_node = reinterpret_cast<LeafPage *>(leaf_page->GetData());
  int size = leaf_node->GetSize();

  // 2. 插入{key, value}
  int new_size = leaf_node->Insert(key, value, comparator_);
  // 2.1有重复的key, 插入失败
  if (new_size == size) {
    // 在FindLeafPageByOperation中fetch了叶子节点
    buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
    LOG_INFO("有重复的key, 插入失败");
    return false;
  }
  // 2.2插入成功，并且不需要进行分裂
  if (new_size < leaf_node->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(leaf_node->GetPageId(), true);
    LOG_INFO("插入成功，不需要进行分裂");
    return true;
  }
  // 2.3插入成功， 但是需要进行分裂(new_size = left_node->GetMaxSize())
  // 分裂当前叶子节点
  LOG_INFO("分裂当前叶子节点, page_id = %d", leaf_node->GetPageId());
  LeafPage *new_leaf_node = Split(leaf_node);
  // 将新节点中的最小key送往parent
  // 锁的问题暂且搁置
  bool *pointer_root_is_latched = new bool(root_is_latched);

  InsertIntoParent(leaf_node, new_leaf_node->KeyAt(0), new_leaf_node, transaction, pointer_root_is_latched);
  delete pointer_root_is_latched;

  buffer_pool_manager_->UnpinPage(leaf_page->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(new_leaf_node->GetPageId(), true);

  return true;
}

/*
 * 将传入的一个node拆分(Split)成两个结点，会产生一个新结点
 * 注意要区分叶子结点和内部结点
 * 如果node为internal page，则产生的新结点作为其孩子结点（疑问：？）
 * 如果node为leaf page，则产生的新结点要连接原结点，即更新这两个结点的next page id
 * Split input page and return newly created page.
 * Using template N to represent either internal page or leaf page.
 * User needs to first ask for new page from buffer pool manager(NOTICE: throw
 * an "out of memory" exception if returned value is nullptr), then move half
 * of key & value pairs from input page to newly created page
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
N *BPLUSTREE_TYPE::Split(N *node) {
  // 该函数用于在进行insert操作而节点满了的情况下会会使用到
  // 分为叶子节点和内部节点两种情况
  page_id_t new_page_id;
  auto new_page = buffer_pool_manager_->NewPage(&new_page_id);
  if (new_page == nullptr) {
    return nullptr;
  }
  // new_node->SetPageType(old_node->GetPageType());
  N *ans;
  // 叶子节点
  if (node->IsLeafPage()) {
    LeafPage *old_node = reinterpret_cast<LeafPage *>(node);
    LeafPage *new_node = reinterpret_cast<LeafPage *>(new_page->GetData());
    // 新产生的叶子节点和旧节点具有相同的parent
    new_node->Init(new_page_id, old_node->GetParentPageId(), leaf_max_size_);
    // 将旧节点的后一半数据拷贝到新节点
    old_node->MoveHalfTo(new_node);
    //更新叶子结点的链表指针
    new_node->SetNextPageId(old_node->GetNextPageId());
    old_node->SetNextPageId(new_page_id);
    ans = reinterpret_cast<N *>(new_node);
  } else {
    // 内部节点
    // 内部节点相对于叶子节点少了更新链表的过程
    InternalPage *old_node = reinterpret_cast<InternalPage *>(node);
    InternalPage *new_node = reinterpret_cast<InternalPage *>(new_page->GetData());
    new_node->Init(new_page_id, old_node->GetParentPageId(), internal_max_size_);
    old_node->MoveHalfTo(new_node, buffer_pool_manager_);
    ans = reinterpret_cast<N *>(new_node);
  }
  return ans;
}

/*
 * 拆分(Split)后，向上找到old_node的父结点
 * 将new_node的第一个key插入到父结点，其位置在 父结点指向old_node的孩子指针 之后
 * 如果插入后>maxsize，则必须继续拆分父结点，然后在其父结点的父结点再插入，即需要递归
 * 直到找到的old_node为根结点时，结束递归（此时将会新建一个根R，关键字为key，old_node和new_node为其孩子）
 * Insert key & value pair into internal page after split
 * @param   old_node      input page from split() method
 * @param   key
 * @param   new_node      returned page from split() method
 * User needs to first find the parent page of old_node, parent node must be
 * adjusted to take info of new_node into account. Remember to deal with split
 * recursively if necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertIntoParent(BPlusTreePage *old_node, const KeyType &key, BPlusTreePage *new_node,
                                      Transaction *transaction, bool *root_is_latched) {
  LOG_INFO("InsertIntoParent() begin");
  // 1.如果old_node是根节点，那么就需要创建一个新的根节点
  if (old_node->IsRootPage()) {
    LOG_INFO("old_node 为根节点, 准备创建新节点");
    page_id_t new_page_id = INVALID_PAGE_ID;
    Page *new_page = buffer_pool_manager_->NewPage(&new_page_id);
    InternalPage *new_root_page = reinterpret_cast<InternalPage *>(new_page->GetData());
    // 更新root
    root_page_id_ = new_page_id;
    // 更新header page中的root page
    UpdateRootPageId(0);
    new_root_page->Init(root_page_id_, INVALID_PAGE_ID, internal_max_size_);
    // 修改根节点的指针
    new_root_page->PopulateNewRoot(old_node->GetPageId(), key, new_node->GetPageId());
    // 修改孩子的parent id
    old_node->SetParentPageId(new_page_id);
    new_node->SetParentPageId(new_page_id);

    // 这里只Unpin root page即可，其余两个子page留在insert主函数进行Unpin
    buffer_pool_manager_->UnpinPage(new_page_id, true);
    LOG_INFO("new root is setted, new_page_id is %d", new_page_id);
    return;
  }
  // 2.old_node不是根节点， 那么直接将{key, new_node->GetPageId()}插入父节点
  // 此时父节点可能会满，如果父节点在插入之前已经满了，那么就需要进行Split, 得到
  // 新节点， 再次调用InsertIntoParent进行递归操作
  LOG_INFO("old_node为非根节点");
  Page *parent_page = buffer_pool_manager_->FetchPage(old_node->GetParentPageId());
  InternalPage *parent_node = reinterpret_cast<InternalPage *>(parent_page);
  // 将{key, new_node->GetPageId()}插入父亲节点
  // 注意， new_node一定是紧插在old_node之后的
  parent_node->InsertNodeAfter(old_node->GetPageId(), key, new_node->GetPageId());

  // 父节点插入之前没有满
  if (parent_node->GetSize() < parent_node->GetMaxSize()) {
    buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
    return;
  }
  // 父节点插入之前已经满了
  // 分裂出新节点
  LOG_INFO("父节点满， 准备分裂");
  InternalPage *parent_new_node = Split(parent_node);
  bool *pointer_root_is_latched = new bool(root_is_latched);
  InsertIntoParent(parent_node, parent_new_node->KeyAt(0), parent_new_node);
  delete pointer_root_is_latched;

  buffer_pool_manager_->UnpinPage(parent_node->GetPageId(), true);
  buffer_pool_manager_->UnpinPage(parent_new_node->GetPageId(), true);
}

/*****************************************************************************
 * REMOVE
 *****************************************************************************/
/*
 * Delete key & value pair associated with input key
 * If current tree is empty, return immdiately.
 * If not, User needs to first find the right leaf page as deletion target, then
 * delete entry from leaf page. Remember to deal with redistribute or merge if
 * necessary.
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::Remove(const KeyType &key, Transaction *transaction) {}

/*
 * User needs to first find the sibling of input page. If sibling's size + input
 * page's size > page's max size, then redistribute. Otherwise, merge.
 * Using template N to represent either internal page or leaf page.
 * @return: true means target leaf page should be deleted, false means no
 * deletion happens
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::CoalesceOrRedistribute(N *node, Transaction *transaction, bool *root_is_latched) {
  return false;
}

/*
 * Move all the key & value pairs from one page to its sibling page, and notify
 * buffer pool manager to delete this page. Parent page must be adjusted to
 * take info of deletion into account. Remember to deal with coalesce or
 * redistribute recursively if necessary.
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 * @param   parent             parent page of input "node"
 * @return  true means parent node should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::Coalesce(N **neighbor_node, N **node,
                              BPlusTreeInternalPage<KeyType, page_id_t, KeyComparator> **parent, int index,
                              Transaction *transaction, bool *root_is_latched) {
  return false;
}

/*
 * Redistribute key & value pairs from one page to its sibling page. If index ==
 * 0, move sibling page's first key & value pair into end of input "node",
 * otherwise move sibling page's last key & value pair into head of input
 * "node".
 * Using template N to represent either internal page or leaf page.
 * @param   neighbor_node      sibling page of input "node"
 * @param   node               input from method coalesceOrRedistribute()
 */
INDEX_TEMPLATE_ARGUMENTS
template <typename N>
void BPLUSTREE_TYPE::Redistribute(N *neighbor_node, N *node, int index) {}
/*
 * Update root page if necessary
 * NOTE: size of root page can be less than min size and this method is only
 * called within coalesceOrRedistribute() method
 * case 1: when you delete the last element in root page, but root page still
 * has one last child
 * case 2: when you delete the last element in whole b+ tree
 * @return : true means root page should be deleted, false means no deletion
 * happend
 */
INDEX_TEMPLATE_ARGUMENTS
bool BPLUSTREE_TYPE::AdjustRoot(BPlusTreePage *old_root_node) { return false; }

/*****************************************************************************
 * INDEX ITERATOR
 *****************************************************************************/
/*
 * Input parameter is void, find the leaftmost leaf page first, then construct
 * index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::begin() { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is low key, find the leaf page that contains the input key
 * first, then construct index iterator
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::Begin(const KeyType &key) { return INDEXITERATOR_TYPE(); }

/*
 * Input parameter is void, construct an index iterator representing the end
 * of the key/value pair in the leaf node
 * @return : index iterator
 */
INDEX_TEMPLATE_ARGUMENTS
INDEXITERATOR_TYPE BPLUSTREE_TYPE::end() { return INDEXITERATOR_TYPE(); }

/*****************************************************************************
 * UTILITIES AND DEBUG
 * 从整个B+树的根结点开始，一直向下找到叶子结点
 * 因为B+树是多路搜索树，所以整个向下搜索就是通过key值进行比较
 * 其中内部结点向下搜索的过程中调用InternalPage的Lookup函数
 *****************************************************************************/
/*
 * Find leaf page containing particular key, if leftMost flag == true, find
 * the left most leaf page
 */

INDEX_TEMPLATE_ARGUMENTS
std::pair<Page *, bool> BPLUSTREE_TYPE::FindLeafPageByOperation(const KeyType &key, Operation operation,
                                                                Transaction *transaction, bool leftMost,
                                                                bool rightMost) {
  // 该函数用于查找索引树中包含key的叶子节点, 除了以下两种情况
  // 如果leftMost为真，那么只返回最左边的叶子节点
  // 如果rightMost为真，那么返回最右边的叶子节点
  LOG_INFO("FindLeafPageByOperation() begin");
  assert(operation == Operation::FIND ? !(leftMost && rightMost) : transaction != nullptr);

  // LOG_INFO("BEGIN FindLeafPage key=%ld Thread=%lu Operation=%d", key.ToString(), getThreadId(),
  //          OpToString(operation));  // DEBUG

  bool is_root_page_id_latched = true;

  Page *page = buffer_pool_manager_->FetchPage(root_page_id_);
  BPlusTreePage *node = reinterpret_cast<BPlusTreePage *>(page->GetData());

  if (operation == Operation::FIND) {
    // page->RLatch();
    is_root_page_id_latched = false;
    // root_latch_.unlock();
  } else {
    // page->WLatch();
    if (IsSafe(node, operation)) {
      is_root_page_id_latched = false;
      // root_latch_.unlock();
    }
  }

  while (!node->IsLeafPage()) {
    InternalPage *i_node = reinterpret_cast<InternalPage *>(node);

    page_id_t child_node_page_id;
    if (leftMost) {
      child_node_page_id = i_node->ValueAt(0);
    } else if (rightMost) {
      child_node_page_id = i_node->ValueAt(i_node->GetSize() - 1);
    } else {
      child_node_page_id = i_node->Lookup(key, comparator_);
    }

    auto child_page = buffer_pool_manager_->FetchPage(child_node_page_id);
    auto child_node = reinterpret_cast<BPlusTreePage *>(child_page->GetData());

    if (operation == Operation::FIND) {
      // child_page->RLatch();
      // page->RUnlatch();
      buffer_pool_manager_->UnpinPage(page->GetPageId(), false);
    } else {
      // child_page->WLatch();
      transaction->AddIntoPageSet(page);
      // child node is safe, release all locks on ancestors
      if (IsSafe(child_node, operation)) {
        if (is_root_page_id_latched) {
          is_root_page_id_latched = false;
          //     root_latch_.unlock();
        }
        UnlockUnpinPages(transaction);
      }
    }

    page = child_page;
    node = child_node;
  }  // end while

  // LOG_INFO("END FindLeafPage key=%ld Thread=%lu Operation=%d", key.ToString(), getThreadId(),
  //          OpToString(operation));  // DEBUG

  return std::make_pair(page, is_root_page_id_latched);
}

/* unlock and unpin all pages */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UnlockUnpinPages(Transaction *transaction) {
  // LOG_INFO("ENTER UnlockUnpinPages Thread=%ld", getThreadId());

  if (transaction == nullptr) {
    return;
  }

  // auto pages = transaction->GetPageSet().get();
  // LOG_INFO("transaction page set size = %d", (int)pages->size());

  // unlock 和 unpin 事务经过的所有parent page
  for (Page *page : *transaction->GetPageSet()) {  // 前面加*是因为page set是shared_ptr类型

    page->WUnlatch();
    buffer_pool_manager_->UnpinPage(page->GetPageId(), false);  // 疑问：此处dirty为false还是true？
    // 应该是false，此函数只在向下find leaf page时使用，向上进行修改时是手动一步步unpin true，这里是一次性unpin
  }
  transaction->GetPageSet()->clear();  // 清空page set
}

INDEX_TEMPLATE_ARGUMENTS
template <typename N>
bool BPLUSTREE_TYPE::IsSafe(N *node, Operation op) {
  // 该函数用于确定执行各种操作是否安全，所谓安全是指是否可以直接进行插入、删除等操作而不用进行分裂、合并
  if (node->IsRootPage()) {
    return (op == Operation::INSERT && node->GetSize() < node->GetMaxSize() - 1) ||
           (op == Operation::DELETE && node->GetSize() > 2);
  }

  if (op == Operation::INSERT) {
    // 疑问：maxsize要减1吗？
    return node->GetSize() < node->GetMaxSize() - 1;
  }

  if (op == Operation::DELETE) {
    // 此处逻辑需要和coalesce函数对应
    return node->GetSize() > node->GetMinSize();
  }

  // LOG_INFO("IsSafe Thread=%ld", getThreadId());

  return true;
}

INDEX_TEMPLATE_ARGUMENTS
Page *BPLUSTREE_TYPE::FindLeafPage(const KeyType &key, bool leftMost) {
  // 该函数用于在索引树中找到包含key的叶子节点
  // 如果leftMost为true, 那么就只返回最左边的叶子节点
  return FindLeafPageByOperation(key, Operation::FIND, nullptr, leftMost, false).first;
}

/*
 * Update/Insert root page id in header page(where page_id = 0, header_page is
 * defined under include/page/header_page.h)
 * Call this method everytime root page id is changed.
 * @parameter: insert_record      defualt value is false. When set to true,
 * insert a record <index_name, root_page_id> into header page instead of
 * updating it.
 */

INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::UpdateRootPageId(int insert_record) {
  // 这个Header用来记录元数据
  HeaderPage *header_page = static_cast<HeaderPage *>(buffer_pool_manager_->FetchPage(HEADER_PAGE_ID));
  if (insert_record != 0) {
    // create a new record<index_name + root_page_id> in header_page
    header_page->InsertRecord(index_name_, root_page_id_);
  } else {
    // update root_page_id in header_page
    header_page->UpdateRecord(index_name_, root_page_id_);
  }
  buffer_pool_manager_->UnpinPage(HEADER_PAGE_ID, true);
}

/*
 * This method is used for test only
 * Read data from file and insert one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::InsertFromFile(const std::string &file_name, Transaction *transaction) {}
/*
 * This method is used for test only
 * Read data from file and remove one by one
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::RemoveFromFile(const std::string &file_name, Transaction *transaction) {}

/**
 * This method is used for debug only, You don't  need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 * @param out
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToGraph(BPlusTreePage *page, BufferPoolManager *bpm, std::ofstream &out) const {
  std::string leaf_prefix("LEAF_");
  std::string internal_prefix("INT_");
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    // Print node name
    out << leaf_prefix << leaf->GetPageId();
    // Print node properties
    out << "[shape=plain color=green ";
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">P=" << leaf->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << leaf->GetSize() << "\">"
        << "max_size=" << leaf->GetMaxSize() << ",min_size=" << leaf->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < leaf->GetSize(); i++) {
      out << "<TD>" << leaf->KeyAt(i) << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Leaf node link if there is a next page
    if (leaf->GetNextPageId() != INVALID_PAGE_ID) {
      out << leaf_prefix << leaf->GetPageId() << " -> " << leaf_prefix << leaf->GetNextPageId() << ";\n";
      out << "{rank=same " << leaf_prefix << leaf->GetPageId() << " " << leaf_prefix << leaf->GetNextPageId() << "};\n";
    }

    // Print parent links if there is a parent
    if (leaf->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << leaf->GetParentPageId() << ":p" << leaf->GetPageId() << " -> " << leaf_prefix
          << leaf->GetPageId() << ";\n";
    }
  } else {
    InternalPage *inner = reinterpret_cast<InternalPage *>(page);
    // Print node name
    out << internal_prefix << inner->GetPageId();
    // Print node properties
    out << "[shape=plain color=pink ";  // why not?
    // Print data of the node
    out << "label=<<TABLE BORDER=\"0\" CELLBORDER=\"1\" CELLSPACING=\"0\" CELLPADDING=\"4\">\n";
    // Print data
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">P=" << inner->GetPageId() << "</TD></TR>\n";
    out << "<TR><TD COLSPAN=\"" << inner->GetSize() << "\">"
        << "max_size=" << inner->GetMaxSize() << ",min_size=" << inner->GetMinSize() << "</TD></TR>\n";
    out << "<TR>";
    for (int i = 0; i < inner->GetSize(); i++) {
      out << "<TD PORT=\"p" << inner->ValueAt(i) << "\">";
      if (i > 0) {
        out << inner->KeyAt(i);
      } else {
        out << " ";
      }
      out << "</TD>\n";
    }
    out << "</TR>";
    // Print table end
    out << "</TABLE>>];\n";
    // Print Parent link
    if (inner->GetParentPageId() != INVALID_PAGE_ID) {
      out << internal_prefix << inner->GetParentPageId() << ":p" << inner->GetPageId() << " -> " << internal_prefix
          << inner->GetPageId() << ";\n";
    }
    // Print leaves
    for (int i = 0; i < inner->GetSize(); i++) {
      auto child_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i))->GetData());
      ToGraph(child_page, bpm, out);
      if (i > 0) {
        auto sibling_page = reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(inner->ValueAt(i - 1))->GetData());
        if (!sibling_page->IsLeafPage() && !child_page->IsLeafPage()) {
          out << "{rank=same " << internal_prefix << sibling_page->GetPageId() << " " << internal_prefix
              << child_page->GetPageId() << "};\n";
        }
        bpm->UnpinPage(sibling_page->GetPageId(), false);
      }
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

/**
 * This function is for debug only, you don't need to modify
 * @tparam KeyType
 * @tparam ValueType
 * @tparam KeyComparator
 * @param page
 * @param bpm
 */
INDEX_TEMPLATE_ARGUMENTS
void BPLUSTREE_TYPE::ToString(BPlusTreePage *page, BufferPoolManager *bpm) const {
  if (page->IsLeafPage()) {
    LeafPage *leaf = reinterpret_cast<LeafPage *>(page);
    std::cout << "Leaf Page: " << leaf->GetPageId() << " parent: " << leaf->GetParentPageId()
              << " next: " << leaf->GetNextPageId() << std::endl;
    for (int i = 0; i < leaf->GetSize(); i++) {
      std::cout << leaf->KeyAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
  } else {
    InternalPage *internal = reinterpret_cast<InternalPage *>(page);
    std::cout << "Internal Page: " << internal->GetPageId() << " parent: " << internal->GetParentPageId() << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      std::cout << internal->KeyAt(i) << ": " << internal->ValueAt(i) << ",";
    }
    std::cout << std::endl;
    std::cout << std::endl;
    for (int i = 0; i < internal->GetSize(); i++) {
      ToString(reinterpret_cast<BPlusTreePage *>(bpm->FetchPage(internal->ValueAt(i))->GetData()), bpm);
    }
  }
  bpm->UnpinPage(page->GetPageId(), false);
}

template class BPlusTree<GenericKey<4>, RID, GenericComparator<4>>;
template class BPlusTree<GenericKey<8>, RID, GenericComparator<8>>;
template class BPlusTree<GenericKey<16>, RID, GenericComparator<16>>;
template class BPlusTree<GenericKey<32>, RID, GenericComparator<32>>;
template class BPlusTree<GenericKey<64>, RID, GenericComparator<64>>;

}  // namespace bustub
