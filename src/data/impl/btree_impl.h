

#ifndef _SMILE_DATA_BTREE_IMPL_H_
#define _SMILE_DATA_BTREE_IMPL_H_ 

#include "../../base/platform.h"
#include "../../memory/buffer_pool.h"

#include <algorithm>
#include <cassert>


SMILE_NS_BEGIN 

class BufferPool;

enum class BTNodeType : uint8_t {
  E_INTERNAL,
  E_LEAF,
};

struct BTNodePageHeader {

  /**
   * @brief The type of the BT node
   */
  BTNodeType    m_type;                      

  /**
   * @brief The maximum number of elements 
   */
  int32_t      m_maxNumElements;

  /**
   * @brief The maximum number of elements 
   */
  int32_t      m_numElements;

  /**
   * @brief The size of the key, either value or pageId
   */
  size_t        m_keySize;

  /**
   * @brief The byte at which the element data starts
   */
  size_t        m_keyStart;

  /**
   * @brief The size of the element stored, either a pageId
   */
  size_t        m_elementSize;

  /**
   * @brief The byte at which the element data starts
   */
  size_t        m_elementStart;

};

/**
 * @brief In memory structure representing an index node
 *
 * @tparam K The key type
 * @tparam V The value type
 */
template<typename K, typename V>
struct BTNode {

  /**
   * @brief The buffer handler to the page with the data of this node
   */
  BufferHandler     m_handler;

  /**
   * @brief Pointer to the page header, which points at the beginning of the
   * buffer in m_handler
   */
  BTNodePageHeader* p_pageHeader; 


  /**
   * @brief Specific fields depending on whether is an internal node or a leaf
   */
  union {
    struct {

      /**
       * @brief Pointer to the keys
       */
      K*            p_keys;

      /**
       * @brief Pointer to the children pages
       */
      pageId_t*     p_children;

    } m_internal; 

    struct {

      /**
       * @brief Pointer to the keys
       */
      K*            p_keys;

      /**
       * @brief Pointer to the values
       */
      V*        p_values;

      pageId_t  m_next;

    } m_leaf; 
  };


  /**
   * @brief Stores whether the node is dirty and must be persisted or not
   */
  bool          m_dirty;
};

template<typename K, typename V>
class BTIterator {
public:
  BTIterator(BTNode<K,V> root);
  virtual ~BTIterator() = default;


  /**
   * @brief Tests whether there are more elements or not in the btree
   *
   * @return Returns true if there are more elements in the iterator
   */
  bool has_next();

  /**
   * @brief Gets the next element in the btree
   *
   * @return Returns the next elemnet in the btree
   */
  V next();

private:
  BTNode<K,V> m_root;
  BTNode<K,V> m_leaf;
  size_t      m_index;
};


/**
 * @brief Creates a new instance of an index node 
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 * @param type The type of the node to create
 *
 * @return A newly created BTNode
 */
template<typename K,typename V>
ErrorCode btree_create_node(BufferPool* bufferPool, BTNodeType type, BTNode<K,V>* node) noexcept {

  ErrorCode err = bufferPool->alloc(&node->m_handler);
  if(err != ErrorCode::E_NO_ERROR) {
    return err;
  }

  node->p_pageHeader = reinterpret_cast<BTNodePageHeader*>(&node->m_handler.m_buffer[0]);

  node->p_pageHeader->m_type = type;

  // seting the size of elements and keys in bytes
  node->p_pageHeader->m_keySize = sizeof(K);
  size_t sizeOfElement = type == BTNodeType::E_INTERNAL ? sizeof(pageId_t) : sizeof(V);
  node->p_pageHeader->m_elementSize = sizeOfElement;

  // computing the maximum number of elements the node can store, depending on
  // the page size and the given types
  int32_t availableSize = bufferPool->getPageSize() - sizeof(BTNodePageHeader);
  availableSize -= (sizeof(K) + sizeof(sizeOfElement));  // we substract the sizes to guarantee there will be room for alignment 
  node->p_pageHeader->m_maxNumElements  = (availableSize) / (sizeof(K) + sizeof(sizeOfElement));
  node->p_pageHeader->m_numElements = 0;

  // computing keyStart and elementStart bytes
  node->p_pageHeader->m_keyStart = std::max(sizeof(BTNodePageHeader), sizeof(K)); // max is used to guarantee alignment
  int32_t keyEnd = node->p_pageHeader->m_keyStart + (sizeof(K)*(node->p_pageHeader->m_maxNumElements));
  node->p_pageHeader->m_elementStart = keyEnd 
                                      - (keyEnd % sizeof(sizeOfElement)) 
                                      + sizeof(sizeOfElement)*(keyEnd % sizeof(sizeOfElement) != 0);

  // initializing helper pointers to keys and pages
  char* keyStartAddress = &node->m_handler.m_buffer[node->p_pageHeader->m_keyStart];
  switch(node->p_pageHeader->m_type) {
  case BTNodeType::E_INTERNAL:
    node->m_internal.p_keys  = reinterpret_cast<K*>(keyStartAddress);
    memset(node->m_internal.p_keys, 0, node->p_pageHeader->m_keySize*node->p_pageHeader->m_maxNumElements);
    break;
  case BTNodeType::E_LEAF:
    node->m_leaf.p_keys  = reinterpret_cast<K*>(keyStartAddress);
    memset(node->m_leaf.p_keys, 0, node->p_pageHeader->m_keySize*node->p_pageHeader->m_maxNumElements);
    break;
  }

  // setting pointers to elements 
  char* elementStartAddress = &node->m_handler.m_buffer[node->p_pageHeader->m_elementStart];
  if(type == BTNodeType::E_INTERNAL) {
    node->m_internal.p_children = reinterpret_cast<pageId_t*>(elementStartAddress);
    for(int32_t i = 0; i < node->p_pageHeader->m_numElements; ++i) {
      node->m_internal.p_children[i] = INVALID_PAGE_ID;
    }
  } else {
    node->m_leaf.p_values = reinterpret_cast<V*>(elementStartAddress);
    node->m_leaf.m_next = INVALID_PAGE_ID;
    memset(node->m_leaf.p_values, 0, node->p_pageHeader->m_elementSize*node->p_pageHeader->m_maxNumElements);
  }

  node->m_dirty = false;
  return ErrorCode::E_NO_ERROR;
}

/**
 * @brief Helper function to create internal nodes
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to store the created node
 * @param node A pointer to the node structure where the information of the
 * created node will be stored
 *
 * @return 
 */
template<typename K,typename V>
ErrorCode btree_create_internal(BufferPool* bufferPool, BTNode<K,V>* node) noexcept {
  return btree_create_node(bufferPool, BTNodeType::E_INTERNAL, node);
}

/**
 * @brief Helper function to create leaf nodes
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to store the created node
 * @param node A pointer to the node structure where the information of the
 * created node will be stored
 *
 * @return An error code 
 */
template<typename K,typename V>
ErrorCode btree_create_leaf(BufferPool* bufferPool, BTNode<K,V>* node) noexcept {
  return btree_create_node(bufferPool, BTNodeType::E_LEAF, node);
}


/**
 * @brief Loads an existing internal node from storage 
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to load the node
 *
 * @return A newly loaded BTNode
 */
template<typename K, typename V>
ErrorCode btree_load_node(BufferPool* bufferPool, pageId_t pId, BTNode<K,V>* node) noexcept {

  assert( pId != INVALID_PAGE_ID );

  ErrorCode err = ErrorCode::E_NO_ERROR;
  err = bufferPool->pin(pId, &node->m_handler);
  if(isError(err)) return err;

  node->p_pageHeader = reinterpret_cast<BTNodePageHeader*>(&node->m_handler.m_buffer[0]);

  size_t sizeOfElement = node->p_pageHeader->m_type == BTNodeType::E_INTERNAL ? sizeof(pageId_t) : sizeof(V);
  if(node->p_pageHeader->m_keySize != sizeof(K) ||
     node->p_pageHeader->m_elementSize != sizeOfElement) {
    bufferPool->unpin(pId);
    return ErrorCode::E_BTREE_CORRUPTED_PAGE;
  }

  // initializing helper pointers to keys and pages
  char* keyStartAddress = &node->m_handler.m_buffer[node->p_pageHeader->m_keyStart];
  switch(node->p_pageHeader->m_type) {
  case BTNodeType::E_INTERNAL:
    node->m_internal.p_keys  = reinterpret_cast<K*>(keyStartAddress);
    break;
  case BTNodeType::E_LEAF:
    node->m_leaf.p_keys  = reinterpret_cast<K*>(keyStartAddress);
    break;
  }

  // setting pointers to elements 
  char* elementStartAddress = &node->m_handler.m_buffer[node->p_pageHeader->m_elementStart];
  if(node->p_pageHeader->m_type == BTNodeType::E_INTERNAL) {
    node->m_internal.p_children = reinterpret_cast<pageId_t*>(elementStartAddress);
  } else {
    node->m_leaf.p_values = reinterpret_cast<V*>(elementStartAddress);
  }

  node->m_dirty = false;
  return ErrorCode::E_NO_ERROR;
}


/**
 * @brief Removes a given BTNode 
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 *
 * @param node The the node to remove
 */
template<typename K,typename V>
ErrorCode btree_destroy_node(BufferPool* bufferPool, 
                             BTNode<K,V>* node) {
  ErrorCode err = ErrorCode::E_NO_ERROR;
  if(node->m_dirty) {
    err = bufferPool->setPageDirty(node->m_handler.m_pId);
    if(isError(err)) {
      return err;
    }
  }
  err = bufferPool->unpin(node->m_handler.m_pId);
  if(isError(err)) return err;
  err = bufferPool->release(node->m_handler.m_pId);
  node->p_pageHeader = nullptr;
  node->m_internal.p_keys = nullptr;
  node->m_internal.p_children = nullptr;
  node->m_leaf.p_keys = nullptr;
  node->m_leaf.p_values = nullptr;
  return err;
}

/**
 * @brief Unloads a given BTNode 
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 *
 * @param node The the node to remove
 */
template<typename K,typename V>
ErrorCode btree_unload_node(BufferPool* bufferPool, 
                            BTNode<K,V>* node) {
  ErrorCode err = ErrorCode::E_NO_ERROR;
  if(node->m_dirty) {
    err = bufferPool->setPageDirty(node->m_handler.m_pId);
    if(isError(err)) return err;
  }

  err = bufferPool->unpin(node->m_handler.m_pId);
  node->p_pageHeader = nullptr;
  node->m_internal.p_keys = nullptr;
  node->m_internal.p_children = nullptr;
  node->m_leaf.p_keys = nullptr;
  node->m_leaf.p_values = nullptr;
  return err;
}

/**
 * @brief Given an internal node and a key, finds the index for the child where
 * the given key should exist or be put
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The the internal node
 * @param key The key
 *
 * @return Returns the index of the position where the key should lay. 
 */
template<typename K, typename V>
int32_t btree_next_internal(const BTNode<K,V>& node, 
                             const K& key) {

  assert(node.p_pageHeader->m_type == BTNodeType::E_INTERNAL);

  int32_t maxNumElements = node.p_pageHeader->m_maxNumElements;
  int32_t numElements = node.p_pageHeader->m_numElements;
  if( numElements <= 1) {
    return 0;
  }

  int32_t i = 0;
  for (; i < maxNumElements-1 && 
         (node.m_internal.p_children[i+1] != INVALID_PAGE_ID) && 
         key >= node.m_internal.p_keys[i]; 
      ++i);

  return i;
} 


/**
 * @brief Given a leaf node and a key, finds the index for the position where
 * the given key should exist or be put
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The leaf node
 * @param key The key
 *
 * @return Returns the index of the position where the key should lay. 
 */
template<typename K, typename V>
int32_t btree_next_leaf(BTNode<K,V> node, 
                         K key) {
  assert(node.p_pageHeader->m_type == BTNodeType::E_LEAF);

  int32_t maxNumElements = node.p_pageHeader->m_maxNumElements;
  int32_t numElements = node.p_pageHeader->m_numElements;

  int32_t i = 0;
  for (; i < numElements && key > node.m_leaf.p_keys[i]; ++i);
  return i;
}

/**
 * @brief Gets the value  associated with the given key
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The node to look for the key at
 * @param key The key of the value to look for
 *
 * @return Returns the pointer to the value associated with the given key.
 * Returns nullptr if the key does not exist
 */
template<typename K, typename V>
ErrorCode btree_get(BufferPool* bufferPool, 
                    BTNode<K,V> node, 
                    K key,
                    V* value) {

  switch(node.p_pageHeader->m_type) {

  case BTNodeType::E_INTERNAL: 
    int32_t child_idx = btree_next_internal(node, key);
    pageId_t childPage = node->m_internal.m_children[child_idx];
    if( childPage != INVALID_PAGE_ID ) { // if we found such a children, then proceed. If INVALID_PAGE_ID, means the node is empty
      BTNode<K,V> child; 
      ErrorCode err = btree_load_node(bufferPool, childPage, &child);
      if(isError(err)) return err;
      err = btree_get(bufferPool, child, key, value);
      btree_unload_node(bufferPool, &child);
      return err;
    }
    break;

  case BTNodeType::E_LEAF:
    int32_t i = btree_next_leaf(node, key);
    if(i < node->p_pageHeader->m_numElements && node->m_leaf.p_keys[i] == key) {
      *value = node->m_leaf.m_leafs[i];
      return ErrorCode::E_NO_ERROR;
    }
    break;
  }
  return ErrorCode::E_BTREE_KEY_NOT_FOUND;
}

/**
 * @brief Splits an internal node into two nodes.
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The internal node to split
 * @param bufferPool A pointer to the buffer pool used to create the node
 * @param sibling_key A pointer to the variable to store the key of the sibling
 * to be created
 *
 * @return Returns a pointer to the sibling of the split node 
 */
template<typename K, typename V>
BTNode<K,V> btree_split_internal(BufferPool* bufferPool, 
                             BTNode<K,V> node, 
                             K* sibling_key);

/**
 * @brief Splits a leaf node into two nodes.
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node A pointer to the leaf node to split
 * @param bufferPool A pointer to the buffer pool used to create the node
 * @param sibling_key A pointer to the variable to store the key of the sibling
 * to be created
 *
 * @return Returns a pointer to the sibling of the split node 
 */
template<typename K, typename V>
BTNode<K,V> btree_split_leaf(BufferPool* bufferPool,
                         BTNode<K,V> node, 
                         K* sibling_key);

/**
 * @brief Shifts the internal node and inserts the given child into the given
 * position
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The node to insert the child to
 * @param idx The index of the position to start shifting
 * @param child A pointer to the child to add at the given position
 * @param key The key of the child to add
 */
template<typename K, typename V>
void btree_shift_insert_internal(BTNode<K,V> node, 
                                 int32_t idx, 
                                 BTNode<K,V> child, 
                                 K key );

/**
 * @brief Shifts the leaf node and inserts the given child into the given
 * position
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The node to insert the child to
 * @param idx The index of the position to start shifting
 * @param value The value to add at the given position
 * @param key The key of the child to add
 */
template<typename K, typename V>
void btree_shift_insert_leaf(BTNode<K,V> node, 
                             int32_t idx, 
                             K key,
                             V value );


/**
 * @brief Inserts an element to the given node. This method assumes that there
 * is free space in the node for inserting the given element
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The node to insert the element to
 * @param key The key of the element to insert
 * @param value The element to insert
 */
template<typename K, typename V>
void btree_insert(BTNode<K,V> node, 
                  K key, 
                  V value);

/**
 * @brief Inserts a given element to the given node. This method assumes that a
 * pointer to the variable pointing to an nternal node is given. If there is not
 * space to such internal node, the given pointer to pointer will be override
 * with a new value pointing to a new internal node. Effectively, this method is
 * growing the tree from the upper side
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node A pointer to a pointer to an internal node
 * @param key The key of the element to add
 * @param value The value to add 
 */

template<typename K, typename V>
void btree_insert_root(BTNode<K,V>* node, K key, V value);

/**
 * @brief Removes the element at position idx and shifts the rest of elements to
 * left
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 *
 * @param node The internal node to remove the element from
 * @param idx The position of the element to remove
 *
 */
template<typename K, typename V>
void btree_remove_shift_internal(BufferPool* bufferPool,
                                 BTNode<K,V> node, 
                                 int32_t idx);

/**
 * @brief Removes the element at position idx and shifts the rest of elements to
 * left
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node The leaf node to remove the element from
 * @param idx The position of the element to remove
 *
 */
template<typename K, typename V>
void btree_remove_shift_leaf(BTNode<K,V> node, int32_t idx);

/**
 * @brief Merges the two internal nodes found at the provided position
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 * @param node The parent node where the nodes to merge belong to
 * @param idx1 The index of the first node to merge
 * @param idx2 the index of the second node to merge
 */
template<typename K, typename V>
void btree_merge_internal(BufferPool* bufferPool, 
                          BTNode<K,V> node, 
                          int32_t idx1, 
                          int32_t idx2);

/**
 * @brief Merges the two leaf nodes found at the provided positions
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param bufferPool A pointer to the buffer pool used to create the node
 * @param node The parent node where the leaf nodes to merge belong to
 * @param idx1 The index of the first node to merge
 * @param idx2 The index of the second node to merge
 */
template<typename K, typename V>
void btree_merge_leaf(BufferPool* bufferPool,
                      BTNode<K,V> node, 
                      int32_t idx1, 
                      int32_t idx2);

/**
 * @brief Removes an element with the given key
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node A pointer to the node to remove the element from
 * @param key The key of the element to remove
 * @param changed_min Pointer to boolean storing whether the minimum changed or
 * not 
 * @param new_min Pointer to store the new minum value of that node 
 *
 * @return Returns a pointer to the removed element
 */
template<typename K, typename V>
V btree_remove(BTNode<K,V> node, 
                   K key, 
                   bool* min_changed, 
                   K* new_min);

/**
 * @brief Removes the element with the given key
 *
 * @tparam K The key type
 * @tparam V The value type
 * @param node A pointer to a pointer to the internal root node of the btree. If
 * the remove operation makes it possible to reduce the tree height, the
 * internal node pointer by node is replaced.
 * @param key The key of the element to remove
 *
 * @return Returns a pointer to the removed element. Returns nullptr if the
 * element was not found.
 */
template<typename K, typename V>
V btree_remove(BTNode<K,V>* node, K key);
  
SMILE_NS_END

#endif /* ifndef _FURIOUS_BTREE_IMPL_H_ */
