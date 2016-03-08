/*
 * Created by Ivo Georgiev on 2/9/16.
 * Ian Kaufman PA1
 */

#include <stdlib.h>
#include <assert.h>
#include <stdio.h> // for perror()

#include "mem_pool.h"

/*************/
/*           */
/* Constants */
/*           */
/*************/
static const float      MEM_FILL_FACTOR                 = 0.75;
static const unsigned   MEM_EXPAND_FACTOR               = 2;

static const unsigned   MEM_POOL_STORE_INIT_CAPACITY    = 20;
static const float      MEM_POOL_STORE_FILL_FACTOR      = 0.75;
static const unsigned   MEM_POOL_STORE_EXPAND_FACTOR    = 2;

static const unsigned   MEM_NODE_HEAP_INIT_CAPACITY     = 40;
static const float      MEM_NODE_HEAP_FILL_FACTOR       = 0.75;
static const unsigned   MEM_NODE_HEAP_EXPAND_FACTOR     = 2;

static const unsigned   MEM_GAP_IX_INIT_CAPACITY        = 40;
static const float      MEM_GAP_IX_FILL_FACTOR          = 0.75;
static const unsigned   MEM_GAP_IX_EXPAND_FACTOR        = 2;



/*********************/
/*                   */
/* Type declarations */
/*                   */
/*********************/
typedef struct _node {
    alloc_t alloc_record;
    unsigned used;
    unsigned allocated;
    struct _node *next, *prev; // doubly-linked list for gap deletion
} node_t, *node_pt;

typedef struct _gap {
    size_t size;
    node_pt node;
} gap_t, *gap_pt;

typedef struct _pool_mgr {
    pool_t pool;
    node_pt node_heap;
    unsigned total_nodes;
    unsigned used_nodes;
    gap_pt gap_ix;
    unsigned gap_ix_capacity;
} pool_mgr_t, *pool_mgr_pt;



/***************************/
/*                         */
/* Static global variables */
/*                         */
/***************************/
static pool_mgr_pt *pool_store = NULL; // an array of pointers, only expand
static unsigned pool_store_size = 0;
static unsigned pool_store_capacity = 0;



/********************************************/
/*                                          */
/* Forward declarations of static functions */
/*                                          */
/********************************************/
static alloc_status _mem_resize_pool_store();
static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr);
static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr);
static alloc_status
        _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                           size_t size,
                           node_pt node);
static alloc_status
        _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                size_t size,
                                node_pt node);
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr);



/****************************************/
/*                                      */
/* Definitions of user-facing functions */
/*                                      */
/****************************************/
alloc_status mem_init() {
    // ensure that it's called only once until mem_free
    // allocate the pool store with initial capacity
    // note: holds pointers only, other functions to allocate/deallocate
    if (pool_store) {
        return ALLOC_CALLED_AGAIN;
    }
    else {
        pool_mgr_pt* store = (pool_mgr_pt*) calloc(MEM_POOL_STORE_INIT_CAPACITY, sizeof(pool_mgr_pt));
        assert(store);
        pool_store_size = 0;
        pool_store_capacity = MEM_POOL_STORE_INIT_CAPACITY;
        pool_store = store;
        return ALLOC_OK;
    }
}

alloc_status mem_free() {
    // ensure that it's called only once for each mem_init
    if (pool_store) {
        for (int i = 0; i < pool_store_size; i++) {
            if(pool_store[i] != NULL) {
                return ALLOC_CALLED_AGAIN;
            }
        }
        free(pool_store);
        pool_store = NULL;
        pool_store_size = 0;
        pool_store_capacity = 0;
        return ALLOC_OK;
    }
    // make sure all pool managers have been deallocated
    // can free the pool store array
    // update static variables

    return ALLOC_CALLED_AGAIN;
}

pool_pt mem_pool_open(size_t size, alloc_policy policy) {
    // make sure there the pool store is allocated
    assert(pool_store);
    // expand the pool store, if necessary
    _mem_resize_pool_store();
    // allocate a new mem pool mgr
    pool_mgr_pt mgr = (pool_mgr_pt) calloc(1, sizeof(pool_mgr_t));
    // check success, on error return null
    if (mgr == NULL) {
        return NULL;
    }
    // allocate a new memory pool
    char* new_pool = (char*) calloc(size, sizeof(char));
    // check success, on error deallocate mgr and return null
    if (new_pool == NULL) {
        free(mgr);
        return NULL;
    }
    // allocate a new node heap
    node_pt new_heap = (node_pt) calloc(MEM_NODE_HEAP_INIT_CAPACITY, sizeof(node_t));
    // check success, on error deallocate mgr/pool and return null
    if (new_heap == NULL) {
        free(new_pool);
        free(mgr);
        return NULL;
    }
    // allocate a new gap index
    gap_pt new_ix = (gap_pt) calloc(MEM_GAP_IX_INIT_CAPACITY, sizeof(gap_t));
    // check success, on error deallocate mgr/pool/heap and return null
    if (new_ix == NULL) {
        free(new_heap);
        free(new_pool);
        free(mgr);
    }
    // assign all the pointers and update meta data:
    mgr->pool.mem = new_pool;
    mgr->pool.total_size = size;
    mgr->pool.num_allocs = 0;
    mgr->pool.alloc_size = 0;
    mgr->pool.num_gaps = 1;
    mgr->pool.policy = policy;

    //   initialize top node of node heap
    new_heap[0].allocated = 0;
    new_heap[0].used = 1;
    new_heap[0].alloc_record.mem = new_pool;
    new_heap[0].prev = NULL;
    new_heap[0].next = NULL;
    new_heap[0].alloc_record.size = size;
    //   initialize top node of gap index
    new_ix[0].size = size;
    new_ix[0].node = new_heap;
    //   initialize pool mgr
    mgr->gap_ix = new_ix;
    mgr->gap_ix_capacity = MEM_GAP_IX_INIT_CAPACITY;
    mgr->node_heap = new_heap;
    mgr->total_nodes = MEM_NODE_HEAP_INIT_CAPACITY;
    mgr->used_nodes = 1;
    //   link pool mgr to pool store
    pool_store[pool_store_size] = mgr;
    pool_store_size += 1;
    // return the address of the mgr, cast to (pool_pt)
    return (pool_pt) mgr;
}

alloc_status mem_pool_close(pool_pt pool) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    // check if this pool is allocated
    // check if pool has only one gap
    // check if it has zero allocations
    // free memory pool
    // free node heap
    // free gap index
    // find mgr in pool store and set to null
    // note: don't decrement pool_store_size, because it only grows
    // free mgr

    pool_mgr_pt del_pool = (pool_mgr_pt) pool;
    if ((pool->mem != NULL) && (pool->num_gaps == 1) && (del_pool->used_nodes ==1)) {
        free(pool->mem);
        free(del_pool->node_heap);
        free(del_pool->gap_ix);

        for (int i = 0; i < pool_store_size; i++) {
            if (del_pool == pool_store[i]) {
                pool_store[i] = NULL;
                break;
            }
        }
        free(del_pool);
        return ALLOC_OK;
    }
    return ALLOC_NOT_FREED;
}

alloc_pt mem_new_alloc(pool_pt pool, size_t size) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt)pool;
    // check if any gaps, return null if none
    if(pool->num_gaps == 0) {
        return NULL;
    }
    // expand heap node, if necessary, quit on error
    _mem_resize_node_heap(mgr);
    // check used nodes fewer than total nodes, quit on error
    assert(mgr->used_nodes < mgr->total_nodes);
    // get a node for allocation:
    node_pt suf_node = NULL;
    // if FIRST_FIT, then find the first sufficient node in the node heap
    if (pool->policy == FIRST_FIT) {
        for (int i = 0; i < mgr->total_nodes; i++) {
            if ((mgr->node_heap[i].allocated == 0) && (mgr->node_heap[i].alloc_record.size >= size)) {
                suf_node = &mgr->node_heap[i];
                break;
            }
        }
    }
    // if BEST_FIT, then find the first sufficient node in the gap index
    else if (pool->policy == BEST_FIT) {
        for (int i = 0; i < mgr->pool.num_gaps; i++) {
            if ((mgr->gap_ix[i].size >= size)) {
                suf_node = mgr->gap_ix[i].node;
                break;
            }
        }
    }
    // check if node found
    if (suf_node == NULL) {
        return NULL;
    }
    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs += 1;
    mgr->pool.alloc_size += size;
    // calculate the size of the remaining gap, if any
    size_t suf_size;
    suf_size = suf_node->alloc_record.size;
    size_t r_size = suf_size  - size;
    // remove node from gap index
    _mem_remove_from_gap_ix(mgr, suf_node->alloc_record.size, suf_node);
    // convert gap_node to an allocation node of given size
    suf_node->allocated = 1;
    suf_node->alloc_record.size = size;
    // adjust node heap:
    if (r_size > 0) {
        //   if remaining gap, need a new node
        node_pt unused_node;
        //   find an unused one in the node heap
        for (int i = 0; i < mgr->total_nodes; i++) {
            if (mgr->node_heap[i].used == 0) {
                unused_node = &mgr->node_heap[i];
                break;
            }
        }
        //   make sure one was found
        assert(unused_node);
        //   initialize it to a gap node
        unused_node->used = 1;
        unused_node->allocated = 0;
        unused_node->alloc_record.size = r_size;
        unused_node->alloc_record.mem = suf_node->alloc_record.mem + size*sizeof(char);
        //   update metadata (used_nodes)
        mgr->used_nodes += 1;
        //   update linked list (new node right after the node for allocation)
        unused_node->next = suf_node->next;
        if (suf_node->next) {
            suf_node->next->prev = unused_node;
        }
        suf_node->next = unused_node;
        unused_node->prev = suf_node;
        //   add to gap index
        assert(_mem_add_to_gap_ix(mgr, unused_node->alloc_record.size, unused_node) == ALLOC_OK);
        //   check if successful
    }
    // return allocation record by casting the node to (alloc_pt)
    return (alloc_pt) suf_node;
}

alloc_status mem_del_alloc(pool_pt pool, alloc_pt alloc) {
    // get mgr from pool by casting the pointer to (pool_mgr_pt)
    pool_mgr_pt mgr = (pool_mgr_pt) pool;
    // get node from alloc by casting the pointer to (node_pt)
    node_pt node = (node_pt) alloc;
    // find the node in the node heap
    node_pt del_node = NULL;
    node_pt node_to_add = NULL;

    for(int i = 0; i < mgr->total_nodes; i++) {
        if (node == &mgr->node_heap[i]) {
            del_node = &mgr->node_heap[i];
            break;
        }
    }
    // this is node-to-delete
    // make sure it's found
    // convert to gap node
    del_node->allocated = 0;
    // update metadata (num_allocs, alloc_size)
    mgr->pool.num_allocs -= 1;
    mgr->pool.alloc_size -= del_node->alloc_record.size;
    // if the next node in the list is also a gap, merge into node-to-delete
    if ((del_node->next) && (del_node->next->allocated == 0)) {
        //   remove the next node from gap index
        node_pt next_node = del_node->next;
        size_t next_node_size = next_node->alloc_record.size;
        assert(_mem_remove_from_gap_ix(mgr, next_node_size, next_node) == ALLOC_OK);
        //   check success
        //   add the size to the node-to-delete
        del_node->alloc_record.size += next_node_size;
        //   update node as unused
        next_node->used = 0;
        //   update metadata (used nodes)
        mgr->used_nodes -= 1;
        //   update linked list:
        if (next_node->next) {
            next_node->next->prev = del_node;
            del_node->next = next_node->next;
        }
        else {
            del_node->next = NULL;
        }
        next_node->next = NULL;
        next_node->prev = NULL;
        /*
                        if (next->next) {
                            next->next->prev = node_to_del;
                            node_to_del->next = next->next;
                        } else {
                            node_to_del->next = NULL;
                        }
                        next->next = NULL;
                        next->prev = NULL;
         */
        node_to_add = del_node;
    }
    // this merged node-to-delete might need to be added to the gap index
    // but one more thing to check...
    // if the previous node in the list is also a gap, merge into previous!
    if ((del_node->prev) && (del_node->prev->allocated == 0)) {
        //   remove the previous node from gap index
        node_pt prev_node = del_node->prev;
        size_t prev_node_size = prev_node->alloc_record.size;
        assert(_mem_remove_from_gap_ix(mgr, prev_node_size, prev_node) == ALLOC_OK);
        //   check success
        //   add the size of node-to-delete to the previous
        prev_node->alloc_record.size += del_node->alloc_record.size;
        //   update node-to-delete as unused
        del_node->used = 0;
        //   update metadata (used_nodes)
        mgr->used_nodes -= 1;
        //   update linked list
        if (del_node->next) {
            prev_node->next = del_node->next;
            del_node->next->prev = prev_node;
        }
        else {
            prev_node->next = NULL;
        }
        del_node->next = NULL;
        del_node->prev = NULL;
        /*
                        if (node_to_del->next) {
                            prev->next = node_to_del->next;
                            node_to_del->next->prev = prev;
                        } else {
                            prev->next = NULL;
                        }
                        node_to_del->next = NULL;
                        node_to_del->prev = NULL;
         */
        //prev_node->next->alloc_record.mem = prev_node->alloc_record.mem + prev_node->alloc_record.size*sizeof(char);
        node_to_add = prev_node;
    }
    //   change the node to add to the previous node!

    // add the resulting node to the gap index
    if (node_to_add) {
        assert(_mem_add_to_gap_ix(mgr, node_to_add->alloc_record.size, node_to_add) == ALLOC_OK);
    }
    else {
        assert(_mem_add_to_gap_ix(mgr, del_node->alloc_record.size, del_node) == ALLOC_OK);
    }
    // check success

    return ALLOC_OK;
}

void mem_inspect_pool(pool_pt pool,
                      pool_segment_pt *segments,
                      unsigned *num_segments) {
    // get the mgr from the pool
    pool_mgr_pt mgr = (pool_mgr_pt) pool;
    // allocate the segments array with size == used_nodes
    pool_segment_pt segs = (pool_segment_pt) calloc(mgr->used_nodes, sizeof(pool_segment_t));
    // check successful
    assert(segs);
    // loop through the node heap and the segments array
    //    for each node, write the size and allocated in the segment
    node_pt first_node = mgr->node_heap;
    int i = 0;
    while (first_node) {
        segs[i].size = first_node->alloc_record.size;
        segs[i].allocated = first_node->allocated;
        first_node = first_node->next;
        i++;
    }

    *segments = segs;
    *num_segments = mgr->used_nodes;
    // "return" the values:
    /*
                    *segments = segs;
                    *num_segments = pool_mgr->used_nodes;
     */
}



/***********************************/
/*                                 */
/* Definitions of static functions */
/*                                 */
/***********************************/
static alloc_status _mem_resize_pool_store() {
    // check if necessary
    /*
                if (((float) pool_store_size / pool_store_capacity)
                    > MEM_POOL_STORE_FILL_FACTOR) {...}
     */
    // don't forget to update capacity variables
    if (((float) pool_store_size / pool_store_capacity) > MEM_POOL_STORE_FILL_FACTOR) {
        pool_mgr_pt* resize = (pool_mgr_pt*) realloc(pool_store, pool_store_capacity*MEM_POOL_STORE_EXPAND_FACTOR );
        assert(resize);
        pool_store = resize;
        pool_store_capacity = pool_store_capacity*MEM_POOL_STORE_EXPAND_FACTOR;
        return ALLOC_OK;
    }

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_node_heap(pool_mgr_pt pool_mgr) {
    if (((float) pool_mgr->used_nodes / pool_mgr->total_nodes) > MEM_NODE_HEAP_FILL_FACTOR) {
        node_pt resize = realloc(pool_mgr->node_heap, pool_mgr->total_nodes*MEM_NODE_HEAP_EXPAND_FACTOR);
        assert(resize);
        pool_mgr->node_heap = resize;
        pool_mgr->total_nodes = pool_mgr->total_nodes*MEM_NODE_HEAP_EXPAND_FACTOR;
        return ALLOC_OK;
    }

    return ALLOC_FAIL;
}

static alloc_status _mem_resize_gap_ix(pool_mgr_pt pool_mgr) {
    if (((float) pool_mgr->pool.num_gaps / pool_mgr->gap_ix_capacity) > MEM_GAP_IX_FILL_FACTOR) {
        gap_pt resize = realloc(pool_mgr->gap_ix, pool_mgr->gap_ix_capacity*MEM_GAP_IX_EXPAND_FACTOR);
        assert(resize);
        pool_mgr->gap_ix = resize;
        pool_mgr->gap_ix_capacity = pool_mgr->gap_ix_capacity*MEM_GAP_IX_EXPAND_FACTOR;
        return ALLOC_OK;
    }

    return ALLOC_FAIL;
}

static alloc_status _mem_add_to_gap_ix(pool_mgr_pt pool_mgr,
                                       size_t size,
                                       node_pt node) {

    // expand the gap index, if necessary (call the function)
    _mem_resize_gap_ix(pool_mgr);
    // add the entry at the end
    gap_t new_gap;
    new_gap.size = size;
    new_gap.node = node;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps] = new_gap;
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps += 1;
    // sort the gap index (call the function)
    assert(_mem_sort_gap_ix(pool_mgr) == ALLOC_OK);
    // check success

    return ALLOC_OK;
}

static alloc_status _mem_remove_from_gap_ix(pool_mgr_pt pool_mgr,
                                            size_t size,
                                            node_pt node) {
    // find the position of the node in the gap index
    int index_pos = 0;
    for (int i = 0; i < pool_mgr->pool.num_gaps; i++) {
        if ((pool_mgr->gap_ix[i].size == size) && (pool_mgr->gap_ix[i].node == node)) {
            index_pos = i;
            break;
        }
    }
    // loop from there to the end of the array:
    for (int i = index_pos; i < pool_mgr->pool.num_gaps - 1; i++) {
        pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i+1];
    }
    //    pull the entries (i.e. copy over) one position up
    //    this effectively deletes the chosen node
    // update metadata (num_gaps)
    pool_mgr->pool.num_gaps -= 1;
    // zero out the element at position num_gaps!
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].size = 0;
    pool_mgr->gap_ix[pool_mgr->pool.num_gaps].node = NULL;

    return ALLOC_OK;
}

// note: only called by _mem_add_to_gap_ix, which appends a single entry
static alloc_status _mem_sort_gap_ix(pool_mgr_pt pool_mgr) {
    // the new entry is at the end, so "bubble it up"
    // loop from num_gaps - 1 until but not including 0:
    for (int i = pool_mgr->pool.num_gaps -1; i > 0; i--) {
        if (pool_mgr->gap_ix[i].size < pool_mgr->gap_ix[i-1].size) {
            gap_t temp = pool_mgr->gap_ix[i];
            pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i-1];
            pool_mgr->gap_ix[i-1] = temp;
        }
        else if (pool_mgr->gap_ix[i].size == pool_mgr->gap_ix[i-1].size) {
            if (pool_mgr->gap_ix[i].node->alloc_record.mem < pool_mgr->gap_ix[i - 1].node->alloc_record.mem) {
                gap_t temp = pool_mgr->gap_ix[i];
                pool_mgr->gap_ix[i] = pool_mgr->gap_ix[i-1];
                pool_mgr->gap_ix[i-1] = temp;
            }
        }
    }
    //    if the size of the current entry is less than the previous (u - 1)
    //    or if the sizes are the same but the current entry points to a
    //    node with a lower address of pool allocation address (mem)
    //       swap them (by copying) (remember to use a temporary variable)

    return ALLOC_OK;
}


