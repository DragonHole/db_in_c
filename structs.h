#ifndef __CLASS_1_H
#define __CLASS_1_H

// content
#include <stdint.h>
#include <stdio.h>
#include <fcntl.h>
#include <stdbool.h>

typedef enum {
    COMPILE_SUCCESS,
    COMPILE_UNRECOGNIZED,
    COMPILE_SYNTAX_ERROR
} CompileResult;

typedef enum {
    EXECUTE_SUCCESS,
    EXECUTE_TABLE_FULL,
    EXECUTE_UNDEFINED_ERROR,
    EXECUTE_DUPLICATE_KEY
} ExecuteResult;

typedef enum {
    STATEMENT_INSERT,
    STATEMENT_SELECT
} StatementType;


#define COLUMN_USERNAME_SIZE 32
#define COLUMN_EMAIL_SIZE 255
typedef struct {
  uint32_t id;
  char username[COLUMN_USERNAME_SIZE];
  char email[COLUMN_EMAIL_SIZE];
} Row;
  
typedef struct {
    StatementType type;
    char **args; // arguments of the statement
    Row row_to_insert;
} Statement;


#define size_of_attribute(Struct, Attribute) sizeof(((Struct*)0)->Attribute)

const uint32_t ID_SIZE = size_of_attribute(Row, id);
const uint32_t USERNAME_SIZE = size_of_attribute(Row, username);
const uint32_t EMAIL_SIZE = size_of_attribute(Row, email);
const uint32_t ID_OFFSET = 0;
const uint32_t USERNAME_OFFSET = ID_OFFSET + ID_SIZE;
const uint32_t EMAIL_OFFSET = USERNAME_OFFSET + USERNAME_SIZE;
const uint32_t ROW_SIZE = ID_SIZE + USERNAME_SIZE + EMAIL_SIZE;

void serialize_row(Row* source, void* destination) {
  memcpy(destination + ID_OFFSET, &(source->id), ID_SIZE);
  memcpy(destination + USERNAME_OFFSET, &(source->username), USERNAME_SIZE);
  memcpy(destination + EMAIL_OFFSET, &(source->email), EMAIL_SIZE);
}

void deserialize_row(void* source, Row* destination) {
  memcpy(&(destination->id), source + ID_OFFSET, ID_SIZE);
  memcpy(&(destination->username), source + USERNAME_OFFSET, USERNAME_SIZE);
  memcpy(&(destination->email), source + EMAIL_OFFSET, EMAIL_SIZE);
}

const uint32_t PAGE_SIZE = 4096; //4k
#define TABLE_MAX_PAGES 100

// deprecated after switch to using B-tree
// const uint32_t ROWS_PER_PAGE = PAGE_SIZE / ROW_SIZE;
// const uint32_t TABLE_MAX_ROWS = ROWS_PER_PAGE * TABLE_MAX_PAGES;

typedef enum {
    NODE_INTERNAL_TYPE, 
    NODE_LEAF_TYPE
} NodeType;

/* node common header layout, all nodes will have this regardless of internal or leaf
Node Type: Enum
IsRoot: Bool
Parent pointer: pointer 
*/
const uint32_t NODE_TYPE_SIZE = sizeof(uint8_t); // 1 byte
const uint32_t NODE_TYPE_OFFSET = 0;
const uint32_t NODE_ISROOT_SIZE = sizeof(uint8_t);
const uint32_t NODE_ISROOT_OFFSET = NODE_TYPE_SIZE;
const uint32_t NODE_PARENTPTR_SIZE = sizeof(uint32_t); // change to void *? just pointer size of the current architecture
const uint32_t NODE_PARENTPTR_OFFSET = NODE_ISROOT_OFFSET+NODE_ISROOT_SIZE;
const uint32_t NODE_COMMON_HEADER_SIZE = 
    + NODE_TYPE_SIZE 
    + NODE_ISROOT_SIZE
    + NODE_PARENTPTR_SIZE;


NodeType node_type(void *node) {
    return (NodeType)*(uint8_t *)(node + NODE_TYPE_OFFSET);
}

void set_node_type(void *node, NodeType type) {
    *(uint8_t *)(node + NODE_TYPE_OFFSET) = (uint8_t)type;
}

/* Leaf node header layout 
NUM_CELLS: a variable storing how many cells(key-value pair) this leaf contains? 
*/
const uint32_t LEAF_NODE_NUM_CELLS_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_NUM_CELLS_OFFSET = NODE_COMMON_HEADER_SIZE;
const uint32_t LEAF_NODE_HEADER_SIZE =
    NODE_COMMON_HEADER_SIZE + LEAF_NODE_NUM_CELLS_SIZE;


/* leaf node body layout
*/
const uint32_t LEAF_NODE_KEY_SIZE = sizeof(uint32_t);
const uint32_t LEAF_NODE_KEY_OFFSET = 0;
const uint32_t LEAF_NODE_VALUE_SIZE = ROW_SIZE; // because the value we store is just a table row
const uint32_t LEAF_NODE_VALUE_OFFSET =
    LEAF_NODE_KEY_OFFSET + LEAF_NODE_KEY_SIZE; // the beginning position
const uint32_t LEAF_NODE_CELL_SIZE = LEAF_NODE_KEY_SIZE + LEAF_NODE_VALUE_SIZE;
const uint32_t LEAF_NODE_SPACE_FOR_CELLS = PAGE_SIZE - LEAF_NODE_HEADER_SIZE;
const uint32_t LEAF_NODE_MAX_CELLS =
    LEAF_NODE_SPACE_FOR_CELLS / LEAF_NODE_CELL_SIZE;  // C division takes floor by default
// maximum is 13 cells per node

/*
the node structure in memory goes like this:
[Node Header]
[Key, value] <-- Cell #1
[key, value] <-- Cell #2
.....
*/


/// @brief returns a pointer for the number of cells in this leaf node(which is a meta-data for the node)
/// @param node 
/// @return 
uint32_t *leaf_node_num_cells(void *node) {
    return node + LEAF_NODE_NUM_CELLS_OFFSET;
}

uint32_t *leaf_node_key_size(void *node) {
    return node + LEAF_NODE_KEY_OFFSET;
}

/// @brief returns the nth cell in this leaf node (not including header metadata)
/// @param node base address of node
/// @param cell_num the index of the wanted cell
/// @return 
void *leaf_node_cell(void *node, uint32_t cell_num) {
    return node + LEAF_NODE_HEADER_SIZE + cell_num * LEAF_NODE_CELL_SIZE;
}

/// @brief returns the key of the specified cell 
/// @param node 
/// @param cell_num 
/// @return 
void *leaf_node_key(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num);
}

void *leaf_node_value(void *node, uint32_t cell_num) {
    return leaf_node_cell(node, cell_num) + LEAF_NODE_KEY_SIZE; 
}

void initialize_leaf_node(void *node) {
    *leaf_node_num_cells(node) = 0;
    set_node_type(node, NODE_LEAF_TYPE);
}


// takes in a pager number, then return back a block of memory
// all fields except the pages are initialized at struct creation
typedef struct {
    int fd; // file descriptor
    uint32_t file_length;
    void *pages[TABLE_MAX_PAGES];
    uint32_t num_pages;
} Pager;

Pager *pager_open(const char *filename) {
    int fd = open(filename,
     	O_RDWR | 	// Read/Write mode
     	O_CREAT	// Create file if it does not exist
    );
    if(fd == -1){
        fprintf(stderr, "Can't open pager");
        exit(-1);
    }

    off_t file_length = lseek(fd, 0, SEEK_END);
    if(file_length == -1){
        fprintf(stderr, "lseek error");
        exit(-1);
    }
    Pager *pager = (Pager *)malloc(sizeof(Pager));
    pager->fd = fd;
    pager->file_length = file_length;
    pager->num_pages = file_length / PAGE_SIZE;

    if(file_length % PAGE_SIZE != 0) {
        printf("ERROR: Pager file length(%lu) is not a multiple of PAGE_SIZE, corrupted file!!!\n", (unsigned long)file_length); // unsigned long is unnecessary on an uint32, but just for something sake
        exit(EXIT_FAILURE);
    }

    // initialize to zeroes
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        pager->pages[i] = NULL;
    }

    return pager;
}

void* pager_get_page(Pager *pager, uint32_t page_num) {
    if(page_num > TABLE_MAX_PAGES){
        fprintf(stderr, "Page index can't be larger than the maximum");
        exit(-1);
    }

    if(pager->pages[page_num] == NULL) {
        void *page = malloc(PAGE_SIZE);
        printf("alloced %p\n", page); 

        uint32_t num_pages = pager->file_length / PAGE_SIZE;
        pager->pages[page_num] = page;
        
        if(pager->file_length % PAGE_SIZE > 0){ // file_length is the old one before adding the new page
            num_pages += 1;
        }

        if(page_num > num_pages) {
            fprintf(stderr, "page index out of range");
            exit(-1);
        }

        lseek(pager->fd, page_num*PAGE_SIZE, SEEK_SET);
        ssize_t bytes = read(pager->fd, page, PAGE_SIZE);
        if(bytes == -1){
            fprintf(stderr, "DB read failure");
            exit(-1);
        }

        pager->pages[page_num] = page;
        
        // if an out-of-range page is requested, then we auto increase the pager's size
        // but is this really what we wanted?
        if(page_num >= pager->num_pages) {
            printf("Out-of-range page (#%d) requested, increasing pager size\n", num_pages);
            pager->num_pages += 1;
        }

        printf("alloced %p\n", pager->pages[page_num]);
    }

    return pager->pages[page_num];
}

/// @brief writes the page to the database file?
/// @param pager 
/// @param page_num 
void pager_flush_page(Pager* pager, uint32_t page_num) {
  if (pager->pages[page_num] == NULL) {
    printf("Tried to flush null page\n");
    exit(EXIT_FAILURE);
  }

  off_t offset = lseek(pager->fd, page_num * PAGE_SIZE, SEEK_SET);

  if (offset == -1) {
    exit(EXIT_FAILURE);
  }

  ssize_t bytes_written = write(pager->fd, pager->pages[page_num], PAGE_SIZE);

  if (bytes_written == -1) {
    exit(EXIT_FAILURE);
  }
}

typedef struct {
    uint32_t root_page_num; // the page number of the root node
    Pager *pager;
    // void *pages[TABLE_MAX_PAGES];
} Table;

/// @brief returns a table 
/// @param filename 
/// @return 
Table *db_open(const char *filename) {
    Pager *pager = pager_open(filename);
    Table *table = (Table *)malloc(sizeof(Table));
    // table->num_rows = pager->file_length / ROW_SIZE;
    table->pager = pager;
    table->root_page_num = 0; // for now assume only 1 table opened at a time

    if(pager->num_pages == 0) {
        void *root_node = pager_get_page(pager, 0);
        initialize_leaf_node(root_node);
    }

    return table;
}

void db_close(Table *table){
    for (uint32_t i = 0; i < table->pager->num_pages; i++){
        if(table->pager->pages[i] == NULL){
            continue;
        } 
        pager_flush_page(table->pager, i);
        free(table->pager->pages[i]);
        table->pager->pages[i] = NULL;
    }
    
    // uint32_t num_additional_rows = table->num_rows % ROWS_PER_PAGE;
    // if (num_additional_rows > 0) {
    //     uint32_t page_num = num_full_pages;
    //     if (table->pager->pages[page_num] != NULL) {
    //         pager_flush_page(table->pager, page_num, num_additional_rows * ROW_SIZE);
    //         free(table->pager->pages[page_num]);
    //         table->pager->pages[page_num] = NULL;
    //     }
    // }

    int res = close(table->pager->fd);
    if(res == -1){
        fprintf(stderr, "error closing db");
        exit(-1);
    }

    // free all possible pages defined by TABLE_MAX_PAGES? don't think it will lead to memory leak
    for(uint32_t i = 0; i < TABLE_MAX_PAGES; i++){
        void *page = table->pager->pages[i];
    }

    free(table->pager);
    free(table);
}

void free_table(Table *table) {
    for (uint32_t i = 0; i < TABLE_MAX_PAGES; i++) {
        table->pager->pages[i] = NULL;
    }

    free(table);
}


/// @brief where to write the data bytes
/// @param table 
/// @param row_num 
/// @return 
// void *row_slot(Table *table, uint32_t row_num) {
//     uint32_t page_num = row_num / ROWS_PER_PAGE;
    
//     void *page = (void*)pager_get_page(table->pager, page_num);
//     // how many rows are we in the page
//     uint32_t row_offset = row_num % ROWS_PER_PAGE;
//     uint32_t bytes_offset = row_offset * ROW_SIZE; // how many bytes are we `in` the page

//     return page + bytes_offset;
// }
/// @brief the cursor object represents the current position in the table
typedef struct {
    Table* table;
    // uint32_t row_num; // no longer use since table is no longer an array of rows 
    // identify using the node's page number(since each node is exactly 1 page)
    uint32_t page_num;
    uint32_t cell_num; // and cell index within that node
    bool end_of_table; // Indicates a position one past the last element
} Cursor;

void leaf_node_insert(Cursor *cursor, uint32_t key, Row *value) {
    void *node = pager_get_page(cursor->table->pager, cursor->page_num);
    
    uint32_t num_cells = *leaf_node_num_cells(node);

    if(num_cells > LEAF_NODE_MAX_CELLS) {
        printf("this shouldn't reach\n");
        exit(EXIT_FAILURE);
    }

    if(cursor->cell_num < num_cells) {
        for(uint32_t i = num_cells; i > cursor->cell_num; i--) {
            memcpy(leaf_node_cell(node, i), leaf_node_cell(node, i-1), LEAF_NODE_CELL_SIZE);
        }
    }

    *leaf_node_num_cells(node) += 1;
    *(uint32_t *)leaf_node_key(node, cursor->cell_num) = key;
    serialize_row(value, leaf_node_cell(node, cursor->cell_num));
}

/// @brief creates a cursor pointing to a table's beginning
/// @param table 
/// @return 
Cursor* table_start(Table* table) {
    Cursor* cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    cursor->page_num = table->root_page_num;
    cursor->cell_num = 0;

    uint32_t *root_node = pager_get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells((void *)root_node);

    cursor->end_of_table = (num_cells == 0);  // if the root node is empty, then we're already at the end of table 

    return cursor;
}

/// @brief returns a cursor pointing to the end of table
/// @param table 
/// @return 
Cursor* table_end(Table* table) {
  Cursor* cursor = malloc(sizeof(Cursor));
  cursor->table = table;
  
  cursor->page_num = table->root_page_num; // todo: i dont understand why point to the root node
  void *root_node = pager_get_page(table->pager, cursor->page_num);
  uint32_t num_cells = *leaf_node_num_cells(root_node);
  cursor->cell_num = num_cells;  // point to the last cell. todo: starts at 0 or 1?
  cursor->end_of_table = true;
  
  return cursor;
}

/// @brief finds the index position of the cell by key
/// @param table 
/// @param key 
/// @return 0 on success, -1 if not found
uint32_t leaf_node_find(Table *table, uint32_t key, uint32_t *page_num_ret, uint32_t *cell_num_ret) {
    void *node = pager_get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    // bin search 
    uint32_t l = 0;
    uint32_t r = num_cells - 1; // last cell 
    uint32_t mid = 0;
    uint32_t result = 0;
    uint32_t key_at_index = 0;

    while(l <= r) {
        mid = (r + l) / 2;
        key_at_index = (uint32_t)*(uint32_t *)leaf_node_key(node, mid); 
        if(key_at_index == key) {
            *page_num_ret = table->root_page_num;
            *cell_num_ret = mid;
            return 0;
        } else if(key_at_index < key) {
            l = mid + 1;
        } else {
            r = mid;
        }
    }

    // if not found, return the index of another key which we need to move in order to insert the new record
    *page_num_ret = table->root_page_num;
    *cell_num_ret = l;
    return 0;
}

/// @brief return cursor at found cell
/// @param table 
/// @param key 
/// @return 
Cursor *table_find(Table *table, uint32_t key) {
    Cursor *cursor = malloc(sizeof(Cursor));
    cursor->table = table;
    uint32_t cell_num_ret;
    uint32_t page_num_ret;

    void *node = pager_get_page(table->pager, table->root_page_num);

    if(node_type(node) == NODE_LEAF_TYPE) {
        uint32_t i = leaf_node_find(table, key, &page_num_ret, &cell_num_ret);
        if (i == -1) {
            fprintf(stderr, "node not found in leaf node\n");
            free(cursor);
            return NULL;
        }

        cursor->page_num = page_num_ret;
        cursor->cell_num = cell_num_ret;
        return cursor;
    }
    else {
        fprintf(stderr, "to implement internal node key search\n");
        exit(EXIT_FAILURE);
    }
}

/// @brief returns the value currently being pointed by the cursor
/// @param cursor 
/// @return 
void* cursor_value(Cursor* cursor) {

  void *node = pager_get_page(cursor->table->pager, cursor->page_num);  
  void *ptr = leaf_node_value(node, cursor->cell_num);

  return ptr;

//   uint32_t row_num = cursor->row_num;
//   uint32_t page_num = row_num / ROWS_PER_PAGE;
//   void *page = pager_get_page(cursor->table->pager, page_num);
//   uint32_t row_offset = row_num % ROWS_PER_PAGE;
//   uint32_t byte_offset = row_offset * ROW_SIZE;
// return page + byte_offset;
}

void cursor_advance(Cursor* cursor) {
  cursor->cell_num += 1;

  void *node = pager_get_page(cursor->table->pager, cursor->page_num);
  // todo: why just the current node? there can be more nodes on the right though!
  if (cursor->cell_num >= *leaf_node_num_cells(node)) {
    cursor->end_of_table = true;
  }
 }

void print_row(Row *row) {
    printf("(%d, %s, %s)\n", row->id, row->username, row->email);
}

void print_leaf_node(void *node) {
	uint32_t num_cells = *leaf_node_num_cells(node);
	printf("leaf size: %lu\n", (unsigned long)num_cells);
    printf("leaf type: %d\n", (uint8_t)node_type(node));
	for(int i=0; i < num_cells; i++) {
		uint32_t key = *(uint32_t *)leaf_node_key(node, i);
		printf(" #%d - key %d - \n", i, key);
	}
}

#endif