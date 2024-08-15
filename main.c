#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/wait.h>

#include "structs.h"

#define DEFAULT_BUF_SIZE 100

void wldb_loop(void);
char *wldb_read_line(void);
char **wldb_split_line(char *line);
int wldb_process(char **args, Table *table);
CompileResult wldb_compile(char **args, Statement *statement);
ExecuteResult wldb_execute(Statement statement, Table *table);

int main(int argc, char **argv){
    // step1: load and execute configs

    wldb_loop();

    return 0;
}

void wldb_loop(void){
    char *line;
    char **args; // a pointer to a pointer to a series of characters
    int status;

    Table *table = db_open("database.db");

    do {
        printf("> ");
        line = wldb_read_line();
        args = wldb_split_line(line);
        status = wldb_process(args, table);
        
        free(line);
        free(args);
    } while(status == 0);

    return;
}

char *wldb_read_line(void){
    int bufsize = DEFAULT_BUF_SIZE;
    int position = 0;
    char *buffer = (char *)malloc(sizeof(char) * DEFAULT_BUF_SIZE);
    int c;

    if(!buffer){
        fprintf(stderr, "Buffer allocation failure\n");
        exit(-1);
    }   

    // this can be replaced with getline()
    while(1){
        c = getchar();
        //c = fgetc(stdin);

        if(c == EOF){
            printf("Shell exiting~");
            exit(0);
        } else if(c == '\n'){ 
            buffer[position] = '\0';
            return buffer;
        } else {
            buffer[position] = c;
            position++;
        }

        if(position >= bufsize){
            bufsize += DEFAULT_BUF_SIZE;
            buffer = realloc(buffer, bufsize);
            if(!buffer){
                fprintf(stderr, "Buffer reallocation failure\n");
                exit(-1);
            }
        }
    }
}


#define TOK_BUFSIZE 10      // allow 10 arguments
#define TOK_DELIM " \t\r\n\a"
char **wldb_split_line(char *line) {
    int bufsize = DEFAULT_BUF_SIZE;
    int position = 0;
    char **tokens = malloc(sizeof(char *) * bufsize); // allow bufsize number of arguments
    char *token; // each argument 

    if(!tokens){
        fprintf(stderr, "Can't allocate for tok buf\n");
        exit(-1);
    }

    token = strtok(line, TOK_DELIM);
    while(token){
        tokens[position] = token;
        position++;

        if(position >= bufsize){
            bufsize += TOK_BUFSIZE;
            tokens = realloc(tokens, bufsize);
            if(!tokens){
                fprintf(stderr, "realloc failed");
                exit(-1);
            }
        }
        token = strtok(NULL, TOK_DELIM);
    }
    tokens[position] = NULL; // so the returned pointer array is null-eneded.
    return tokens;
}


int wldb_cd(char **args, Table *table);
int wldb_exit(char **args, Table *table);
int wldb_btree_print(char **args, Table *table);

char *builtin_funcs_str[] = {
    "cd",
    "exit",
	"btree"
};

// list of function pointers
int (*builtin_funcs[]) (char **, Table *table) = {
    &wldb_cd,
    &wldb_exit,
	&wldb_btree_print
};

int wldb_num_builtins() {
    return sizeof(builtin_funcs_str) / sizeof(char *);
}

int wldb_cd(char **args, Table *table){
    if(args[1] == NULL) {
        fprintf(stderr, "No arg given to cd\n");
        exit(-1);
    }
    else {
        if(chdir(args[1]) != 0) {
            perror("Cd");
        }
    }
    return 0;
}

int wldb_exit(char **args, Table *table) {
    db_close(table);
    return -1; 
}

int wldb_btree_print(char **args, Table *table) {
	print_leaf_node(pager_get_page(table->pager, 0));
	return 0;
}

int wldb_process(char **args, Table *table){
    int num_builtins = wldb_num_builtins();
    if(strncmp(args[0], ".", 1) == 0) { // it's a meta command
        for(int i = 0; i < num_builtins; i++) {
            if(strcmp(builtin_funcs_str[i], args[0]+1) == 0) {
                return (*builtin_funcs[i])(args, table);
            }
        }
        fprintf(stderr, "Unrecognized meta command\n");
        return 0;
    }

    Statement statement;
    switch(wldb_compile(args, &statement)) {
        case COMPILE_SUCCESS:
            printf("Successfully compiled\n");
            break;
        case COMPILE_UNRECOGNIZED:
            fprintf(stderr, "Unrecognized SQL command\n");
            return -1;
            // return -1;
        case COMPILE_SYNTAX_ERROR:
            fprintf(stderr, "Compile failure\n");
            return -1;
        default:
            fprintf(stderr, "case defaulted!!??\n");
            return -1;
    }

    switch(wldb_execute(statement, table)) {
        case EXECUTE_SUCCESS:
            return 0;
        case EXECUTE_TABLE_FULL:
            fprintf(stderr, "Table was FULL :(\n");
            break;
        case EXECUTE_UNDEFINED_ERROR:
            fprintf(stderr, "Undefined execution error");
            return -1;
        default:
            fprintf(stderr, "what's going on? unknown execution result state\n");
            break;
    }
}

ExecuteResult wldb_select(Statement statement, Table *table);
ExecuteResult wldb_insert(Statement statement, Table *table);

/// @brief built-in SQL commands
StatementType sql_cmd_strs[] = {
    STATEMENT_SELECT,
    STATEMENT_INSERT
};

/// @brief list of function pointers, the order matters
ExecuteResult (*sql_cmd_funcs[]) (Statement, Table *) = {
    &wldb_select,
    &wldb_insert
};

ExecuteResult wldb_num_sql_builtins() {
    return sizeof(sql_cmd_strs) / sizeof(STATEMENT_SELECT);
}

ExecuteResult wldb_select(Statement statement, Table *table) {
    printf("select called\n");
    Row row;
    Cursor *cursor = table_start(table);
    // printf("%d\n", table->num_rows);

    while (!(cursor->end_of_table)) {
        deserialize_row(cursor_value(cursor), &row);
        print_row(&row);
        cursor_advance(cursor);
   }

    free(cursor);
    
    return EXECUTE_SUCCESS;
}

ExecuteResult wldb_insert(Statement statement, Table *table) {
    printf("insert called\n");

    void *node = pager_get_page(table->pager, table->root_page_num);
    uint32_t num_cells = *leaf_node_num_cells(node);

    if(num_cells > LEAF_NODE_MAX_CELLS) {
        printf("Max cell count reached, need to implement split node\n");
        return EXECUTE_TABLE_FULL;
    }

    Row *row_to_insert = &(statement.row_to_insert);
    uint32_t key_to_insert = row_to_insert->id;
    Cursor *cursor = table_find(table, key_to_insert);

    if(cursor->cell_num > num_cells) {
        printf("index reference out of range.\n");
        exit(EXIT_FAILURE);
    }

    uint32_t key_at_index = *(uint32_t *)leaf_node_key(node, cursor->cell_num);

    if(key_at_index == key_to_insert) {
        printf("insert error: duplicate key\n");
        return EXECUTE_DUPLICATE_KEY;
    }

    leaf_node_insert(cursor, row_to_insert->id, row_to_insert);

    free(cursor);

    return EXECUTE_SUCCESS;
}

// 这个其实很多余。。but just for the sake of logicality~
CompileResult wldb_compile(char **args, Statement *statement) {
    if(strcmp(args[0], "select") == 0) {
        statement->type = STATEMENT_SELECT;
        statement->args = args;
        return COMPILE_SUCCESS;
    }
    if(strcmp(args[0], "insert") == 0) {
        statement->type = STATEMENT_INSERT;
        statement->args = args;

        // todo: this lacks a args size check

        statement->row_to_insert.id = atoi(statement->args[1]);
        strcpy(statement->row_to_insert.username, statement->args[2]);
        strcpy(statement->row_to_insert.email, statement->args[3]);
        //strcpy(&(statement->row_to_insert.username), statement->args[1]);
        //strcpy(&(statement->row_to_insert.email), statement->args[2]);

        return COMPILE_SUCCESS;
    }

    // do some syntax check and return COMPILE_SYNTAX_ERROR

    return COMPILE_UNRECOGNIZED;
}

ExecuteResult wldb_execute(Statement statement, Table *table){
    int num_sql_builtins = wldb_num_sql_builtins();
    for(int i = 0; i < num_sql_builtins; i++) {
        if(sql_cmd_strs[i] == statement.type) { // comparing 2 enums
            // todo: could do some error handling for the return values of type ExecuteResult
            // e.g int ret = *(sql_cmd_funcs...), if(ret == EXECUTE_TABLE_FULL...etc)
            // or just deal in each individual funcs, also fine actually...
            return (*sql_cmd_funcs[i])(statement, table);
        }   
    }
    return EXECUTE_UNDEFINED_ERROR;
}
