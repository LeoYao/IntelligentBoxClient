/*
 * sqlite_utils.h
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#ifndef SQLITE_UTILS_H_
#define SQLITE_UTILS_H_

#include <sqlite3.h>

struct directory
{
	char* full_path;
	char* parent_folder_full_path;
	char* entry_name;
	char* old_full_path;
	int type;
	int size;
	long mtime;
	long atime;
	int is_locked;
	int is_modified;
	int is_local;
	int is_delete;
	int in_use_count;
	char* revision;
};

typedef struct directory directory;

struct lru_entry
{
	char* curr;
	char* prev;
	char* next;
};

typedef struct lru_entry lru_entry;

sqlite3* init_db(char* dbfile_path);
int insert_directory(sqlite3* db, directory* data);
int begin_transaction(sqlite3* db);
int commit_transaction(sqlite3* db);
int rollback_transaction(sqlite3* db);

lru_entry* select_lru(sqlite3* db, const char* path);
#endif /* SQLITE_UTILS_H_ */
