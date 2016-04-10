/*
 * sqlite_utils.h
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#ifndef SQLITE_UTILS_H_
#define SQLITE_UTILS_H_

#include <common_utils.h>
#include <params.h>
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

int insert_directory(sqlite3* db, directory* data);
int begin_transaction(sqlite3* db);
int commit_transaction(sqlite3* db);
int rollback_transaction(sqlite3* db);

#endif /* SQLITE_UTILS_H_ */
