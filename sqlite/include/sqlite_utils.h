/*
 * sqlite_utils.h
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#ifndef SQLITE_UTILS_H_
#define SQLITE_UTILS_H_

#include <stddef.h>
#include <sqlite3.h>
#include <dropbox.h>

struct directory
{
	char* full_path;
	char* parent_folder_full_path;
	char* entry_name;
	char* old_full_path;
	int type;
	unsigned int size;
	sqlite_int64 mtime;
	sqlite_int64 atime;
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

directory* new_directory(const char* full_path,
		const char* parent_folder_full_path,
		const char* entry_name,
		const char* old_full_path,
		int type,
		unsigned int size,
		sqlite_int64 mtime,
		sqlite_int64 atime,
		int is_locked,
		int is_modified,
		int is_local,
		int is_delete,
		int in_use_count,
		char* revision);
directory* directory_from_dbx(drbMetadata* metadata);
void free_directory(directory* dir);
void free_directories(directory** dirs, int size);
directory* search_directory(sqlite3* db, char* full_path);
int update_time(sqlite3* db, char* full_path, int mode, long time);
int update_isLocal(sqlite3* db, char* full_path, int mode);
int update_isDeleted(sqlite3* db, char* full_path);
int update_isModified(sqlite3* db, char* full_path);
int insert_directory(sqlite3* db, directory* data);
int clean_subdirectories(sqlite3* db, char* parent_path);
directory** search_subdirectories(sqlite3* db, char* parent_path, int* count);

int begin_transaction(sqlite3* db);
int commit_transaction(sqlite3* db);
int rollback_transaction(sqlite3* db);

lru_entry* pop_lru(sqlite3* db, int create_transaction);
int push_lru(sqlite3* db, const char* path, int create_transaction);
int remove_lru(sqlite3* db, const char* path, int create_transaction);

#endif /* SQLITE_UTILS_H_ */
