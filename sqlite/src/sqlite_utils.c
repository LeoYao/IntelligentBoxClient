/*
 * sqlite_utils.c
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#include <sqlite_utils.h>


#include <common_utils.h>
#include <params.h>

static char* HEAD = ".head";
static char* TAIL = ".tail";


char* get_text(sqlite3_stmt* stmt, int col){
	char* result = NULL;
	char* tmp = sqlite3_column_text(stmt, col);
	result = copy_text(tmp);
	return result;
}

directory* new_directory(
		const char* full_path,
		const char* parent_folder_full_path,
		const char* entry_name,
		const char* old_full_path,
		int type,
		int size,
		sqlite_int64 mtime,
		sqlite_int64 atime,
		int is_locked,
		int is_modified,
		int is_local,
		int is_delete,
		int in_use_count,
		char* revision
		){
	directory* data = malloc(sizeof(directory));

	data->full_path = copy_text(full_path);
	data->parent_folder_full_path = copy_text(parent_folder_full_path);
	data->entry_name = copy_text(entry_name);
	data->old_full_path = copy_text(old_full_path);
	data->type = type;
	data->size = size;
	data->mtime = mtime;
	data->atime = atime;
	data->is_locked = is_locked;
	data->is_modified = is_modified;
	data->is_local = is_local;
	data->is_delete = is_delete;
	data->in_use_count = in_use_count;
	data->revision = copy_text(revision);

	return data;
}

void free_directory_content(directory* dir){
	if (dir == NULL){
			return;
	}

	free(dir->full_path);
	free(dir->entry_name);
	free(dir->old_full_path);
	free(dir->parent_folder_full_path);
	free(dir->revision);
}

void free_directory(directory* dir){
	if (dir == NULL){
		return;
	}

	free_directory_content(dir);

	free(dir);
}

void free_directories(directory** dirs, int size){
	for (int i = 0; i < size; ++i){
		free_directory_content(dirs[i]);
	}

	free(dirs);
}

directory* populate_directory(sqlite3_stmt *stmt){
	directory* data = malloc(sizeof(directory));
	data->full_path = get_text(stmt, 0);
	data->parent_folder_full_path = get_text(stmt,1);
	data->entry_name  = get_text(stmt, 2);
	data->old_full_path = get_text(stmt, 3);
	data->type = sqlite3_column_int(stmt, 4);
	data->size = sqlite3_column_int(stmt, 5);
	data->atime = sqlite3_column_int64(stmt, 6);
	data->mtime = sqlite3_column_int64(stmt, 7);
	data->is_locked = sqlite3_column_int(stmt, 8);
	data->is_modified = sqlite3_column_int(stmt, 9);
	data->is_local = sqlite3_column_int(stmt, 10);
	data->is_delete = sqlite3_column_int(stmt, 11);
	data->in_use_count = sqlite3_column_int(stmt, 12);
	data->revision = get_text(stmt, 13);
	return data;
}

directory* search_directory(sqlite3* db, char* full_path){

	log_msg("\nsearch_metadata: Begin\n");
	directory* data = NULL;
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "SELECT full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision FROM Directory WHERE full_path = ?";

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if( rc == SQLITE_OK){
		log_msg("search_metadata: Statement is prepared: %s\n", sql);
		sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
		log_msg("search_metadata: Statement is binded.\n");
	} else {
		log_msg("search_metadata: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK) {
		rc = sqlite3_step(stmt);

		if( rc == SQLITE_ROW){
			data = populate_directory(stmt);
		} else if (rc == SQLITE_DONE){
			log_msg("search_metadata: No record is found for path [%s]\n", full_path);
		}else {
			log_msg("search_metadata: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("search_metadata: Completed\n");
	return data;
}

directory** search_subdirectories(sqlite3* db, char* parent_path, int* count){

	log_msg("\nsearch_subdirectories: Begin\n");

	*count = 0;
	directory** datas = NULL;
	int capacity = 1;
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "SELECT full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision FROM Directory WHERE parent_folder_full_path = ?";

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if( rc == SQLITE_OK){
		log_msg("search_subdirectories: Statement is prepared: %s\n", sql);
		sqlite3_bind_text(stmt, 1, parent_path, -1, SQLITE_TRANSIENT);
		log_msg("search_subdirectories: Statement is binded.\n");
	} else {
		log_msg("search_subdirectories: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK) {
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW){
			datas = malloc(capacity * sizeof(directory*));
		}
		while(rc == SQLITE_ROW){
			datas[*count] = populate_directory(stmt);
			*count = *count + 1;

			if (*count == capacity){
				if (expand_mem(&datas, capacity) == EXPAND_OK){
					capacity *= 2;
				} else {
					log_msg("search_metadata: Failed to expand data.");
					break;
				}
			}
			rc = sqlite3_step(stmt);
		}

		if (rc != SQLITE_DONE){
			log_msg("search_metadata: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("search_subdirectories: Completed\n");
	return datas;
}

int update_isLocal(sqlite3* db, char* full_path){

	log_msg("\nupdate_isLocal: Begin\n");
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "UPDATE Directory SET is_local = 1 WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_isLocal: Statement is prepared: %s\n", sql);
		sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
		log_msg("update_isLocal: Statement is binded.\n");
	} else {
		log_msg("update_isLocal: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_isLocal: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_isLocal: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("update_isLocal: Completed\n");
	return 0;
}


int insert_directory(sqlite3* db, directory* data){

	log_msg("\ninsert_directory: Begin\n");
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "INSERT INTO Directory (full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\0";

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
	if (rc == SQLITE_OK) {
		log_msg("insert_directory: Statement is prepared: %s\n", sql);
        sqlite3_bind_text(stmt, 1, data->full_path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, data->parent_folder_full_path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, data->entry_name, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 4, data->old_full_path, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int(stmt, 5, data->type);
        sqlite3_bind_int(stmt, 6, data->size);
        sqlite3_bind_int(stmt, 7, data->atime);
        sqlite3_bind_int(stmt, 8, data->mtime);
        sqlite3_bind_int(stmt, 9, data->is_locked);
        sqlite3_bind_int(stmt, 10, data->is_modified);
        sqlite3_bind_int(stmt, 11, data->is_local);
        sqlite3_bind_int(stmt, 12, data->is_delete);
        sqlite3_bind_int(stmt, 13, data->in_use_count);
        sqlite3_bind_text(stmt, 14, data->revision, -1, SQLITE_TRANSIENT);
		log_msg("insert_directory: Statement is binded.\n");
	} else {
		log_msg("insert_directory: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("insert_directory: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("insert_directory: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("insert_directory: Completed\n");
	return 0;
}

//Initiating database
sqlite3* init_db(char* dbfile_path){

	int rc;
	char *zErrMsg = 0;

	sqlite3 *sqlite_conn = NULL;
	rc = sqlite3_open_v2(dbfile_path, &sqlite_conn, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if( rc ){
		log_msg("Can't open database: %s\n", sqlite3_errmsg(sqlite_conn));
		sqlite3_close(sqlite_conn);
		return(NULL);
	}

	// Create a table Directory for storage several metadata on the files
	// Or on Dropbox. If table already exists, ignore the SQL.
	char *sql = "CREATE TABLE IF NOT EXISTS Directory (full_path varchar(4000) PRIMARY KEY, parent_folder_full_path varchar(4000), entry_name varchar(255), old_full_path varchar(4000), type integer, size integer, mtime datetime, atime datetime, is_locked integer, is_modified integer, is_local integer, is_deleted integer, in_use_count integer, revision string);";
	log_msg("\ncreate_db: %s\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	sql = "create table if not exists LOCK (dummy char(1));";
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}


	sql = concat_string(2,
			"create table if not exists LRU_QUEUE\n",
	    	"(curr varchar(4000)  PRIMARY KEY,prev varchar(4000),next varchar(4000));");
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "insert or ignore into lru_queue (curr, next) values('.head', '.tail');";
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "insert or ignore into lru_queue (curr, prev) values('.tail', '.head');";
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	return (sqlite_conn);
}

int begin_transaction(sqlite3* db){

	log_msg("\nbegin_transaction: Begin\n");
	char* sql = "BEGIN TRANSACTION;";
	char *zErrMsg = 0;
	//Begin transaction to database
	int rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		if(rc == SQLITE_BUSY){
			free(zErrMsg);
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);

		}else{
			break;
		}
	}

	if (rc != SQLITE_OK){
		log_msg("begin_transaction: Failed to run 'BEGIN TRANSACTION'. Error message: %s\n", zErrMsg);
		free(zErrMsg);
		return rc;
	}
	sql = "UPDATE LOCK SET dummy = 1;";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		if(rc == SQLITE_BUSY){
			free(zErrMsg);
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		}else{
			break;
		}
	}
	if (rc != SQLITE_OK){
		log_msg("\nbegin_transaction: Failed to create a transaction. Error message: %s\n", zErrMsg);
		free(zErrMsg);
	}
	log_msg("begin_transaction: Completed\n");

	return rc;
}

int commit_transaction(sqlite3* db){
	log_msg("\ncommit_transaction: Begin\n");
	char *zErrMsg = 0;
	int rc = sqlite3_exec(db, "Commit Transaction;", 0, 0, &zErrMsg);

	if (rc != SQLITE_OK){
		log_msg("\commit_transaction: Failed to commit a transaction. Error message: %s\n", sqlite3_errmsg(db));
	}
	free(zErrMsg);
	log_msg("commit_transaction: Completed\n");

	return rc;
}

int rollback_transaction(sqlite3* db){
	log_msg("\nrollback_transaction: Begin\n");
	char *zErrMsg = 0;
	int rc = sqlite3_exec(db, "Rollback Transaction;", 0, 0, &zErrMsg);
	if (rc != SQLITE_OK){
		log_msg("rollback_transaction: Failed to rollback a trasaction. Error message: %s", zErrMsg);
	}
	free(zErrMsg);
	log_msg("rollback_transaction: Completed\n");
	return rc;
}

void free_lru(lru_entry* lru){
	if (lru == NULL){
		return;
	}

	free(lru->curr);
	free(lru->prev);
	free(lru->next);
	free(lru);
}

lru_entry* select_lru(sqlite3* db, const char* path){
	lru_entry* result = NULL;
	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\nselect_lru: Begin\n");
    char* sql = "SELECT curr, prev, next FROM LRU_QUEUE where curr = ?";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("select_lru: Statement is prepared: %s\n", sql);
		sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
		log_msg("select_lru: Statement is binded.\n");
	} else {
		log_msg("select_lru: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_ROW){
			result = (lru_entry*)malloc(sizeof(lru_entry));
			result->curr = get_text(stmt, 0);
			result->prev = get_text(stmt, 1);
			result->next = get_text(stmt, 2);
			log_msg("select_lru: Successful for path [%s]\n", path);
		}else if (rc == SQLITE_DONE){
			log_msg("select_lru: No record is found for path [%s]\n", path);
		}else {
			log_msg("select_lru: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("select_lru: Completed\n");
	return result;
}

int insert_lru(sqlite3* db, lru_entry* lru){

	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\ninsert_lru: Begin\n");

    char* sql = "INSERT INTO LRU_QUEUE (curr, prev, next) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("insert_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, lru->curr, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 2, lru->prev, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 3, lru->next, -1, SQLITE_TRANSIENT);
		log_msg("insert_lru: Statement is binded.\n");
	} else {
		log_msg("insert_lru: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("insert_lru: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("insert_lru: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);
	log_msg("insert_lru: Completed\n");

	return rc;
}

int delete_lru(sqlite3* db, const char* path){

	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\ndelete_lru: Begin\n");

	char* sql = "DELETE FROM LRU_QUEUE WHERE curr = ?;";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("delete_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
		log_msg("delete_lru: Statement is binded.\n");
	} else {
		log_msg("delete_lru: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("delete_lru: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("delete_lru: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);
	log_msg("delete_lru: Completed\n");

	return rc;
}



int update_lru(sqlite3* db, lru_entry* lru){

	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\nupdate_lru: Begin\n");

	char* sql = "UPDATE LRU_QUEUE SET prev = ?, next = ? WHERE curr = ?;";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, lru->prev, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 2, lru->next, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 3, lru->curr, -1, SQLITE_TRANSIENT);
		log_msg("update_lru: Statement is binded.\n");
	} else {
		log_msg("update_lru: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_lru: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_lru: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);
	log_msg("update_lru: Completed\n");

	return rc;
}

lru_entry* pop_lru(sqlite3* db, int create_transaction){
	int in_transaction = 0;
	int rc = 0;
	if (create_transaction){
		rc = begin_transaction(db);
		if (rc == SQLITE_OK){
			in_transaction = 1;
		}
	}

	if (in_transaction){
		if (rc == SQLITE_OK){
			rc = commit_transaction(db);
		} else {
			rc = rollback_transaction(db);
		}

	}
}

