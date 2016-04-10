/*
 * sqlite_utils.c
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#include <sqlite_utils.h>

int insert_directory(sqlite3* db, directory* data){
	char *err_msg = 0;
	sqlite3_stmt *res;
	int rc;
	char* sql = "INSERT INTO Directory (full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision) VALUES( ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
	 if (rc == SQLITE_OK) {

	        sqlite3_bind_text(res, 1, data->full_path, -1, SQLITE_TRANSIENT);
	        sqlite3_bind_text(res, 2, data->parent_folder_full_path, -1, SQLITE_TRANSIENT);
	        sqlite3_bind_text(res, 3, data->entry_name, -1, SQLITE_TRANSIENT);
	        sqlite3_bind_text(res, 4, data->old_full_path, -1, SQLITE_TRANSIENT);
	        sqlite3_bind_int(res, 5, data->type);
	        sqlite3_bind_int(res, 6, data->size);
	        sqlite3_bind_int(res, 7, data->atime);
	        sqlite3_bind_int(res, 8, data->mtime);
	        sqlite3_bind_int(res, 9, data->is_locked);
	        sqlite3_bind_int(res, 10, data->is_modified);
	        sqlite3_bind_int(res, 11, data->is_local);
	        sqlite3_bind_int(res, 12, data->is_delete);
	        sqlite3_bind_int(res, 13, data->in_use_count);
	        sqlite3_bind_text(res, 14, data->revision, -1, SQLITE_TRANSIENT);
	        log_msg("\nInsert Directory SQL created!: %s\n", sql);

	    } else {

	        fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
	        log_msg("\nInsert Data Error Occured: %s\n", sqlite3_errmsg(db));

	    }
	 int step = sqlite3_step(res);
	 if( step== SQLITE_DONE){

	 log_msg("\nInserted into Directory!\n");
	 }else if(step ==SQLITE_BUSY){
		 log_msg("\nSQLITE IS BUSY!");
	 }else{
		 log_msg("\nAn Error Has Occured!");
	 }

	 sqlite3_free(err_msg);

	return 0;
}

int begin_transaction(sqlite3* db){
	char* sql_begin = "BEGIN TRANSACTION; UPDATE LOCK SET dummy = 1;";
	char *zErrMsg = 0;
	//Begin transaction to database
	int rc = sqlite3_exec(db, sql_begin, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		if(rc == SQLITE_BUSY){
			delay(50);
			rc = sqlite3_exec(db, sql_begin, 0, 0, &zErrMsg);
		}else{
			return 0;
		}
	}

	return 1;
}

int commit_transaction(sqlite3* db){

	char *zErrMsg = 0;
	int rc = sqlite3_exec(db, "Commit Transaction;", 0, 0, &zErrMsg);

	return rc;
}

int rollback_transaction(sqlite3* db){
	char *zErrMsg = 0;
	int rc = sqlite3_exec(db, "Rollback Transaction;", 0, 0, &zErrMsg);

	return rc;
}
