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

	char* sql = "BEGIN TRANSACTION;";
	char *zErrMsg = 0;
	//Begin transaction to database
	int rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		int k = 2;
		log_msg("\ni: %d %d\n", i, k);
		log_msg("\nrc2: %d %d\n", rc, SQLITE_BUSY);
		if(rc == SQLITE_BUSY){
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		}else{
			break;
		}
	}

	if (rc != SQLITE_OK){
		return -1;
	}
	sql = "UPDATE LOCK SET dummy = 1;";
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		int k = 2;
		log_msg("\ni: %d %d\n", i, k);
		log_msg("\nrc3: %d %d\n", rc, SQLITE_BUSY);
		if(rc == SQLITE_BUSY){
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		}else{
			return 0;
		}
	}
	log_msg("\nFailed to create a transaction\n");
	return -1;
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

lru_entry* select_lru(sqlite3* db, const char* path){
	lru_entry* result = NULL;
	char *err_msg = 0;
	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\nselect_lru: Begin\n");
    char* sql = "SELECT curr, prev, next FROM LRU_QUEUE where curr = ?";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("select_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
		if (rc == SQLITE_OK){
			log_msg("select_lru: Statement is binded.\n");
		}
		else {
			log_msg("select_lru: Statement is failed to bind: %s.\n", sqlite3_errmsg(db));
		}
	} else {
		log_msg("select_lru: Failed to prepare statement. Error message %s\n", sqlite3_errmsg(db));
	}

	int step = sqlite3_step(stmt);
	if (step == SQLITE_ROW){
		result = malloc(sizeof(lru_entry));

		char* tmp_curr = sqlite3_column_text(stmt, 0);
		char* tmp_prev = sqlite3_column_text(stmt, 1);
		char* tmp_next = sqlite3_column_text(stmt, 2);

		if (tmp_curr != NULL){
			int len_curr = strlen(tmp_curr);
			result->curr = malloc((len_curr + 1)*sizeof(char));
			strncpy(result->curr, tmp_curr, len_curr);
			result->curr[len_curr] = '\0';
		} else {
			result->curr = malloc(sizeof(char));
			result->curr[0] = '\0';
		}

		if (tmp_prev != NULL){
			int len_prev = strlen(tmp_prev);
			result->prev = malloc((len_prev + 1)*sizeof(char));
			strncpy(result->prev, tmp_prev, len_prev);
			result->prev[len_prev] = '\0';
		} else {
			result->prev = malloc(sizeof(char));
			result->prev[0] = '\0';
		}

		if (tmp_next != NULL){
			int len_next = strlen(tmp_next);
			result->next = malloc((len_next + 1)*sizeof(char));
			strncpy(result->next, tmp_next, len_next);
			result->next[len_next] = '\0';
		} else {
			result->next = malloc(sizeof(char));
			result->next[0] = '\0';
		}

		log_msg("select_lru: Successful for path [%s]\n", path);
	}else if (step == SQLITE_ROW){
		log_msg("select_lru: No record is found for path [%s]\n", path);
	}else {
		log_msg("select_lru: An Error Has Occured! Error message %s\n", sqlite3_errmsg(db));
	}

	sqlite3_free(err_msg);
	sqlite3_finalize(stmt);
	return result;
}



