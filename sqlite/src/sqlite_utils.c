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

int search_metadata(sqlite3* db, char* full_path){
	int *i = 0;
	directory* data;
	sqlite3_stmt *res;
	int rc;
	char* sql = "SELECT * FROM Directory WHERE full_path = ?";
	rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
	printf("1");
	if( rc == SQLITE_OK){
		sqlite3_bind_text(res, 1, full_path, -1, SQLITE_TRANSIENT);
	}else{
        fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
        log_msg("\nError Occured Searching for metadata: %s\n", sqlite3_errmsg(db));
	}
	int step = sqlite3_step(res);


	if( step == SQLITE_ROW){
		i++;
		data->full_path = sqlite3_column_text(res, 0);
		data->parent_folder_full_path = sqlite3_column_text(res,1);
		data->entry_name  = sqlite3_column_text(res, 2);
		data->old_full_path = sqlite3_column_text(res, 3);
		data->type = sqlite3_column_text(res, 4);
		data->size = sqlite3_column_text(res, 5);
		data->atime = sqlite3_column_text(res, 6);
		data->mtime = sqlite3_column_text(res, 7);
		data->is_locked = sqlite3_column_text(res, 8);
		data->is_modified = sqlite3_column_text(res, 9);
		data->is_local = sqlite3_column_text(res, 10);
		data->is_delete = sqlite3_column_text(res, 11);
		data->in_use_count = sqlite3_column_text(res, 12);
		data->revision = sqlite3_column_text(res, 13);
		log_msg("\nSuccessfully get all the metadata of file %s\n", data->full_path);
		log_msg("\nSuccessfully get all the metadata of file %s\n", data->entry_name);
		log_msg("\nSuccessfully get all the metadata of file %s\n", data->mtime);
		log_msg("\nSuccessfully get all the metadata of file %s\n", data->is_local);

	}else if( step == SQLITE_DONE && i == "0"){
		log_msg("\nNo Such Directory Can Be Found!\n");
	}

	sqlite3_finalize(res);
	return 0;
}

int update_isLocal(sqlite3* db, char* full_path){
	char *err_msg = 0;
	sqlite3_stmt *res;
	int rc;
	char* sql = "UPDATE Directory SET is_local = 1 WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &res, 0);
	if(rc == SQLITE_OK){
		sqlite3_bind_text(res, 1, full_path, -1, SQLITE_TRANSIENT);
	}else {

        fprintf(stderr, "Failed to execute statement: %s\n", sqlite3_errmsg(db));
        log_msg("\nError Occured Updating is_local: %s\n", sqlite3_errmsg(db));
	}
	int step = sqlite3_step(res);
	 if( step== SQLITE_DONE){
	 log_msg("\nis_local UPDATED!\n");
	 }else if(step ==SQLITE_BUSY){
		 log_msg("\nSQLITE IS BUSY!");
	 }else{
		 log_msg("\nAn Error Has Occured: %s\n", sqlite3_errmsg(db));
	 }

	 sqlite3_free(err_msg);
	return 0;
}


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

char* get_text(sqlite3_stmt* stmt, int col){
	char* result = NULL;
	char* tmp = sqlite3_column_text(stmt, col);
	if (tmp != NULL){
		int len = strlen(tmp);
		result = malloc((len + 1)*sizeof(char));
		strncpy(result, tmp, len);
		result[len] = '\0';
	} else {
		result = malloc(sizeof(char));
		result[0] = '\0';
	}

	return result;
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
	char *err_msg = 0;
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
		log_msg("select_lru: Failed to prepare statement. Error message %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		int step = sqlite3_step(stmt);
		if (step == SQLITE_ROW){
			result = malloc(sizeof(lru_entry));
			result->curr = get_text(stmt, 0);
			result->prev = get_text(stmt, 1);
			result->next = get_text(stmt, 2);
			log_msg("select_lru: Successful for path [%s]\n", path);
		}else if (step == SQLITE_DONE){
			log_msg("select_lru: No record is found for path [%s]\n", path);
		}else {
			log_msg("select_lru: An Error Has Occured! Error message %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_free(err_msg);
	sqlite3_finalize(stmt);

	log_msg("select_lru: Completed\n");
	return result;
}

int insert_lru(sqlite3* db, lru_entry* lru){

	char *err_msg = 0;
	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\insert_lru: Begin\n");

    char* sql = "INSERT INTO LRU_QUEUE (curr, prev, next) VALUES (?, ?, ?);";
    rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("insert_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, lru->curr, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 2, lru->prev, -1, SQLITE_TRANSIENT);
		rc = sqlite3_bind_text(stmt, 3, lru->next, -1, SQLITE_TRANSIENT);
		log_msg("insert_lru: Statement is binded.\n");
	} else {
		log_msg("insert_lru: Failed to prepare statement. Error message %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		int step = sqlite3_step(stmt);
		if (step == SQLITE_DONE){
			log_msg("insert_lru: Successful\n");
		}else {
			log_msg("insert_lru: An Error Has Occured! Error message %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_free(err_msg);
	sqlite3_finalize(stmt);
	log_msg("insert_lru: Completed\n");

	return rc;
}

int delete_lru(sqlite3* db, const char* path){
	char *err_msg = 0;
	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\delete_lru: Begin\n");

	char* sql = "DELETE FROM LRU_QUEUE WHERE curr = ?;";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("delete_lru: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, path, -1, SQLITE_TRANSIENT);
		log_msg("delete_lru: Statement is binded.\n");
	} else {
		log_msg("delete_lru: Failed to prepare statement. Error message %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		int step = sqlite3_step(stmt);
		if (step == SQLITE_DONE){
			log_msg("delete_lru: Successful\n");
		}else {
			log_msg("delete_lru: An Error Has Occured! Error message %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_free(err_msg);
	sqlite3_finalize(stmt);
	log_msg("delete_lru: Completed\n");

	return rc;
}



int update_lru(sqlite3* db, lru_entry* lru){
	char *err_msg = 0;
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
		log_msg("update_lru: Failed to prepare statement. Error message %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		int step = sqlite3_step(stmt);
		if (step == SQLITE_DONE){
			log_msg("update_lru: Successful\n");
		}else {
			log_msg("update_lru: An Error Has Occured! Error message %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_free(err_msg);
	sqlite3_finalize(stmt);
	log_msg("update_lru: Completed\n");

	return rc;
}

