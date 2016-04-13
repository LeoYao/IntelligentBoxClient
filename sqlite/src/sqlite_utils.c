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
		unsigned int size,
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

directory* directory_from_dbx(drbMetadata* metadata){
	char* full_path = metadata->path;
	char* parent_path = get_parent_path(metadata->path);
	char* name = get_file_name(metadata->path);

	int type = 2;
	if (*(metadata->isDir))
	{
		type = 1;
	}

	unsigned int size = *(metadata->bytes);
	long mtime = parse_time(metadata->modified);
	long atime = parse_time(metadata->modified);

	directory* dir = new_directory(
			full_path,
			parent_path,
			name,
			"",
			type,
			size,
			mtime,
			atime,
			0,
			0,
			0,
			0,
			0,
			""
			);
	free(parent_path);
	free(name);

	return dir;
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

directory* search_directory(sqlite3* db, const char* full_path){

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

directory** search_subdirectories(sqlite3* db, const char* parent_path, int* count, int include_deleted){

	log_msg("\nsearch_subdirectories: Begin\n");

	*count = 0;
	directory** datas = NULL;
	int capacity = 1;
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "SELECT full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision FROM Directory WHERE parent_folder_full_path = lower(?)";

	if (!include_deleted){
		sql = "SELECT full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision FROM Directory WHERE parent_folder_full_path = lower(?) and is_deleted = 0";
	}

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
				if (expand_mem(&datas, capacity * sizeof(directory*)) == EXPAND_OK){
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

int update_time(sqlite3* db, const char* full_path, int mode, long time){
	log_msg("\nUpdate_Time: Begin [%s], mode[%d], time[%ld]\n", full_path, mode, time);
	sqlite3_stmt *stmt;
	int rc;
	char* sql;
	if(mode == 1){
		sql = "UPDATE Directory SET atime = ? WHERE full_path = ?\0";
		log_msg("\nUpdate mtime!\n");
	} else {
		sql = "UPDATE Directory SET mtime = ? WHERE full_path = ?\0";
		log_msg("\nUpdate atime!\n");
	}

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_isLocal: Statement is prepared: %s\n", sql);
		rc += sqlite3_bind_int(stmt, 1, time);
		rc += sqlite3_bind_text(stmt, 2, full_path, -1, SQLITE_TRANSIENT);
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
	return rc;
}

int update_isLocal(sqlite3* db, const char* full_path, int mode){

	log_msg("\nupdate_isLocal: Begin [%s], mode [%d]\n", full_path, mode);
	sqlite3_stmt *stmt;
	int rc;
	char* sql;
	if(mode == 1){
		sql = "UPDATE Directory SET is_local = 1 WHERE full_path = ?\0";
		}else if(mode == 0){
		sql = "UPDATE Directory SET is_local = 0 WHERE full_path = ?\0";
	}
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_isLocal: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
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
	return rc;
}

int update_isModified(sqlite3* db, const char* full_path){

	log_msg("\nupdate_isModified: Begin [%s]\n", full_path);
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "UPDATE Directory SET is_modified = 1 WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_isModified: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
		log_msg("update_isModified: Statement is binded.\n");
	} else {
		log_msg("update_isModified: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_isModified: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_isModified: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("update_isModified: Completed\n");
	return rc;
}

int update_isDeleted(sqlite3* db, const char* full_path){
	log_msg("\nupdate_isDeleted: Begin [%s]\n", full_path);
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "UPDATE Directory SET is_deleted = 1 WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_isDeleted: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
		log_msg("update_isDeleted: Statement is binded.\n");
	} else {
		log_msg("update_isDeleted: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_isDeleted: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_isDeleted: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("update_isDeleted: Completed\n");
	return rc;
}


int update_in_use_count(sqlite3* db, const char* full_path, int delta){
	log_msg("\nupdate_in_use_count: Begin [%s]\n", full_path);
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "UPDATE Directory SET in_use_count = MAX(0, in_use_count + ?) WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_in_use_count: Statement is prepared: %s\n", sql);
		rc += sqlite3_bind_int(stmt, 1, delta);
		rc += sqlite3_bind_text(stmt, 2, full_path, -1, SQLITE_TRANSIENT);
		log_msg("update_in_use_count: Statement is binded.\n");
	} else {
		log_msg("update_in_use_count: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_in_use_count: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_in_use_count: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("update_in_use_count: Completed\n");
	return rc;
}

int update_size(sqlite3* db, const char* full_path, int size){
	log_msg("\nupdate_size: Begin [%s], size[%d]\n", full_path, size);
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "UPDATE Directory SET size = ? WHERE full_path = ?\0";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("update_in_use_count: Statement is prepared: %s\n", sql);
		rc += sqlite3_bind_int(stmt, 1, size);
		rc += sqlite3_bind_text(stmt, 2, full_path, -1, SQLITE_TRANSIENT);
		log_msg("update_in_use_count: Statement is binded.\n");
	} else {
		log_msg("update_in_use_count: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("update_in_use_count: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("update_in_use_count: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("update_in_use_count: Completed\n");
	return rc;
}

int insert_directory(sqlite3* db, directory* data){

	log_msg("\ninsert_directory: Begin [%s]\n", data->full_path);
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "INSERT INTO Directory (full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision) VALUES( lower(?), lower(?), lower(?), lower(?), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)\0";

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
	if (rc == SQLITE_OK) {
		log_msg("insert_directory: Statement is prepared: %s\n", sql);
		rc += sqlite3_bind_text(stmt, 1, data->full_path, -1, SQLITE_TRANSIENT);
		rc += sqlite3_bind_text(stmt, 2, data->parent_folder_full_path, -1, SQLITE_TRANSIENT);
		rc += sqlite3_bind_text(stmt, 3, data->entry_name, -1, SQLITE_TRANSIENT);
		rc += sqlite3_bind_text(stmt, 4, data->old_full_path, -1, SQLITE_TRANSIENT);
		rc += sqlite3_bind_int(stmt, 5, data->type);
		rc += sqlite3_bind_int(stmt, 6, data->size);
		rc += sqlite3_bind_int(stmt, 7, data->atime);
		rc += sqlite3_bind_int(stmt, 8, data->mtime);
		rc += sqlite3_bind_int(stmt, 9, data->is_locked);
		rc += sqlite3_bind_int(stmt, 10, data->is_modified);
		rc += sqlite3_bind_int(stmt, 11, data->is_local);
		rc += sqlite3_bind_int(stmt, 12, data->is_delete);
		rc += sqlite3_bind_int(stmt, 13, data->in_use_count);
		rc += sqlite3_bind_text(stmt, 14, data->revision, -1, SQLITE_TRANSIENT);
		rc += log_msg("insert_directory: Statement is binded.\n");
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


int delete_directory(sqlite3* db, const char* full_path){

	sqlite3_stmt *stmt = NULL;
	int rc;
	log_msg("\ndelete_directory: Begin\n");

	char* sql = "DELETE FROM directory WHERE full_path = ?;";
	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);

	if (rc == SQLITE_OK) {
		log_msg("delete_directory: Statement is prepared: %s\n", sql);
		rc = sqlite3_bind_text(stmt, 1, full_path, -1, SQLITE_TRANSIENT);
		log_msg("delete_directory: Statement is binded.\n");
	} else {
		log_msg("delete_directory: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("delete_directory: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("delete_directory: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);
	log_msg("delete_directory: Completed\n");

	return rc;
}

int clean_subdirectories(sqlite3* db, const char* parent_path){

	log_msg("\clean_subdirectories: Begin\n");
	sqlite3_stmt *stmt;
	int rc;
	char* sql = "delete from Directory where parent_folder_full_path = ?\0";

	rc = sqlite3_prepare_v2(db, sql, -1, &stmt, 0);
	if (rc == SQLITE_OK) {
		log_msg("clean_subdirectories: Statement is prepared: %s\n", sql);
		rc += sqlite3_bind_text(stmt, 1, parent_path, -1, SQLITE_TRANSIENT);
		log_msg("clean_subdirectories: Statement is binded.\n");
	} else {
		log_msg("clean_subdirectories: Failed to prepare statement. Error message: %s\n", sqlite3_errmsg(db));
	}

	if (rc == SQLITE_OK){
		rc = sqlite3_step(stmt);
		if (rc == SQLITE_DONE){
			log_msg("clean_subdirectories: Successful\n");
			rc = SQLITE_OK;
		}else {
			log_msg("clean_subdirectories: An Error Has Occured! Error message: %s\n", sqlite3_errmsg(db));
		}
	}

	sqlite3_finalize(stmt);

	log_msg("clean_subdirectories: Completed\n");
	return 0;
}

//Initiating database
sqlite3* init_db(const char* dbfile_path){

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
	char *sql = "CREATE TABLE IF NOT EXISTS Directory (full_path varchar(4000) PRIMARY KEY, parent_folder_full_path varchar(4000), entry_name varchar(255), old_full_path varchar(4000), type integer, size integer, mtime datetime, atime datetime, is_locked integer, is_modified integer, is_local integer, is_deleted integer, in_use_count integer, revision string);\0";
	log_msg("init_db: Creating table DIRECTORY\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("init_db: Failed to create table DIRECTORY. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed creating table DIRECTORY\n", sql);

	//Insert initial record for root folder
	unsigned int epoch_now = time(NULL);
	char epoch_now_str[15];
	memset(epoch_now_str, '\0', 15);
	sprintf(epoch_now_str, "%u", epoch_now);
	sql = concat_string(6,
			"insert or ignore into directory (full_path, parent_folder_full_path, entry_name, old_full_path, type, size, mtime, atime, is_locked, is_modified, is_local, is_deleted, in_use_count, revision)",
			"values('','.','','',1,0,",
			epoch_now_str,
			",",
			epoch_now_str,
			",0,0,0,0,0,'')");

	log_msg("init_db: Initializing table DIRECTORY\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("init_db: Failed to initialize table DIRECTORY. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed initializing table DIRECTORY\n", sql);
	free(sql);


	sql = "create table if not exists LOCK (dummy char(1));";
	log_msg("init_db: createing table LOCK\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("init_db: Failed to create table LOCK. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed creating table LOCK\n", sql);

	sql = concat_string(2,
			"create table if not exists LRU_QUEUE\n",
	    	"(curr varchar(4000)  PRIMARY KEY,prev varchar(4000),next varchar(4000));");
	log_msg("init_db: creating table LRU_QUEUE\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("init_db: Failed to create table LRU_QUEUE. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed creating table LRU_QUEUE\n", sql);

	sql = "insert or ignore into lru_queue (curr, next) values('.head', '.tail');";
	log_msg("init_db: initializing table LRU_QUEUE (head)\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		log_msg("init_db: Failed to initalize table LRU_QUEUE (head). Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed initalize table LRU_QUEUE (head)\n", sql);

	sql = "insert or ignore into lru_queue (curr, prev) values('.tail', '.head');";
	log_msg("init_db: initializing table LRU_QUEUE (tail)\n", sql);
	rc = sqlite3_exec(sqlite_conn, sql, 0, 0, &zErrMsg);
	if( rc != SQLITE_OK ){
		log_msg("init_db: Failed to initalize table LRU_QUEUE (tail). Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}
	log_msg("init_db: Completed initalize table LRU_QUEUE (head)\n", sql);

	return (sqlite_conn);
}

int begin_transaction(sqlite3* db){

	log_msg("\nbegin_transaction: Begin\n");
	char* sql = "BEGIN TRANSACTION;";
	char *zErrMsg = 0;
	//Begin transaction to database
	log_msg("begin_transaction: Runing [%s]\n", sql);
	int rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		if(rc == SQLITE_BUSY){
			log_msg("begin_transaction: Retrying [%d] times\n", i);
			sqlite3_free(zErrMsg);
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		}else{
			break;
		}
	}

	if (rc != SQLITE_OK){
		log_msg("begin_transaction: Failed to run 'BEGIN TRANSACTION'. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
		return rc;
	}

	sql = "UPDATE LOCK SET dummy = 1;";
	log_msg("begin_transaction: Runing [%s]\n", sql);
	rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
	//If SQLITE is busy, retry twice, if still busy then abort
	for(int i=0;i<2;i++){
		if(rc == SQLITE_BUSY){
			log_msg("begin_transaction: Retrying [%d] times\n", i);
			sqlite3_free(zErrMsg);
			delay(500);
			rc = sqlite3_exec(db, sql, 0, 0, &zErrMsg);
		}else{
			break;
		}
	}
	if (rc != SQLITE_OK){
		log_msg("\nbegin_transaction: Failed to create a transaction. Error message: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
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
	sqlite3_free(zErrMsg);
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
	sqlite3_free(zErrMsg);
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
	log_msg("\npop_lru: Begin\n");
	int in_transaction = 0;
	int rc = 0;
	lru_entry* result = NULL;
	lru_entry* head = NULL;
	lru_entry* next = NULL;

	if (create_transaction){
		log_msg("pop_lru: begin_transaction\n");
		rc = begin_transaction(db);
		if (rc == SQLITE_OK){
			in_transaction = 1;
		} else {
			log_msg("pop_lru: Failed to begin_transaction\n");
		}
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: find head [%s]\n", HEAD);
		head = select_lru(db, HEAD);
		if (head == NULL || (compare_string(head->next, TAIL))){
			log_msg("pop_lru: Failed to find head\n");
			rc = -1;
		}
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: find result [%s]\n", head->next);
		result = select_lru(db, head->next);
		if (result == NULL){
			log_msg("pop_lru: Failed to find result\n");
			rc = 1;
		}
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: find next [%s]\n", result->next);
		next = select_lru(db, result->next);
		if (next == NULL){
			log_msg("pop_lru: Failed to find next\n");
			rc = 1;
		}
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: update head->next\n");
		free(head->next);
		head->next = copy_text(result->next);
		rc = update_lru(db, head);
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: update next->prev\n");
		free(next->prev);
		next->prev = copy_text(result->prev);
		rc = update_lru(db, next);
	}

	if (rc == SQLITE_OK){
		log_msg("pop_lru: delete result\n");
		delete_lru(db, result->curr);
	}

	if (in_transaction){
		if (rc == SQLITE_OK){
			log_msg("pop_lru: commit_transaction\n");
			rc = commit_transaction(db);
		} else {
			log_msg("pop_lru: rollback_transaction\n");
			rc = rollback_transaction(db);
		}
	}

	log_msg("pop_lru: free memory\n");
	free_lru(head);
	free_lru(next);

	log_msg("pop_lru: Completed\n");
	return result;
}

int push_lru(sqlite3* db, const char* path, int create_transaction){
	log_msg("\npush_lru: Begin [%s]\n", path);
	int in_transaction = 0;
	int rc = 0;
	lru_entry* curr = NULL;
	lru_entry* prev = NULL;
	lru_entry* next = NULL;
	lru_entry* tail = NULL;
	lru_entry* tail_prev = NULL;

	if (create_transaction){
		log_msg("push_lru: begin_transaction\n");
		rc = begin_transaction(db);
		if (rc == SQLITE_OK){
			in_transaction = 1;
		} else {
			log_msg("push_lru: Failed to begin_transaction\n");
		}
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: find curr [%s]\n", path);
		curr = select_lru(db, path);
		if (curr == NULL){
			log_msg("push_lru: create curr [%s]\n", path);
			curr = (lru_entry*)malloc(sizeof(lru_entry));
			curr->curr = copy_text(path);
			curr->prev = copy_text("");
			curr->next = copy_text("");
			rc = insert_lru(db, curr);
		} else {
			if (rc == SQLITE_OK) {
				log_msg("push_lru: find prev [%s]\n", curr->prev);
				prev = select_lru(db, curr->prev);
				if (prev == NULL){
					log_msg("push_lru: Failed to find prev\n");
					rc = -1;
				}
			}

			if (rc == SQLITE_OK) {
				log_msg("push_lru: find next [%s]\n", curr->next);
				next = select_lru(db, curr->next);
				if (next == NULL){
					log_msg("push_lru: Failed to find next\n");
					rc = -1;
				}
			}

            if (rc == SQLITE_OK) {
            	log_msg("push_lru: update prev->next\n");
            	free(prev->next);
            	prev->next = copy_text(curr->next);
            	rc = update_lru(db, prev);
            }

            if (rc == SQLITE_OK) {
            	log_msg("push_lru: update next->prev\n");
            	free(next->prev);
            	next->prev = copy_text(curr->prev);
            	rc = update_lru(db, next);
            }
		}
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: find tail [%s]\n", TAIL);
		tail = select_lru(db, TAIL);
		if (tail == NULL){
			log_msg("push_lru: Failed to find tail\n");
			rc = 1;
		}
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: find tail_prev [%s]\n", tail->prev);
		tail_prev = select_lru(db, tail->prev);
		if (tail_prev == NULL){
			log_msg("push_lru: Failed to find tail_prev\n");
			rc = 1;
		}
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: update curr->next and curr->prev\n");
		free(curr->next);
		curr->next = copy_text(TAIL);
		free(curr->prev);
		curr->prev = copy_text(tail_prev->curr);
		rc = update_lru(db, curr);
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: update tail->prev)\n");
		free(tail->prev);
		tail->prev = copy_text(curr->curr);
		rc = update_lru(db, tail);
	}

	if (rc == SQLITE_OK){
		log_msg("push_lru: update tail_prev->next\n");
		free(tail_prev->next);
		tail_prev->next = copy_text(curr->curr);
		rc = update_lru(db, tail_prev);
	}

	if (in_transaction){
		if (rc == SQLITE_OK){
			log_msg("push_lru: commit_transaction\n");
			rc = commit_transaction(db);
		} else {
			log_msg("push_lru: rollback_transaction\n");
			rc = rollback_transaction(db);
		}
	}

	log_msg("push_lru: free memory\n");
	free_lru(curr);
	free_lru(prev);
	free_lru(next);
	free_lru(tail);
	free_lru(tail_prev);

	log_msg("push_lru: Completed\n");
	return rc;
}

int remove_lru(sqlite3* db, const char* path, int create_transaction){

	log_msg("\nremove_lru: Begin [%s]\n", path);
	int in_transaction = 0;
	int rc = 0;
	lru_entry* to_remove = NULL;
	lru_entry* prev = NULL;
	lru_entry* next = NULL;

	if (create_transaction){
		log_msg("remove_lru: begin_transaction\n");
		rc = begin_transaction(db);
		if (rc == SQLITE_OK){
			in_transaction = 1;
		} else {
			log_msg("remove_lru: Failed to begin_transaction\n");
		}
	}

	if (rc == SQLITE_OK){
		log_msg("remove_lru: find to_remove\n");
		to_remove = select_lru(db, path);
	}

	if (rc == SQLITE_OK && to_remove != NULL){
		if (rc == SQLITE_OK){
			log_msg("remove_lru: find prev\n");
			prev = select_lru(db, to_remove->prev);
			if (prev == NULL){
				log_msg("remove_lru: Failed to find prev\n");
				rc = -1;
			}
		}

		if (rc == SQLITE_OK){
			log_msg("remove_lru: find next\n");
			next = select_lru(db, to_remove->next);
			if (next == NULL){
				log_msg("remove_lru: Failed to find next\n");
				rc = -1;
			}
		}

		if (rc == SQLITE_OK){
			log_msg("remove_lru: update prev->next\n");
			free(prev->next);
			prev->next = copy_text(to_remove->next);
			rc = update_lru(db, prev);
		}

		if (rc == SQLITE_OK){
			log_msg("remove_lru: update next->prev\n");
			free(next->prev);
			next->prev = copy_text(to_remove->prev);
			rc = update_lru(db, next);
		}

		if (rc == SQLITE_OK){
			log_msg("remove_lru: delete to_remove\n");
			rc = delete_lru(db, to_remove->curr);
		}
	}

	if (in_transaction){
		if (rc == SQLITE_OK){
			log_msg("remove_lru: commit_transaction\n");
			rc = commit_transaction(db);
		} else {
			log_msg("remove_lru: rollback_transaction\n");
			rc = rollback_transaction(db);
		}
	}

	log_msg("remove_lru: release memory\n");
	free_lru(to_remove);
	free_lru(prev);
	free_lru(next);

	log_msg("remove_lru: Completed\n");

	return rc;
}
