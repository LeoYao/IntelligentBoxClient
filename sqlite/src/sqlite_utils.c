/*
 * sqlite_utils.c
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#include <params.h>
#include <sqlite_utils.h>

int insert_directory(sqlite3* db1, directory* data){

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
