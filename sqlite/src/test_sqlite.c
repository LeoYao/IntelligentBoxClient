
#include <sqlite3.h>
#include <stdio.h>

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("-------------------------\n");
  return 0;
}

int test_sqlite(){
	sqlite3 *db1;
	char *zErrMsg = 0;
	int rc;
		rc = sqlite3_open_v2("test.db", &db1, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if( rc ){
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db1));
		sqlite3_close(db1);
		return(1);
	}

	//Clean the database
	char *sql = "DROP TABLE IF EXISTS Cars;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Create a new table
	sql = "CREATE TABLE Cars(Id INT, Name TEXT, Price INT);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Insert a record with autocommit
	sql = "INSERT INTO Cars VALUES(1, 'Audi', 52642);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Query the result
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Transaction
	sql = "BEGIN TRANSACTION;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "INSERT INTO Cars VALUES(2, 'Mercedes', 57127);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sql = "ROLLBACK TRANSACTION;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//No change should be made
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//Transaction with commiting
	sql = "BEGIN TRANSACTION;"
			"INSERT INTO Cars VALUES(3, 'Skoda', 9000);"
			"COMMIT;";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	//A new row is inserted
	rc = sqlite3_exec(db1, "select * from Cars", callback, 0, &zErrMsg);
	if( rc!=SQLITE_OK ){
		fprintf(stderr, "SQL error: %s\n", zErrMsg);
		sqlite3_free(zErrMsg);
	}

	sqlite3_close(db1);
	return 0;
}
