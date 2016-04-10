/*
 * sqlite_utils.h
 *
 *  Created on: Apr 10, 2016
 *      Author: hadoop
 */

#ifndef SQLITE_UTILS_H_
#define SQLITE_UTILS_H_

#include <sqlite3.h>

struct
{
	int is_locked;
	int is_delete;
	int is_modified;
	int is_local;

} directory;

#endif /* SQLITE_UTILS_H_ */
