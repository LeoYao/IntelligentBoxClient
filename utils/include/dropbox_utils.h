/*
 * dropbox_utils.h
 *
 *  Created on: Apr 11, 2016
 *      Author: hadoop
 */

#ifndef DROPBOX_UTILS_H_
#define DROPBOX_UTILS_H_

#include <stddef.h>
#include <dropbox.h>

long parse_time(char* time_string);
int get_dbx_metadata(drbClient* cli, drbMetadata** output, const char* path);
int download_dbx_file(drbClient* cli, drbMetadata** metadata_ref, const char* remote_path, const char* local_path);
void release_dbx_metadata(drbMetadata* metadata);
#endif /* DROPBOX_UTILS_H_ */
