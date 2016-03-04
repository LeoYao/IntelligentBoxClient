/*
 * dropbox_log_utils.hpp
 *
 *  Created on: Feb 20, 2016
 *      Author: hadoop
 */

#ifndef DROPBOX_LOG_UTILS_H_
#define DROPBOX_LOG_UTILS_H_

#include <common_utils.hpp>

extern "C"
{
	#include <log.h>
	#include <dropbox.h>
	#include <memStream.h>


	void displayAccountInfo(drbAccountInfo *info);
	void displayMetadata(drbMetadata *meta, char *title);
	void displayMetadataList(drbMetadataList* list, char* title);

}
#endif /* DROPBOX_LOG_UTILS_HPP_ */
