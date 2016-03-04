/*
 * dropbox_log_utils.cpp
 *
 *  Created on: Feb 20, 2016
 *      Author: hadoop
 */


#include <common_utils.hpp>
#include <log.h>
#include <dropbox.h>
#include <memStream.h>


char* strFromBool(bool b) { return b ? "true" : "false"; }

/*!
 * \brief  Display a drbAccountInfo item in stdout.
 * \param  info    account informations to display.
 * \return  void
 */
void displayAccountInfo(drbAccountInfo* info) {
    if(info) {
    	log_msg("\n---------[ Account info ]---------\n");
        if(info->referralLink)         log_msg("referralLink: %s\n", info->referralLink);
        if(info->displayName)          log_msg("displayName:  %s\n", info->displayName);
        if(info->uid)                  log_msg("uid:          %d\n", *info->uid);
        if(info->country)              log_msg("country:      %s\n", info->country);
        if(info->email)                log_msg("email:        %s\n", info->email);
        if(info->quotaInfo.datastores) log_msg("datastores:   %u\n", *info->quotaInfo.datastores);
        if(info->quotaInfo.shared)     log_msg("shared:       %u\n", *info->quotaInfo.shared);
        if(info->quotaInfo.quota)      log_msg("quota:        %u\n", *info->quotaInfo.quota);
        if(info->quotaInfo.normal)     log_msg("normal:       %u\n", *info->quotaInfo.normal);
    }
}

/*!
 * \brief  Display a drbMetadata item in stdout.
 * \param  meta    metadata to display.
 * \param   title   display the title before the metadata.
 * \return  void
 */
void displayMetadata(drbMetadata* meta, char* title) {
    if (meta) {
        if(title) log_msg("\n---------[ %s ]---------\n", title);
        if(meta->hash)        log_msg("hash:        %s\n", meta->hash);
        if(meta->rev)         log_msg("rev:         %s\n", meta->rev);
        if(meta->thumbExists) log_msg("thumbExists: %s\n", strFromBool(*meta->thumbExists));
        if(meta->bytes)       log_msg("bytes:       %d\n", *meta->bytes);
        if(meta->modified)    log_msg("modified:    %s\n", meta->modified);
        if(meta->path)        log_msg("path:        %s\n", meta->path);
        if(meta->isDir)       log_msg("isDir:       %s\n", strFromBool(*meta->isDir));
        if(meta->icon)        log_msg("icon:        %s\n", meta->icon);
        if(meta->root)        log_msg("root:        %s\n", meta->root);
        if(meta->size)        log_msg("size:        %s\n", meta->size);
        if(meta->clientMtime) log_msg("clientMtime: %s\n", meta->clientMtime);
        if(meta->isDeleted)   log_msg("isDeleted:   %s\n", strFromBool(*meta->isDeleted));
        if(meta->mimeType)    log_msg("mimeType:    %s\n", meta->mimeType);
        if(meta->revision)    log_msg("revision:    %d\n", *meta->revision);
        if(meta->contents)    displayMetadataList(meta->contents, "Contents");
    }
}

/*!
 * \brief  Display a drbMetadataList item in stdout.
 * \param  list    list to display.
 * \param   title   display the title before the list.
 * \return  void
 */
void displayMetadataList(drbMetadataList* list, char* title) {
    if (list){
    	log_msg("\n---------[ %s ]---------\n", title);
        for (int i = 0; i < list->size; i++) {

            displayMetadata(list->array[i], list->array[i]->path);
        }
    }
}
