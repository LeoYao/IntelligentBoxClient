

#define FUSE_USE_VERSION 26
#define VMWARE

#include <params.h>
#include <ctype.h>
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <fuse.h>
#include <libgen.h>
#include <limits.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/types.h>
#include <pthread.h>

#include <dropbox.h>
#include <memStream.h>
#include <log.h>
#include <time.h>

#include <sqlite_utils.h>

#include <dropbox_log_utils.h>
#include <common_utils.h>

#include <sqlite3.h>


int is_log_to_file = 0;

// Report errors to logfile and give -errno to caller
static int ibc_error(char *str)
{
    int ret = -errno;

    log_msg("%s: Error message: %s\n", str, strerror(errno));

    return ret;
}

// Check whether the given user is permitted to perform the given operation on the given

//  All the paths I see are relative to the root of the mounted
//  filesystem.  In order to get to the underlying filesystem, I need to
//  have the mountpoint.  I'll save it away early on in main(), and then
//  whenever I need a path for something I'll call this to construct
//  it.
static void ibc_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->datadir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will break here

    //log_msg("    ibc_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	//BB_DATA->rootdir, path, fpath);
}

static int get_file_size(const char* fpath, off_t * size){
	int retstat = 0;
	int fd;
	struct stat stbuf;

	log_msg("get_file_size: open [%s]\n", fpath);
	fd = open(fpath, O_RDONLY);
	if (fd < 0) {
		retstat = ibc_error("get_file_size open");
	}


	if (retstat >= 0){
		log_msg("get_file_size: fstat\n");
		retstat = fstat(fd, &stbuf);
		if (retstat < 0) {
			retstat = ibc_error("get_file_size fstat");
		}
	}

	if (retstat >= 0){
		log_msg("get_file_size: S_ISREG\n");
		if (!S_ISREG(stbuf.st_mode)){
			log_msg("get_file_size: file is not a regular file\n");
			retstat = -1;
		}
	}

	if (retstat >= 0){
		*size = stbuf.st_size;
	}

	return retstat;
}
/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int ibc_mknod(const char *path, mode_t mode, dev_t dev)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n", path, mode, dev);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	pthread_mutex_lock(BB_DATA->lck);

	int retstat = 0;
	int err_sqlite = 0;
	//local full path
	char fpath[PATH_MAX];
	ibc_fullpath(fpath, path);

	//If it is pipe, just create a pipe
	if (S_ISFIFO(mode)) {
		log_msg("ibc_mknod: mkfifo is called\n");
		retstat = mkfifo(fpath, mode);
		if (retstat < 0)
			retstat = ibc_error("ibc_mknod mkfifo");
		return retstat;
	}

	//Not a pipe, so create a file now
	char* path_in_sqlite = path;
	char* parent_path_in_sqlite = get_parent_path(path);
	char* file_name = get_file_name(path);
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".");
	}

	directory* dir = NULL;
	log_msg("ibc_mknod: get_current_epoch_time\n");
	long now = get_current_epoch_time();

	//Search to see if the directory is already in the database
	log_msg("ibc_mknod: begin_transaction");
	int err_trans = begin_transaction(BB_DATA->sqlite_conn);

	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0)
	{
		log_msg("ibc_mknod: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);

		if (dir != NULL) {

			if (!(dir->is_delete)){
				retstat = -EEXIST;
			} else {
				log_msg("ibc_mknod: delete_directory [%s]\n", path_in_sqlite);
				//Clear old record
				delete_directory(BB_DATA->sqlite_conn, path_in_sqlite);
			}
		}
	}

	if (retstat >= 0){
		dir = new_directory(
				path_in_sqlite,
				parent_path_in_sqlite,
				file_name,
				"",
				2,
				0,
				now,
				now,
				0,
				1,
				1,
				0,
				0,
				""
				);

		log_msg("ibc_mknod: insert_directory\n");
		err_sqlite = insert_directory(BB_DATA->sqlite_conn, dir);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	//Always give full access to owner
	mode_t new_mode = mode | 0700;

	if (retstat >= 0){
		// On Linux this could just be 'mknod(path, mode, rdev)' but this
		//  is more portable
		if (S_ISREG(mode)) {
			log_msg("ibc_mknod: open [%s], mode: [0%3o]\n", fpath, new_mode);
			retstat = open(fpath, O_CREAT | O_WRONLY | O_TRUNC, new_mode);
			if (retstat < 0)
				retstat = ibc_error("bb_mknod open");
			else {
				log_msg("ibc_mknod: open\n");
				retstat = close(retstat);
				if (retstat < 0)
					retstat = ibc_error("bb_mknod close");
			}
		} else {
			log_msg("ibc_mknod: mknod[%s], mode: [0%3o]\n", fpath, new_mode);
			retstat = mknod(fpath, new_mode, dev);
			if (retstat < 0)
				retstat = ibc_error("bb_mknod mknod");
		}
	}

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_mknod: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_mknod: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_mknod: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	log_msg("ibc_mknod: free_memory\n");
	free_directory(dir);
	free(parent_path_in_sqlite);
	free(file_name);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_mknod: Completed\n");

	return retstat;
}

/** Create a directory */
int ibc_mkdir(const char *path, mode_t mode)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_mkdir(path=\"%s\", mode=0%3o)\n", path, mode);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    int retstat = 0;
    int err_sqlite = 0;

    pthread_mutex_lock(BB_DATA->lck);

    char* path_in_sqlite = path;
    char* parent_path_in_sqlite = get_parent_path(path);
    char* file_name = get_file_name(path);
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".");
	}
    char fpath[PATH_MAX];
    ibc_fullpath(fpath, path);

    directory* dir = NULL;

    log_msg("ibc_mkdir: get_current_epoch_time\n");
    long now = get_current_epoch_time();

	//Begin transaction
    log_msg("ibc_mkdir: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    //In case previous deleted metadata has not been synchronized
    if (retstat >= 0){
    	log_msg("ibc_mkdir: search_directory [%s]\n", path_in_sqlite);
    	dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (dir != NULL){
    		retstat = -EAGAIN;
    	}
    	free_directory(dir);
	}

    if (retstat >= 0){
    	dir = new_directory(
    				path_in_sqlite,
    				parent_path_in_sqlite,
    				file_name,
    				"",
    				1,
    				0,
    				now,
					now,
    				0,
    				1,
    				1,
    				0,
    				0,
    				""
    				);

    	log_msg("ibc_mkdir: insert_directory\n");
    	err_sqlite += insert_directory(BB_DATA->sqlite_conn, dir);

    	if (err_sqlite != 0){
    		retstat = -EIO;
    	}
    	free_directory(dir);
    }

    if (retstat >= 0){
		log_msg("ibc_mkdir: mkdir [%s], mode: [0%3o]\n", fpath, mode);
		retstat = mkdir(fpath, mode);
		if (retstat < 0){
			retstat = ibc_error("ibc_mkdir mkdir");
		}
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_mkdir: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_mkdir: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_mkdir: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}
	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_mkdir: Completed\n");
    return retstat;
}

/** Rename a file */
// both path and newpath are fs-relative
int ibc_rename(const char *path, const char *newpath)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_rename(path=\"%s\")\n",path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	directory* dir = NULL;

    int retstat = 0;
    int err_sqlite = 0;
    int err_dbx = 0;
    char fpath[PATH_MAX];
    char fnewpath[PATH_MAX];
    ibc_fullpath(fpath, path);
    ibc_fullpath(fnewpath, newpath);

    char* parent_path = get_parent_path(path);
	char* path_in_sqlite = path;
	char* parent_path_in_sqlite = get_parent_path(path);
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".\0");
	}
    char* parent_newpath = get_parent_path(newpath);
	char* newpath_in_sqlite = newpath;
	char* parent_newpath_in_sqlite = get_parent_path(newpath);
	char* new_filename = get_file_name(newpath);

	if (strlen(path) == 1){
		newpath_in_sqlite = "\0";
		parent_newpath_in_sqlite = copy_text(".\0");
	}

    //Begin transaction
	log_msg("ibc_rename: begin_transaction\n");
	int err_trans = begin_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		log_msg("ibc_rename: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
		if (dir == NULL || dir->is_delete){
			retstat = -ENOENT;
		}
	}

	if (retstat >= 0){
		// If the file is not on local, Download it from Dropbox
		if(dir->is_local == 0){
			drbMetadata* metadata;
			log_msg("ibc_rename: download_dbx_file from [%s] to [%s]\n", path, fpath);
			err_dbx = download_dbx_file(BB_DATA->client, &metadata, path, fpath);
			dir->is_local = 1;
			if (err_dbx != 0){
				retstat = -EIO;
			}
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: update_isDeleted [%s]\n", path_in_sqlite);
		err_sqlite = update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: update_isModified [%s]\n", path_in_sqlite);
		err_sqlite = update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: delete_directory [%s]\n", newpath_in_sqlite);
		err_sqlite = delete_directory(BB_DATA->sqlite_conn, newpath_in_sqlite);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		free(dir->full_path);
		free(dir->parent_folder_full_path);
		free(dir->entry_name);
		dir->full_path = copy_text(newpath_in_sqlite);
		dir->parent_folder_full_path = copy_text(parent_newpath_in_sqlite);
		dir->entry_name = copy_text(new_filename);
		dir->is_modified = 1;

		log_msg("ibc_rename: delete_directory [%s]\n", newpath_in_sqlite);
		err_sqlite = insert_directory(BB_DATA->sqlite_conn, dir);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: remove_lru [%s]\n", path_in_sqlite);
		err_sqlite = remove_lru(BB_DATA->sqlite_conn, path_in_sqlite, 0);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: push_lru [%s]\n", newpath_in_sqlite);
		err_sqlite = push_lru(BB_DATA->sqlite_conn, newpath_in_sqlite, 0);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_rename: rename from [%s] to [%s]\n", fpath, fnewpath);
		retstat = rename(fpath, fnewpath);
		if (retstat < 0)
			retstat = ibc_error("ibc_rename rename");
	}

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_rename: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_rename: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_rename: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

    free(parent_path);
    free(parent_path_in_sqlite);
    free(parent_newpath);
    free(parent_newpath_in_sqlite);
    free(new_filename);
    free_directory(dir);

    pthread_mutex_unlock(BB_DATA->lck);

    log_msg("ibc_rename: Completed\n");
    return retstat;
}

/** Remove a file */
int ibc_unlink(const char *path)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_unlink(path=\"%s\")\n",path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	directory* dir = NULL;

	pthread_mutex_lock(BB_DATA->lck);

    int retstat = 0;
    int err_dbx = DRBERR_OK;
    int err_sqlite = 0;

    char fpath[PATH_MAX];
	ibc_fullpath(fpath, path);

    char* path_in_sqlite = path;
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

    long now = get_current_epoch_time();

    //Begin transaction
    log_msg("ibc_unlink: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >= 0){
    	log_msg("ibc_unlink: search_directory [%s]\n", path_in_sqlite);
    	dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (dir == NULL || dir->is_delete){
    		retstat = -ENOENT;
    	}
    }

    if (retstat >= 0){
		if(dir->is_local == 0){
			log_msg("ibc_unlink: update_isModified [%s]\n", path_in_sqlite);
			err_sqlite += update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);

			log_msg("ibc_unlink: update_isDeleted [%s]\n", path_in_sqlite);
			err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);

			log_msg("ibc_unlink: update_time [%s], mode: atime\n", path_in_sqlite);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, now);

			log_msg("ibc_unlink: update_time [%s], mode: mtime\n", path_in_sqlite);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, now);

			if (err_sqlite != 0){
				retstat = -EIO;
			}
		} else {
			if (retstat >= 0){
				log_msg("ibc_unlink: update_isModified [%s]\n", path_in_sqlite);
				err_sqlite += update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_unlink: update_isLocal [%s]\n", path_in_sqlite);
				err_sqlite += update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 0);

				log_msg("ibc_unlink: update_isDeleted [%s]\n", path_in_sqlite);
				err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_unlink: update_time [%s], mode: atime\n", path_in_sqlite);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, now);

				log_msg("ibc_unlink: update_time [%s], mode: mtime\n", path_in_sqlite);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, now);

				if (err_sqlite != 0){
					retstat = -EIO;
				}
			}

			if (retstat >= 0){
				log_msg("ibc_unlink: unlink [%s]\n", fpath);
				retstat = unlink(fpath);
				if (retstat < 0){
					retstat = ibc_error("ibc_unlink unlink");
				}
			}
		}
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_unlink: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_unlink: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_unlink: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	log_msg("ibc_unlink: release memory\n");
	free_directory(dir);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_unlink: Completed\n");
	return retstat;
}

/** Remove a directory */
int ibc_rmdir(const char *path)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_rmdir(path=\"%s\")\n", path);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    pthread_mutex_lock(BB_DATA->lck);

    int retstat = 0;
    char* path_in_sqlite = path;
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}
    char fpath[PATH_MAX];

    int err_sqlite = 0;
    int subdir_cnt = 0;
    long now = get_current_epoch_time();

    directory* dir = NULL;
    directory** subdirs = NULL;

    log_msg("ibc_rmdir: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >=0){
    	log_msg("ibc_rmdir: search_directory [%s]\n", path_in_sqlite);
        dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
        if (dir == NULL || dir->is_delete){
        	retstat = -ENOENT;
        }
    }

    if (retstat >= 0){
		if(dir->is_local == 0){
			//Call dropbox without locking sqlite
			drbMetadata* metadata = NULL;
			if (retstat >= 0) {
				log_msg("ibc_opendir: get_dbx_metadata [%s]\n", path);
				int err_dbx = get_dbx_metadata(BB_DATA->client, &metadata, path);
				if (err_dbx != DRBERR_OK){
					log_msg("ibc_opendir: Failed to drbGetMetadata. Error Code: (%d).\n", err_dbx);
					retstat = -EIO;
				}
			}

			//Check if it is a folder on dropbox
			if (retstat >= 0 && !(dir->is_local)){
				if (!(*(metadata->isDir))){
					log_msg("ibc_opendir: dir is not a folder in dropbox\n");
					retstat = -ENOTDIR;
				}
			}

			if (retstat >= 0 && !(dir->is_local)){
				drbMetadataList* list = metadata->contents;
				int list_size = list->size;
				if (list_size > 0){
					retstat = -ENOTEMPTY;
				}
			}

			if (retstat >= 0) {
				log_msg("ibc_rmdir: update_isModified [%s]\n", path_in_sqlite);
				err_sqlite += update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_rmdir: update_isDeleted [%s]\n", path_in_sqlite);
				err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_rmdir: update atime [%s], time[%ld]\n", path_in_sqlite, now);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, now);

				log_msg("ibc_rmdir: update mtime [%s], time[%ld]\n", path_in_sqlite, now);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, now);
				if (err_sqlite != 0){
					retstat = -EIO;
				}
			}
		}else{
			subdirs = search_subdirectories(BB_DATA->sqlite_conn, path_in_sqlite, &subdir_cnt, 0);
			if (subdirs != NULL){
				retstat = -ENOTEMPTY;
			}

			if (retstat >= 0){
				log_msg("ibc_rmdir: update_isModified [%s]\n", path_in_sqlite);
				err_sqlite += update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_rmdir: update_isDeleted [%s]\n", path_in_sqlite);
				err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);

				log_msg("ibc_rmdir: update_isLocal to 0 [%s]\n", path_in_sqlite);
				err_sqlite += update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 0);

				log_msg("ibc_rmdir: update atime [%s], time[%ld]\n", path_in_sqlite, now);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, now);

				log_msg("ibc_rmdir: update mtime [%s], time[%ld]\n", path_in_sqlite, now);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, now);

				if (err_sqlite != 0){
					retstat = -EIO;
				}
			}

			if (retstat >= 0){
			    ibc_fullpath(fpath, path);
				retstat = rmdir(fpath);
				if (retstat < 0)
					retstat = ibc_error("ibc_rmdir rmdir");
			}
		}
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_rmdir: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_rmdir: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_rmdir: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	log_msg("ibc_rmdir: release memory\n");
	free_directory(dir);
	free_directories(subdirs, subdir_cnt);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_rmdir: Completed\n");
	return retstat;
}


/** Change the size of a file */
int ibc_truncate(const char *path, off_t newsize)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_truncate(path=\"%s\", newsize=%lld)\n", path, newsize);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    int retstat = 0;
    int err_sqlite;
    char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}
    char fpath[PATH_MAX];
    ibc_fullpath(fpath, path);

    log_msg("ibc_truncate: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		log_msg("ibc_truncate: update_size [%s], size [%lld]\n", path_in_sqlite, newsize);
		err_sqlite = update_size(BB_DATA->sqlite_conn, path_in_sqlite, newsize);
		if (err_sqlite != SQLITE_OK){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_truncate: truncate [%s], size [%lld]\n", fpath, newsize);
		retstat = truncate(fpath, newsize);
		if (retstat < 0)
			retstat = ibc_error("bb_truncate truncate");
	}

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_truncate: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_truncate: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_truncate: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	log_msg("ibc_truncate: Completed\n");
	return retstat;
}

/** Change the access and/or modification times of a file */
/* note -- I'll want to change this as soon as 2.6 is in debian testing */
int ibc_utime(const char *path, struct utimbuf *ubuf)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_utime(path=\"%s\", ubuf=0x%08x)\n",path, ubuf);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

    int retstat = 0;
    int err_sqlite;
    char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}
    char fpath[PATH_MAX];
    ibc_fullpath(fpath, path);

    log_msg("ibc_utime: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		err_sqlite = 0;
		log_msg("ibc_utime: update atime [%s], time[%ld]\n", path_in_sqlite, ubuf->actime);
		err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, ubuf->actime);

		log_msg("ibc_utime: update mtime [%s], time[%ld]\n", path_in_sqlite, ubuf->modtime);
		err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, ubuf->modtime);

		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_utime: utime\n");
		retstat = utime(fpath, ubuf);
		if (retstat < 0)
			retstat = ibc_error("ibc_utime utime");
	}

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_utime: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_utime: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_utime: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("utime: Completed\n");
    return retstat;
}

/** File open operation
 *
 * No creation, or truncation flags (O_CREAT, O_EXCL, O_TRUNC)
 * will be passed to open().  Open should check if the operation
 * is permitted for the given flags.  Optionally open may also
 * return an arbitrary filehandle in the fuse_file_info structure,
 * which will be passed to all file operations.
 *
 * Changed in version 2.2
 *
 * ychen71: If the file is on local storage (search database), open it
 * Otherwise download the file from Dropbox, update on database then open it.
 *
 *
 */
int ibc_open(const char *path, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_open: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}
	int err_dbx = 0;
	int err_sqlite = 0;
    int retstat = 0;
    int fd;
    directory* dir = NULL;

    //local full path
    char fpath[PATH_MAX];
    ibc_fullpath(fpath, path);

    long now = get_current_epoch_time();

    //Search to see if the directory is already in the database
    log_msg("ibc_open: begin_transaction\n");
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);

    if (err_trans != 0){
    	retstat = -EBUSY;
	}

    if (retstat >= 0)
    {
    	log_msg("ibc_open: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);

		if (dir == NULL || dir->is_delete) {
			retstat = -ENOENT;
		}
    }

	if (retstat >= 0){
		log_msg("ibc_open: update_isLocal [%s]\n", path_in_sqlite);
		err_sqlite = update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 1);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_open: update atime [%s], time[%ld]\n", path_in_sqlite, now);
		err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, now);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_open: update_in_use_count [%s]\n", path_in_sqlite);
		err_sqlite += update_in_use_count(BB_DATA->sqlite_conn, path_in_sqlite, 1);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_open: push_lru [%s]\n", path_in_sqlite);
		err_sqlite += push_lru(BB_DATA->sqlite_conn, path_in_sqlite, 0);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}


	if (retstat >= 0) {
		// If the file is not on local, Download it from Dropbox
		if(dir->is_local == 0){
			drbMetadata* metadata;
			log_msg("ibc_open: download_dbx_file from [%s] to [%s]\n", path, fpath);
			err_dbx = download_dbx_file(BB_DATA->client, &metadata, path, fpath);

			if (err_dbx != 0){
				retstat = -EIO;
			}
		}
	}

	if (retstat >= 0){
		log_msg("ibc_open: open [%s], mode: [0%10o]\n", fpath, fi->flags);
		fd = open(fpath, fi->flags);
		if (fd < 0)
			retstat = ibc_error("bb_open open\n");
		else{
			log_msg("ibc_open: file is opened.\n");
			fi->fh = fd;
		}
	}

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_open: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_open: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_open: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	free_directory(dir);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_open: Completed\n");
	return retstat;

}

/** Read data from an open file
 *
 * Read should return exactly the number of bytes requested except
 * on EOF or error, otherwise the rest of the data will be
 * substituted with zeroes.  An exception to this is when the
 * 'direct_io' mount option is specified, in which case the return
 * value of the read system call will reflect the return value of
 * this operation.
 *
 * Changed in version 2.2
 */
// I don't fully understand the documentation above -- it doesn't
// match the documentation for the read() system call which says it
// can return with anything up to the amount of data requested. nor
// with the fusexmp code which returns the amount of data also
// returned by read.
int ibc_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_read: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    int retstat = 0;

	log_msg("ibc_read: pread. fi->fh: [%lld], size: [%d], offset: [%lld]\n", fi->fh, size, offset);
    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0)
    	retstat = ibc_error("bb_read read");

    log_msg("ibc_read: Completed\n");
    return retstat;
}

/** Write data to an open file
 *
 * Write should return exactly the number of bytes requested
 * except on error.  An exception to this is when the 'direct_io'
 * mount option is specified (see read operation).
 *
 * Changed in version 2.2
 */
// As  with read(), the documentation above is inconsistent with the
// documentation for the write() system call.
int ibc_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_write: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	int err_sqlite = 0;
    int retstat = 0;
    int err_trans = -1;

    log_msg("ibc_write: begin_transaction\n");
    err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >= 0){
    	log_msg("ibc_write: update_isModified [%s]\n", path_in_sqlite);
    	err_sqlite = update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (err_sqlite != 0){
    		retstat = -EIO;
    	}
    }

    if (retstat >= 0){
    	log_msg("ibc_write: pwrite. fi->fh: [%lld], size: [%d], offset: [%lld]\n", fi->fh, size, offset);
    	retstat = pwrite(fi->fh, buf, size, offset);
    	if (retstat < 0)
    		ibc_error("ibc_write pwrite");
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_write: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_write: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_write: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_write: Completed\n");
	return retstat;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int ibc_statfs(const char *path, struct statvfs *statv)
{

    log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_statfs(path=\"%s\", statv=0x%08x)\n", path, statv);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    int retstat = 0;
    char fpath[PATH_MAX];
    ibc_fullpath(fpath, path);

    // get stats for underlying filesystem
    retstat = statvfs(fpath, statv);
    if (retstat < 0)
    	retstat = ibc_error("bb_statfs statvfs");

    //log_statvfs(statv);

    return retstat;
}

/** Possibly flush cached data
 *
 * BIG NOTE: This is not equivalent to fsync().  It's not a
 * request to sync dirty data.
 *
 * Flush is called on each close() of a file descriptor.  So if a
 * filesystem wants to return write errors in close() and the file
 * has cached dirty data, this is a good place to write back data
 * and return any errors.  Since many applications ignore close()
 * errors this is not always useful.
 *
 * NOTE: The flush() method may be called more than once for each
 * open().  This happens if more than one file descriptor refers
 * to an opened file due to dup(), dup2() or fork() calls.  It is
 * not possible to determine if a flush is final, so each flush
 * should be treated equally.  Multiple write-flush sequences are
 * relatively rare, so this shouldn't be a problem.
 *
 * Filesystems shouldn't assume that flush will always be called
 * after some writes, or that if will be called at all.
 *
 * Changed in version 2.2
 */
int ibc_flush(const char *path, struct fuse_file_info *fi)
{
    log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_flush(path=\"%s\", fi=0x%08x)\n", path, fi);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    int retstat = 0;

    //log_msg("bb_flush: fsync\n");
    //retstat = fsync(fi->fh);
    //if (retstat < 0)
    //	retstat = ibc_error("bb_flush fsync");

    return retstat;
}

/** Release an open file
 *
 * Release is called when there are no more references to an open
 * file: all file descriptors are closed and all memory mappings
 * are unmapped.
 *
 * For every open() call there will be exactly one release() call
 * with the same flags and file descriptor.  It is possible to
 * have a file opened more than once, in which case only the last
 * release will mean, that no more reads/writes will happen on the
 * file.  The return value of release is ignored.
 *
 * Changed in version 2.2
 */
int ibc_release(const char *path, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_release: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	char fpath[PATH_MAX];
	ibc_fullpath(fpath, path);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	int err_sqlite = 0;
    int retstat = 0;
    int err_trans = -1;
    int size = 0;
    directory* dir;

    log_msg("ibc_release: begin_transaction\n");
    err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >= 0){
		log_msg("ibc_release: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
		if (dir == NULL){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_release: update_in_use_count [%s]\n", path_in_sqlite);
		err_sqlite += update_in_use_count(BB_DATA->sqlite_conn, path_in_sqlite, -1);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		if (dir->in_use_count == 1){
			err_sqlite += push_lru(BB_DATA->sqlite_conn, path_in_sqlite, 0);
			if (err_sqlite != 0){
				retstat = -EIO;
			}
		}
	}

	if (retstat >= 0){
		// We need to close the file.  Had we allocated any resources
		// (buffers etc) we'd need to free them here as well.
		log_msg("ibc_release: close. fi->fh: [%lld]\n", fi->fh);
		retstat = close(fi->fh);
		if (retstat < 0)
			retstat = ibc_error("ibc_release close");
	}

	if (retstat >= 0){
		log_msg("ibc_release: get_file_size [%s]\n", fpath);
		if (get_file_size(fpath, &size) >= 0){
			log_msg("ibc_release: update_size [%s], size: [%lld]\n", path_in_sqlite, size);
			update_size(BB_DATA->sqlite_conn, path_in_sqlite, size);
		}
	}
	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_release: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_release: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_release: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	log_msg("ibc_release: release memory\n");
	free_directory(dir);

	pthread_mutex_unlock(BB_DATA->lck);

    log_msg("ibc_release: Completed\n");
    return retstat;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int ibc_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	retstat = fsync(fi->fh);

    if (retstat < 0)
    	retstat = ibc_error("ibc_fsync fsync");

    return retstat;
}


/** Synchronize directory contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data
 *
 * Introduced in version 2.3
 */
// when exactly is this called?  when a user calls fsync and it
// happens to be a directory? ???
int bb_fsyncdir(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;
    log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("bb_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_fi(fi);

    return retstat;
}

/**
 * Initialize filesystem
 *
 * The return value will passed in the private_data field of
 * fuse_context to all file operations and as a parameter to the
 * destroy() method.
 *
 * Introduced in version 2.3
 * Changed in version 2.6
 */
// Undocumented but extraordinarily useful fact:  the fuse_context is
// set up before this function is called, and
// fuse_get_context()->private_data returns the user_data passed to
// fuse_main().  Really seems like either it should be a third
// parameter coming in here, or else the fact should be documented
// (and this might as well return void, as it did in older versions of
// FUSE).
void *ibc_init(struct fuse_conn_info *conn)
{
	is_log_to_file = 1; //enable log to file
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_init()\n");
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_conn(conn);
    log_fuse_context(fuse_get_context());

    log_msg("ibc_init: Completed\n");
    return BB_DATA;
}

/**
 * Clean up filesystem
 *
 * Called on filesystem exit.
 *
 * Introduced in version 2.3
 */
void ibc_destroy(void *userdata)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("\nibc_destroy(userdata=0x%08x)\n", userdata);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	struct bb_state *private_data = userdata;
	drbDestroyClient(private_data->client);
	drbCleanup();
	sqlite3_close_v2(private_data->sqlite_conn);
	log_msg("ibc_destroy: Completed\n", userdata);
}

/**
 * Check file access permissions
 *
 * This will be called for the access() system call.  If the
 * 'default_permissions' mount option is given, this method is not
 * called.
 *
 * This method is not called under Linux kernel versions 2.4.x
 *
 * Introduced in version 2.5
 */
int ibc_access(const char *path, int mask)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_access: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	return 0;
}

/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int ibc_opendir(const char *path, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_opendir: %s\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	int retstat = 0;
	int err_trans = -1;
	int err_sqlite = 0;

	char* parent_path = get_parent_path(path);
	char* path_in_sqlite = path;
	char* parent_path_in_sqlite = get_parent_path(path);
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".\0");
	}

	directory* dir = NULL;

	log_msg("ibc_opendir: begin_read_transaction (1)\n");
	err_trans = begin_read_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		log_msg("ibc_opendir: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
		if (dir == NULL || dir->is_delete){
			log_msg("ibc_opendir: dir is not in sqlite or is_delete is set\n");
			retstat = -ENOENT;
		} else if (dir->type != 1){
			log_msg("ibc_opendir: dir is not a folder in sqlite. type: [%d]\n", dir->type);
			retstat = -ENOTDIR;
		}
	}

	//Finish transaction
	if (err_trans == 0){
		log_msg("ibc_opendir: rollback_transaction (1)\n");
		err_trans = rollback_transaction(BB_DATA->sqlite_conn);

		if (err_trans != 0){
			log_msg("ibc_opendir: Failed to commit_transaction or rollback_transaction (1)\n");
			retstat = -EIO;
		}

		//reset err_trans
		err_trans = -1;
	}


	if (retstat >= 0){
		//Get local full path
		char fpath[PATH_MAX];
		ibc_fullpath(fpath, path);
		//create local folder
		struct stat st = {0};
		if (stat(fpath, &st) < 0) {
			log_msg("ibc_opendir: mkdir [%s]\n", fpath);
			retstat = mkdir(fpath, 0700);
		} else {
			log_msg("ibc_opendir: folder is already there. no need to mkdir [%s]\n", fpath);
		}
	}

	//Call dropbox without locking sqlite
	drbMetadata* metadata = NULL;
	if (retstat >= 0 && !(dir->is_local)) {
		log_msg("ibc_opendir: get_dbx_metadata [%s]\n", path);
		int err_dbx = get_dbx_metadata(BB_DATA->client, &metadata, path);
		if (err_dbx != DRBERR_OK){
			log_msg("ibc_opendir: Failed to drbGetMetadata. Error Code: (%d).\n", err_dbx);
			retstat = -EIO;
		}
	}

	//Check if it is a folder on dropbox
	if (retstat >= 0 && !(dir->is_local)){
		if (!(*(metadata->isDir))){
			log_msg("ibc_opendir: dir is not a folder in dropbox\n");
			retstat = -ENOTDIR;
		}
	}

	//Begin transaction now
	if (retstat >= 0 && !(dir->is_local)){
		log_msg("ibc_opendir: begin_transaction (2)\n");
		err_trans = begin_transaction(BB_DATA->sqlite_conn);
		if (err_trans != 0){
			retstat = -EBUSY;
		}
	}

	//Query the latest info within transaction in case of some changes
	if (retstat >= 0 && !(dir->is_local)){
		free_directory(dir);
		log_msg("ibc_opendir: search_directory again [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
		if (dir == NULL || dir->is_delete){
			retstat = -ENOENT;
		} else if (dir->type != 1){
			retstat = -ENOTDIR;
		}
	}

	if (retstat >= 0 && !(dir->is_local)){
		//Set is_local to 1 for parent folder
		log_msg("ibc_opendir: update_isLocal\n");
		update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 1);

		//Save metadata into sqlite for sub file/folders
		drbMetadataList* list = metadata->contents;
		int list_size = list->size;
		for (int i = 0; i < list_size; ++i){
			drbMetadata* sub_metadata = list->array[i];
			directory* sub_dir = directory_from_dbx(sub_metadata);
			log_msg("ibc_opendir: insert_directory [%s]\n", sub_dir->full_path);
			err_sqlite = insert_directory(BB_DATA->sqlite_conn, sub_dir);
			free_directory(sub_dir);

			if (err_sqlite != 0){
				retstat = -EIO;
				break;
			}
		}
	}

	log_msg("ibc_opendir: release memory\n");
	release_dbx_metadata(metadata);
	free_directory(dir);
	free(parent_path);
	free(parent_path_in_sqlite);

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_opendir: commit_transaction (2)\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_opendir: rollback_transaction (2)\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_opendir: Failed to commit_transaction or rollback_transaction (2)\n");
			retstat = -EIO;
		}
	}

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_opendir: Completed\n");
	return retstat;
}

/** Read directory
 *
 * This supersedes the old getdir() interface.  New applications
 * should use this.
 *
 * The filesystem may choose between two modes of operation:
 *
 * 1) The readdir implementation ignores the offset parameter, and
 * passes zero to the filler function's offset.  The filler
 * function will not return '1' (unless an error happens), so the
 * whole directory is read in a single readdir operation.  This
 * works just like the old getdir() method.
 *
 * 2) The readdir implementation keeps track of the offsets of the
 * directory entries.  It uses the offset parameter and always
 * passes non-zero offset to the filler function.  When the buffer
 * is full (or an error happens) the filler function will return
 * '1'.
 *
 * Introduced in version 2.3
 */
int ibc_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_readdir: [%s]\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	pthread_mutex_lock(BB_DATA->lck);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	int retstat = 0;
	int dir_cnt = 0;
	directory** sub_dirs = NULL;

	log_msg("ibc_opendir: begin_read_transaction\n");
	int err_trans = begin_read_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		sub_dirs = search_subdirectories(BB_DATA->sqlite_conn, path_in_sqlite, &dir_cnt, 0);
	}

	if (err_trans == 0){
		log_msg("ibc_opendir: rollback_transaction (2)\n");
		err_trans = rollback_transaction(BB_DATA->sqlite_conn);

		if (err_trans != 0){
			log_msg("ibc_opendir: Failed to commit_transaction or rollback_transaction (2)\n");
			retstat = -EIO;
		}
	}

	if (sub_dirs != NULL){
		for (int i = 0; i < dir_cnt; ++i){
			if (filler(buf, sub_dirs[i]->entry_name, NULL, 0) != 0) {
				retstat = -ENOMEM;
				break;
			}
		}
	}
	free_directories(sub_dirs, dir_cnt);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_readdir: Complted [%s]\n", path);
	return retstat;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int ibc_releasedir(const char *path, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_releasedir: %s\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	return 0;
}

/**
 * Change the size of an open file
 *
 * This method is called instead of the truncate() method if the
 * truncation was invoked from an ftruncate() system call.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the truncate() method will be
 * called instead.
 *
 * Introduced in version 2.5
 */
int ibc_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n", path, offset, fi);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    pthread_mutex_lock(BB_DATA->lck);

    int retstat = 0;
	int err_sqlite = 0;

	char fpath[PATH_MAX];
	ibc_fullpath(fpath, path);

	log_msg("ibc_ftruncate: begin_transaction\n");
	int err_trans = begin_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		log_msg("ibc_ftruncate: update_size\n");
		err_sqlite = update_size(BB_DATA->sqlite_conn, fpath, offset);
		if (err_sqlite != SQLITE_OK){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_ftruncate: truncate\n");
		retstat = ftruncate(fpath, offset);
		if (retstat < 0)
			retstat = ibc_error("ibc_ftruncate truncate");
	}

	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			log_msg("ibc_ftruncate: commit_transaction\n");
			err_trans = commit_transaction(BB_DATA->sqlite_conn);
		}
		else{
			log_msg("ibc_ftruncate: rollback_transaction\n");
			err_trans = rollback_transaction(BB_DATA->sqlite_conn);
		}

		if (err_trans != 0){
			log_msg("ibc_ftruncate: Failed to commit_transaction or rollback_transaction\n");
			retstat = -EIO;
		}
	}

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_ftruncate: Completed\n");
	return retstat;
}


///////////////////////////////////////////////////////////
//
// Prototypes for all these functions, and the C-style comments,
// come indirectly from /usr/include/fuse.h
//
/** Get file attributes.
 *
 * Similar to stat().  The 'st_dev' and 'st_blksize' fields are
 * ignored.  The 'st_ino' field is ignored except if the 'use_ino'
 * mount option is given.
 */
int ibc_getattr(const char *path, struct stat *statbuf)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
    log_msg("ibc_getattr: %s\n", path);
    log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

    pthread_mutex_lock(BB_DATA->lck);

#ifdef VMWARE
    //Skip some strange files which do not exit
    if (strlen("/.Trash") == strlen(path) && !strncmp("/.Trash", path, strlen("/.Trash")))
    {
    	log_msg("The dummy file .Trash is skipped.\n");
    	return 0;
    }
    if (strlen("/.Trash-1000") == strlen(path) && !strncmp("/.Trash-1000", path, strlen("/.Trash-1000")))
	{
		log_msg("The dummy file .Trash-1000 is skipped.\n");
		return 0;
	}
    if (strlen("/autorun.inf") == strlen(path)      && !strncmp("/autorun.inf", path, strlen("/autorun.inf")))
    {
		log_msg("The dummy file autorun.inf is skipped.\n");
		return 0;
	}
    if (strlen("/.xdg-volume-info") == strlen(path) && !strncmp("/.xdg-volume-info", path, strlen("/.xdg-volume-info")))
    {
		log_msg("The dummy file .xdg-volume-info is skipped.\n");
		return 0;
	}
#endif

    int retstat = 0;
    directory* dir = NULL;

    //clear memory
	memset(statbuf, 0, sizeof(struct stat));

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	log_msg("ibc_opendir: begin_read_transaction\n");
	int err_trans = begin_read_transaction(BB_DATA->sqlite_conn);
	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0){
		log_msg("ibc_getattr: search_directory [%s]\n", path_in_sqlite);
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
	}

	if (dir == NULL || dir->is_delete){
		retstat = -ENOENT;
	}

	//Finish transaction
	if (err_trans == 0){
		log_msg("ibc_ftruncate: rollback_transaction\n");
		err_trans = rollback_transaction(BB_DATA->sqlite_conn);

		if (err_trans != 0){
			log_msg("ibc_ftruncate: Failed to rollback_transaction\n");
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		log_msg("ibc_getattr: fill in statbuf\n");
		statbuf->st_size = dir->size;
		statbuf->st_uid = getuid();
		statbuf->st_gid = getgid();
		statbuf->st_atime = dir->atime;// = "access time";
		statbuf->st_ctime = dir->mtime; //= "change time";
		statbuf->st_mtime = dir->mtime;// = "modify time";

		if (dir->type == 1)
		{
			statbuf->st_mode = S_IFDIR | 0755;
			statbuf->st_nlink = 2;
		}
		else
		{
			statbuf->st_mode = S_IFREG | 0755;
			statbuf->st_nlink = 1;
		}
		//log_stat(statbuf);
	}

	free_directory(dir);

	pthread_mutex_unlock(BB_DATA->lck);

	log_msg("ibc_getattr: Completed.\n");

	return retstat;
}


/**
 * Get attributes from an open file
 *
 * This method is called instead of the getattr() method if the
 * file information is available.
 *
 * Currently this is only called after the create() method if that
 * is implemented (see above).  Later it may be called for
 * invocations of fstat() too.
 *
 * Introduced in version 2.5
 */
int ibc_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
	log_msg("\n>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");
	log_msg("ibc_fgetattr: %s\n", path);
	log_msg(">>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>>\n");

	int ret = ibc_getattr(path, statbuf);
	log_msg("ibc_fgetattr: Completed\n");
	return ret;
}

struct fuse_operations bb_oper = {
	  .getattr = ibc_getattr,
	  .mknod = ibc_mknod,
	  .mkdir = ibc_mkdir,
	  .rename = ibc_rename,
	  .unlink = ibc_unlink,
	  .rmdir = ibc_rmdir,
	  .truncate = ibc_truncate,
	  .utime = ibc_utime,
	  .open = ibc_open,
	  .read = ibc_read,
	  .write = ibc_write,
	  .statfs = ibc_statfs, /** Just a placeholder*/
	  .flush = ibc_flush,
	  .release = ibc_release,
	  .fsync = ibc_fsync,
	  .opendir = ibc_opendir,
	  .readdir = ibc_readdir,
	  .releasedir = ibc_releasedir,
	  .init = ibc_init,
	  .destroy = ibc_destroy,
	  .access = ibc_access,
	  .ftruncate = ibc_ftruncate,
	  .fgetattr = ibc_fgetattr
};

void ibc_usage()
{
    fprintf(stderr, "usage:  IntelligentBoxClient [FUSE and mount options] rootDir mountPoint\n");
    abort();
}

int busy_handler(void* arg, int times){
	//fprintf(stderr, "I am here. %d\n", times);
	if (times == 100){
		return 0;
	}
	else {
		delay(30);
		return 1;
	}
}

int main(int argc, char *argv[])
{
    int fuse_stat;
    struct bb_state *bb_data;
    // bbfs doesn't do any access checking on its own (the comment
    // blocks in fuse.h mention some of the functions that need
    // accesses checked -- but note there are other functions, like
    // chown(), that also need checking!).  Since running bbfs as root
    // will therefore open Metrodome-sized holes in the system
    // security, we'll check if root is trying to mount the filesystem
    // and refuse if it is.  The somewhat smaller hole of an ordinary
    // user doing it with the allow_other flag is still there because
    // I don't want to parse the options string.
    if ((getuid() == 0) || (geteuid() == 0)) {
    	fprintf(stderr, "Running BBFS as root opens unnacceptable security holes\n");
    	return 1;
    }

    char *c_key    = "d9m9s1iylifpqsx";  //< consumer key
	char *c_secret = "x2pfq4vkf5bytnq";  //< consumer secret

	// User key and secret. Leave them NULL or set them with your AccessToken.
	//char *t_key    = "8pfo7r8fjml1xo6i"; // iihdh3t3dcld9svd < access token key
	//char *t_secret = "m4glqxs42dcop4i";  // 0fw3qvfrqo1dlxx < access token secret
	char *t_key    = "iihdh3t3dcld9svd"; //< access token key
	char *t_secret = "0fw3qvfrqo1dlxx";  //< access token secret

    // Perform some sanity checking on the command line:  make sure
    // there are enough arguments, and that neither of the last two
    // start with a hyphen (this will break if you actually have a
    // rootpoint or mountpoint whose name starts with a hyphen, but so
    // will a zillion other programs)
    if ((argc < 3) || (argv[argc-2][0] == '-') || (argv[argc-1][0] == '-'))
    	ibc_usage();

    bb_data = malloc(sizeof(struct bb_state));
    if (bb_data == NULL) {
    	perror("main calloc");
    	abort();
    }

    // Pull the rootdir out of the argument list and save it in my
    // internal data

    bb_data->rootdir = realpath(argv[argc-2], NULL);

    char datadir[PATH_MAX];
    char metadatadir[PATH_MAX];
    strncpy(datadir, bb_data->rootdir, strlen(bb_data->rootdir));
    strncpy(datadir + strlen(bb_data->rootdir), "/data", 5);
    datadir[strlen(bb_data->rootdir) + 5] = '\0';
    strncpy(metadatadir, bb_data->rootdir, strlen(bb_data->rootdir));
    strncpy(metadatadir + strlen(bb_data->rootdir), "/metadata", 9);
    metadatadir[strlen(bb_data->rootdir) + 9] = '\0';

    bb_data->datadir = datadir;
    bb_data->metadatadir = metadatadir;

    //create local folder
    int ret = 0;
	struct stat st = {0};
	if (stat(bb_data->datadir, &st) == -1) {
		ret = mkdir(bb_data->datadir, 0700);
		if (ret < 0){
			ibc_error("Create data folder");
			return -1;
		}
	}

	if (stat(bb_data->metadatadir, &st) == -1) {
		ret = mkdir(bb_data->metadatadir, 0700);
		if (ret < 0){
			ibc_error("Create metadata folder");
			return -1;
		}
	}

    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;

    pthread_mutex_t lock;

	//Init mutex
	if (pthread_mutex_init(&lock, NULL) != 0)
	{
		fprintf(stderr, "mutex init failed\n");
		return -1;
	}

	bb_data->lck = &lock;
    bb_data->logfile = log_open("fs_logs.log");

    // Global initialisation
	drbInit();
	// Create a Dropbox client
	drbClient* cli = drbCreateClient(c_key, c_secret, t_key, t_secret);
	// Set default arguments to not repeat them on each API call
	drbSetDefault(cli, DRBOPT_ROOT, DRBVAL_ROOT_AUTO, DRBOPT_END);
    bb_data->client = cli;

    char dbfile_path[PATH_MAX];
    strncpy(dbfile_path, bb_data->metadatadir, strlen(bb_data->metadatadir));
    strncpy(dbfile_path + strlen(bb_data->metadatadir), "/dir.db", 7);
    dbfile_path[strlen(bb_data->metadatadir) + 7] = '\0';
    sqlite3 *sqlite_conn = init_db(dbfile_path);
    sqlite3_busy_handler(sqlite_conn, busy_handler, NULL); //busy timeout 3 seconds
    bb_data->sqlite_conn = sqlite_conn;

    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &bb_oper, bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

    pthread_mutex_destroy(&lock);

    return fuse_stat;
}

