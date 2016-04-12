

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
static int bb_error(char *str)
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
static void bb_fullpath(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->datadir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will break here

    //log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	//BB_DATA->rootdir, path, fpath);
}

static void bb_metadata_path(char fpath[PATH_MAX], const char *path)
{
    strcpy(fpath, BB_DATA->metadatadir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will break here

    //log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	//BB_DATA->rootdir, path, fpath);
}


/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int bb_mknod(const char *path, mode_t mode, dev_t dev)
{
	log_msg("\nbb_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n", path, mode, dev);

	//local full path
	char fpath[PATH_MAX];
	bb_fullpath(fpath, path);

	//If it is pipe, just create a pipe
	if (S_ISFIFO(mode)) {
		log_msg("bb_mknod: mkfifo is called\n");
		retstat = mkfifo(fpath, mode);
		if (retstat < 0)
			retstat = bb_error("bb_mknod mkfifo");
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

	int err_sqlite = 0;
	int retstat = 0;
	int fd;
	directory* dir;

	//Search to see if the directory is already in the database
	int err_trans = begin_transaction(BB_DATA->sqlite_conn);

	if (err_trans != 0){
		retstat = -EBUSY;
	}

	if (retstat >= 0)
	{
		log_msg("bb_mknod: Begin transaction function has been called!\n");
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);

		if (dir != NULL) {
			if (dir->is_delete)
				retstat = -EAGAIN;
			else
				retstat = -EEXIST;
		}
	}

	if (retstat >= 0){
		log_msg("bb_mknod: Creating a file\n");

		long now = get_current_epoch_time();
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

		// On Linux this could just be 'mknod(path, mode, rdev)' but this
		//  is more portable
		if (S_ISREG(mode)) {
			log_msg("bb_mknod: open is called\n");
			retstat = open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode);
			if (retstat < 0)
				retstat = bb_error("bb_mknod open");
			else {
				retstat = close(retstat);
				if (retstat < 0)
					retstat = bb_error("bb_mknod close");
			}
		} else {
			log_msg("bb_mknod: mknod is called\n");
			retstat = mknod(fpath, mode, dev);
			if (retstat < 0)
				retstat = bb_error("bb_mknod mknod");
		}

		if (retstat >= 0){
			err_sqlite = insert_directory(BB_DATA->sqlite_conn, dir);
			if (err_sqlite != 0){
				retstat = -EIO;
			}
		}
	}

	//Complete transaction
	if (err_trans == 0){
		if (retstat >= 0){
			commit_transaction(BB_DATA->sqlite_conn);
		} else {
			rollback_transaction(BB_DATA->sqlite_conn);
		}
	}

	free_directory(dir);
	free(parent_path_in_sqlite);
	free(file_name);
	log_msg("bb_open: Completed\n");

	return retstat;
}

/** Create a directory */
int ibc_mkdir(const char *path, mode_t mode)
{
	log_msg("\nibc_mkdir(path=\"%s\", mode=0%3o)\n",
		    path, mode);
    int retstat = 0;
    int err_sqlite = 0;

    char* path_in_sqlite = path;
    char* parent_path_in_sqlite = get_parent_path(path);
    char* file_name = get_file_name(path);
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".");
	}
    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);

    long time = get_current_epoch_time();

	//Begin transaction
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    //In case previous deleted metadata has not been synchronized
    if (retstat >= 0){
    	directory* dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (dir != NULL){
    		retstat = -EAGAIN;
    	}
    	free_directory(dir);
	}

    if (retstat >= 0){
		retstat = mkdir(fpath, mode);
		if (retstat < 0){
			retstat = bb_error("ibc_mkdir mkdir");
		}
    }

    if (retstat >= 0){
    	directory* dir = new_directory(
    				path_in_sqlite,
    				parent_path_in_sqlite,
    				file_name,
    				"",
    				1,
    				0,
    				0,
    				0,
    				0,
    				1,
    				1,
    				0,
    				0,
    				""
    				);
    	err_sqlite += insert_directory(BB_DATA->sqlite_conn, dir);
    	err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, time);
    	err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, time);

    	if (err_sqlite != 0){
    		retstat = -EIO;
    	}
    	log_msg("\nInsert Directory Info Into Database Table After mkdir!\n");
    	free_directory(dir);
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			commit_transaction(BB_DATA->sqlite_conn);
			log_msg("\nUpdate Database Table After mkdir!\n");
		}
		else{
			rollback_transaction(BB_DATA->sqlite_conn);
			log_msg("\nRollback Database Table After mkdir!\n");
		}
	}

    return retstat;
}

/** Remove a file */
int ibc_unlink(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    char* path_in_sqlite = path;
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}
    int err_dbx = DRBERR_OK;
    int err_sqlite = 0;
    directory* dir;
    long time = get_current_epoch_time();

    //Begin transaction
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >= 0){
    	dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (dir == NULL || dir->is_delete){
    		retstat = -ENOENT;
    	}
    }

    if (retstat >= 0){
		if(dir->is_local == 0){
			err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, time);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, time);

			if (err_sqlite != 0){
				retstat = -EIO;
			}
		}else{
			log_msg("\nibc_unlink(path=\"%s\")\n",path);
			bb_fullpath(fpath, path);
			retstat = unlink(fpath);
			if (retstat < 0){
				retstat = bb_error("ibc_unlink unlink");
			}

			if (retstat >= 0){
				err_sqlite += update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 0);
				err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, time);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, time);

				if (err_sqlite != 0){
					retstat = -EIO;
				}
			}

		}
    }

    //Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			commit_transaction(BB_DATA->sqlite_conn);
			log_msg("\nUpdate Database Table After rm File!\n");
		}
		else{
			rollback_transaction(BB_DATA->sqlite_conn);
			log_msg("\nRollback Database Table After rm File!\n");
		}
	}
    return retstat;
}

/** Remove a directory */
int ibc_rmdir(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char* path_in_sqlite = path;
    if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

    int err_sqlite = 0;

    directory* dir;
    long time = get_current_epoch_time();

    log_msg("\nibc_rmdir(path=\"%s\")\n", path);
    bb_fullpath(fpath, path);

    int err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >=0){
        dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
        if (dir == NULL || dir->is_delete){
        	retstat = -ENOENT;
        }
    }

    if (retstat >= 0){
		if(dir->is_local == 0){
			err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, time);
			err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, time);
			if (err_sqlite != 0){
				retstat = -EIO;
			}
		}else{
			int subdir_cnt = 0;
			directory** subdirs = search_subdirectories(BB_DATA->sqlite_conn, path_in_sqlite, &subdir_cnt);
			if (subdirs != NULL){
				retstat = -ENOTEMPTY;
			}
			free_directories(subdirs, subdir_cnt);

			if (retstat >= 0){
				retstat = rmdir(fpath);
				if (retstat < 0)
					retstat = bb_error("ibc_rmdir rmdir");
			}

			if (retstat >= 0){
				//Update the database table to set is_deleted to 1
				err_sqlite += update_isDeleted(BB_DATA->sqlite_conn, path_in_sqlite);
				err_sqlite += update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 0);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 1, time);
				err_sqlite += update_time(BB_DATA->sqlite_conn, path_in_sqlite, 0, time);

				if (err_sqlite != 0){
					retstat = -EIO;
				}
			}
		}
    }
	//Finish transaction
	if (err_trans == 0){
		if (retstat >= 0){
			commit_transaction(BB_DATA->sqlite_conn);
		    log_msg("\nUpdate Database Table After rmdir!\n");
		}
		else{
			rollback_transaction(BB_DATA->sqlite_conn);
		    log_msg("\nRollback Database Table After rmdir!\n");
		}
	}

	free_directory(dir);
    return retstat;
}


/** Change the size of a file */
int bb_truncate(const char *path, off_t newsize)
{
	log_msg("\nbb_truncate: [%s]\n", path);
    int retstat = 0;
    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);

    log_msg("bb_truncate: truncate\n");
    retstat = truncate(fpath, newsize);
    if (retstat < 0)
    	retstat = bb_error("bb_truncate truncate");

    //TODO: update size

    log_msg("bb_truncate: Completed\n");
    return retstat;
}

/** Change the access and/or modification times of a file */
/* note -- I'll want to change this as soon as 2.6 is in debian testing */
int bb_utime(const char *path, struct utimbuf *ubuf)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_utime(path=\"%s\", ubuf=0x%08x)\n",
	    path, ubuf);
    bb_fullpath(fpath, path);

    retstat = utime(fpath, ubuf);
    if (retstat < 0)
    	retstat = bb_error("bb_utime utime");

    //TODO update mtime, atime
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
int bb_open(const char *path, struct fuse_file_info *fi)
{
	log_msg("\nbb_open: [%s]\n", path);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

    int retstat = 0;
    int fd;
    directory* dir;

    //local full path
    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);

    //Search to see if the directory is already in the database
    int err_trans = begin_transaction(BB_DATA->sqlite_conn);

    if (err_trans != 0){
    	retstat = -EBUSY;
	}

    if (retstat >= 0)
    {
		log_msg("bb_open: Begin transaction function has been called!\n");
		dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);

		if (dir == NULL || dir->is_delete) {
			retstat = -ENOENT;
		}
    }

	if (retstat >= 0) {
		log_msg("bb_open: A Record Has Been Found In The Database!\n");

		// If the file is not on local, Download it from Dropbox
		if(dir->is_local == 0){
			log_msg("bb_open: Downloading file.\n");

			drbMetadata* metadata;
			int err_dbx = download_dbx_file(BB_DATA->client, &metadata, path, fpath);

			if (err_dbx != 0){
				retstat = -EIO;
			}
		}
	}

	if (retstat >= 0){
		log_msg("bb_open: update_isLocal\n");
		int err_sqlite = update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 1);
		if (err_sqlite != 0){
			retstat = -EIO;
		}
	}

	if (retstat >= 0){
		fd = open(fpath, fi->flags);
		if (fd < 0)
			retstat = bb_error("bb_open\n");
		else{
			log_msg("bb_open: file is opened.\n");
			fi->fh = fd;
		}
	}

	//TODO update atime, is_locked, in_use_count

	//Complete transaction
	if (err_trans == 0){
		if (retstat >= 0){
			commit_transaction(BB_DATA->sqlite_conn);
		} else {
			rollback_transaction(BB_DATA->sqlite_conn);
		}
	}

	free_directory(dir);

	log_msg("bb_open: Completed\n");

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
int bb_read(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
	log_msg("\nbb_read: [%s]\n", path);
    int retstat = 0;

    char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0)
    	retstat = bb_error("bb_read read");

    //TODO update atime
    //update_atime(path_in_sqlite);


    log_msg("bb_read: Completed\n");
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
int bb_write(const char *path, const char *buf, size_t size, off_t offset,
	     struct fuse_file_info *fi)
{
	log_msg("\nbb_write: [%s]\n", path);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

    int retstat = 0;
    int err_trans = 0;

    log_msg("bb_write: begin_transaction\n");
    err_trans = begin_transaction(BB_DATA->sqlite_conn);
    if (err_trans != 0){
    	retstat = -EBUSY;
    }

    if (retstat >= 0){
    	log_msg("bb_write: pwrite\n");
    	retstat = pwrite(fi->fh, buf, size, offset);
    	if (retstat < 0)
    		bb_error("bb_write pwrite");
    }

    if (retstat >= 0){
    	log_msg("bb_write: update_isModified\n");
    	int err_sqlite = update_isModified(BB_DATA->sqlite_conn, path_in_sqlite);
    	if (err_sqlite != 0){
    		retstat = -EIO;
    	}
    }

    //TODO update mtime, atime

    if (err_trans == 0){
    	if (retstat >= 0){
    		log_msg("bb_write: commit_transaction\n");
    		commit_transaction(BB_DATA->sqlite_conn);
    	} else {
    		log_msg("bb_write: rollback_transaction\n");
    		rollback_transaction(BB_DATA->sqlite_conn);
    	}
    }

    log_msg("bb_write: Completed\n");
    return retstat;
}

/** Get file system statistics
 *
 * The 'f_frsize', 'f_favail', 'f_fsid' and 'f_flag' fields are ignored
 *
 * Replaced 'struct statfs' parameter with 'struct statvfs' in
 * version 2.5
 */
int bb_statfs(const char *path, struct statvfs *statv)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_statfs(path=\"%s\", statv=0x%08x)\n",
	    path, statv);
    bb_fullpath(fpath, path);

    // get stats for underlying filesystem
    retstat = statvfs(fpath, statv);
    if (retstat < 0)
	retstat = bb_error("bb_statfs statvfs");

    log_statvfs(statv);

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
int bb_flush(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_flush(path=\"%s\", fi=0x%08x)\n", path, fi);

    retstat = fsync(fi->fh);
	if (retstat < 0)
		retstat = bb_error("bb_flush fsync");

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
int bb_release(const char *path, struct fuse_file_info *fi)
{
	log_msg("\nbb_release: [%s]\n", path);
    int retstat = 0;

    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    retstat = close(fi->fh);
    if (retstat < 0)
       	retstat = bb_error("bb_release close");

    //TODO update in_use_count
    log_msg("bb_release: Completed\n");
    return retstat;
}

/** Synchronize file contents
 *
 * If the datasync parameter is non-zero, then only the user data
 * should be flushed, not the meta data.
 *
 * Changed in version 2.2
 */
int bb_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_fsync(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);

	retstat = fsync(fi->fh);

    if (retstat < 0)
    	retstat = bb_error("bb_fsync fsync");

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

    log_msg("\nbb_fsyncdir(path=\"%s\", datasync=%d, fi=0x%08x)\n",
	    path, datasync, fi);
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

    log_msg("\nibc_init()\n");

    log_conn(conn);
    log_fuse_context(fuse_get_context());

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
	struct bb_state *private_data = userdata;
	drbDestroyClient(private_data->client);
	drbCleanup();
	sqlite3_close_v2(private_data->sqlite_conn);
    log_msg("\nibc_destroy(userdata=0x%08x)\n", userdata);
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
	log_msg("\nibc_access: %s\n", path);
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
	log_msg("\nibc_opendir: %s\n", path);

	char* parent_path = get_parent_path(path);
	char* path_in_sqlite = path;
	char* parent_path_in_sqlite = get_parent_path(path);
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
		parent_path_in_sqlite = copy_text(".\0");
	}

	directory* dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);
	if (dir == NULL || dir->is_delete){
		free(parent_path);
		free(parent_path_in_sqlite);
		return -ENOENT;
	}

	if (dir->type != 1){
		free(parent_path);
		free(parent_path_in_sqlite);
		return -ENOTDIR;
	}

	//Get local full path
	char fpath[PATH_MAX];
	bb_fullpath(fpath, path);

	int ret = 0;
	int err_dbx = DRBERR_OK;
	if (!(dir->is_local)) {

		//create local folder
		struct stat st = {0};
		if (stat(fpath, &st) == -1) {
			ret = mkdir(fpath, 0700);
		}

		if (ret == 0){
			drbMetadata* metadata = NULL;
			err_dbx = get_dbx_metadata(BB_DATA->client, &metadata, path);

			int err_sqlite = 0;
			if (err_dbx == DRBERR_OK) {
				if (*(metadata->isDir)){
					//Begin transaction
					int err_trans = begin_transaction(BB_DATA->sqlite_conn);
					if (err_trans != 0){
						ret = -EBUSY;
					} else {
						//Set is_local to 1 for parent folder
						update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite, 1);

						//Save metadata into sqlite for sub file/folders
						drbMetadataList* list = metadata->contents;
						int list_size = list->size;
						for (int i = 0; i < list_size; ++i){
							drbMetadata* sub_metadata = list->array[i];
							directory* sub_dir = directory_from_dbx(sub_metadata);
							err_sqlite = insert_directory(BB_DATA->sqlite_conn, sub_dir);
							free_directory(sub_dir);

							if (err_sqlite != 0){
								ret = -EIO;
								break;
							}
						}

						//Finish transaction
						if (err_sqlite == 0){
							commit_transaction(BB_DATA->sqlite_conn);
						}
						else{
							rollback_transaction(BB_DATA->sqlite_conn);
						}
					}


				} else {
					ret = -ENOTDIR;
				}
				release_dbx_metadata(metadata);
			} else {
				log_msg("ibc_opendir - Failed to drbGetMetadata. Error Code: (%d).\n", err_dbx);
				ret = -EIO;
			}
		}
	}

	//May not need to open the local folder since everything is logical
	/*
	if (ret == 0){
		DIR *dp;
		dp = opendir(fpath);
		if (dp == NULL){
			log_msg("bb_opendir - Failed to opendir(%s).\n", fpath);
			ret = ENOENT;
		}
		fi->fh = (intptr_t) dp;
	}*/

	free_directory(dir);
	free(parent_path);
	free(parent_path_in_sqlite);

	log_msg("ibc_opendir: Completed [%s]\n", path);
	return ret;
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
	log_msg("\nibc_readdir: %s\n", path);

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	int ret = 0;
	int dir_cnt = 0;
	directory** sub_dirs = search_subdirectories(BB_DATA->sqlite_conn, path_in_sqlite, &dir_cnt);
	if (sub_dirs != NULL){
		for (int i = 0; i < dir_cnt; ++i){
			if (sub_dirs[i]->is_delete){
				continue;
			}
			if (filler(buf, sub_dirs[i]->entry_name, NULL, 0) != 0) {
				ret = -ENOMEM;
				break;
			}
		}
	}
	free_directories(sub_dirs, dir_cnt);
	log_msg("ibc_readdir: Complted [%s]\n", path);
	return ret;
}

/** Release directory
 *
 * Introduced in version 2.3
 */
int ibc_releasedir(const char *path, struct fuse_file_info *fi)
{
	log_msg("\nibc_releasedir: %s\n", path);
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
int bb_ftruncate(const char *path, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;

    log_msg("\nbb_ftruncate(path=\"%s\", offset=%lld, fi=0x%08x)\n",
	    path, offset, fi);
    log_fi(fi);

    retstat = ftruncate(fi->fh, offset);
    if (retstat < 0)
    	retstat = bb_error("bb_ftruncate ftruncate");

    //TODO: update size

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
    log_msg("\nibc_getattr: %s\n", path);

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

    int ret = 0;
    //clear memory
	memset(statbuf, 0, sizeof(struct stat));

	char* path_in_sqlite = path;
	if (strlen(path) == 1){
		path_in_sqlite = "\0";
	}

	log_msg("ibc_getattr: search_directory [%s]\n", path_in_sqlite);
	directory* dir = search_directory(BB_DATA->sqlite_conn, path_in_sqlite);

	if (dir == NULL || dir->is_delete){
		ret = -ENOENT;
	} else {
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
		log_stat(statbuf);
	}

	free_directory(dir);

	log_msg("ibc_getattr: Completed.\n");

	return ret;
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
	log_msg("\nibc_fgetattr: %s\n", path);
	int ret = ibc_getattr(path, statbuf);
	log_msg("ibc_fgetattr: Completed\n");
	return ret;
}

struct fuse_operations bb_oper = {
	  .getattr = ibc_getattr,
	  .mknod = bb_mknod,
	  .mkdir = ibc_mkdir,
	  .unlink = ibc_unlink,
	  .rmdir = ibc_rmdir,
	  .truncate = bb_truncate,
	  .utime = bb_utime,
	  .open = bb_open,
	  .read = bb_read,
	  .write = bb_write,
	  .statfs = bb_statfs, /** Just a placeholder*/
	  .flush = bb_flush,
	  .release = bb_release,
	  .fsync = bb_fsync,
	  .opendir = ibc_opendir,
	  .readdir = ibc_readdir,
	  .releasedir = ibc_releasedir,
	  .init = ibc_init,
	  .destroy = ibc_destroy,
	  .access = ibc_access,
	  .ftruncate = bb_ftruncate,
	  .fgetattr = ibc_fgetattr
};

void bb_usage()
{
    fprintf(stderr, "usage:  bbfs [FUSE and mount options] rootDir mountPoint\n");
    abort();
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
	bb_usage();

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
			bb_error("Create data folder");
			return -1;
		}
	}

	if (stat(bb_data->metadatadir, &st) == -1) {
		ret = mkdir(bb_data->metadatadir, 0700);
		if (ret < 0){
			bb_error("Create metadata folder");
			return -1;
		}
	}

    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;

    bb_data->logfile = log_open("bbfs.log");

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
    bb_data->sqlite_conn = sqlite_conn;

    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &bb_oper, bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

    return fuse_stat;
}

