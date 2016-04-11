

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
//#define HAVE_SYS_XATTR_H

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include <dropbox_log_utils.h>
#include <common_utils.h>


#include <sqlite3.h>


int is_log_to_file = 0;

static int callback(void *NotUsed, int argc, char **argv, char **azColName);
void test_sqlite_insert(sqlite3* db);

// Report errors to logfile and give -errno to caller
static int bb_error(char *str)
{
    int ret = -errno;

    log_msg("    ERROR %s: %s\n", str, strerror(errno));

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
    strcpy(fpath, BB_DATA->rootdir);
    strncat(fpath, path, PATH_MAX); // ridiculously long paths will
				    // break here

    log_msg("    bb_fullpath:  rootdir = \"%s\", path = \"%s\", fpath = \"%s\"\n",
	    BB_DATA->rootdir, path, fpath);
}

void update_atime(const char *path){

	int rc;
	char *zErrMsg = 0;
	char fpath[PATH_MAX];
	bb_fullpath(fpath, path);


	sqlite3* db1 = BB_DATA->sqlite_conn;

	time_t now;
	struct tm ts;
	char buf[80];
	time(&now);
	ts = *localtime(&time);
	strftime(buf, sizeof(buf), "%a %Y-%m-%d %H:%M:%S %Z", &ts);
	char* sql4 = "UPDATE Directory SET atime =";
	char* sql5 = "WHERE full_path =";
	char* sql_update_atime = "";
//			(char*)malloc(5+strlen(sql4)+strlen(buf)+strlen(sql5)+strlen(fpath));
	strncpy(sql_update_atime, sql4, strlen(sql4)+1);
	strncpy(sql_update_atime, buf, strlen(buf)+1);
	strncpy(sql_update_atime, sql5, strlen(sql5)+1);
	strncpy(sql_update_atime, fpath, strlen(fpath)+1);

   //Update on atime in Directory table
	rc = sqlite3_exec(db1, sql_update_atime, 0, 0, &zErrMsg);

   // If there's an error updating the database table, print it out.
	if( rc!=SQLITE_OK ){
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
//	return (0);
}

void update_mtime(const char *path){
	int rc=0;
	char* zErrMsg = 0;
	char fpath[PATH_MAX];
	bb_fullpath(fpath, path);


	sqlite3* db1 = BB_DATA->sqlite_conn;

	time_t now;
		struct tm ts;
		char buf[80];
		time(&now);
		ts = *localtime(&time);
		strftime(buf, sizeof(buf), "%a %Y-%m-%d %H:%M:%S %Z", &ts);
		char* sql4 = "UPDATE Directory SET mtime =";
		char* sql5 = "WHERE full_path =";
		char* sql_update_mtime = "";
				//(char*)malloc(5+strlen(sql4)+strlen(buf)+strlen(sql5)+strlen(fpath));
		strncpy(sql_update_mtime, sql4, strlen(sql4)+1);
		strncpy(sql_update_mtime, buf, strlen(buf)+1);
		strncpy(sql_update_mtime, sql5, strlen(sql5)+1);
		strncpy(sql_update_mtime, fpath, strlen(fpath)+1);
		//Update on atime in Directory table
		rc = sqlite3_exec(db1, sql_update_mtime, 0, 0, &zErrMsg);

		// If there's an error updating the database table, print it out.
		if( rc!=SQLITE_OK ){
				fprintf(stderr, "SQL error: %s\n", zErrMsg);
				sqlite3_free(zErrMsg);
			}
//		return (0);

}

/** Read the target of a symbolic link
 *
 * The buffer should be filled with a null terminated string.  The
 * buffer size argument includes the space for the terminating
 * null character.  If the linkname is too long to fit in the
 * buffer, it should be truncated.  The return value should be 0
 * for success.
 */
// Note the system readlink() will truncate and lose the terminating
// null.  So, the size passed to to the system readlink() must be one
// less than the size passed to bb_readlink()
// bb_readlink() code by Bernardo F Costa (thanks!)
int bb_readlink(const char *path, char *link, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_readlink(path=\"%s\", link=\"%s\", size=%d)\n",
	  path, link, size);
    bb_fullpath(fpath, path);

    retstat = readlink(fpath, link, size - 1);
    if (retstat < 0)
	retstat = bb_error("bb_readlink readlink");
    else  {
	link[retstat] = '\0';
	retstat = 0;
    }

    return retstat;
}

/** Create a file node
 *
 * There is no create() operation, mknod() will be called for
 * creation of all non-directory, non-symlink nodes.
 */
// shouldn't that comment be "if" there is no.... ?
int bb_mknod(const char *path, mode_t mode, dev_t dev)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_mknod(path=\"%s\", mode=0%3o, dev=%lld)\n",
	  path, mode, dev);
    bb_fullpath(fpath, path);

    // On Linux this could just be 'mknod(path, mode, rdev)' but this
    //  is more portable
    if (S_ISREG(mode)) {
        retstat = open(fpath, O_CREAT | O_EXCL | O_WRONLY, mode);
	if (retstat < 0)
	    retstat = bb_error("bb_mknod open");
        else {
            retstat = close(retstat);
	    if (retstat < 0)
		retstat = bb_error("bb_mknod close");
	}
    } else
	if (S_ISFIFO(mode)) {
	    retstat = mkfifo(fpath, mode);
	    if (retstat < 0)
		retstat = bb_error("bb_mknod mkfifo");
	} else {
	    retstat = mknod(fpath, mode, dev);
	    if (retstat < 0)
		retstat = bb_error("bb_mknod mknod");
	}

    return retstat;
}

/** Create a directory */
int bb_mkdir(const char *path, mode_t mode)
{
	int rc;
	char *zErrMsg = 0;
    int retstat = 0;
    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);
    char* parent_path = get_parent_path(fpath);
    char* file_name = get_file_name(fpath);

//    drbClient* cli = BB_DATA->client;
    sqlite3* db1 = BB_DATA->sqlite_conn;

    log_msg("\nbb_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);
    bb_fullpath(fpath, path);
    retstat = mkdir(fpath, mode);
    if (retstat < 0){
    	retstat = bb_error("bb_mkdir mkdir");
    }else{
    	directory* dir = new_directory(
    				fpath,
    				parent_path,
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
    	insert_directory(db1, dir);
    	log_msg("\nInsert Directory Info Into Database Table After mkdir!\n");
    	free_directory(dir);
    }
    return retstat;
}

/** Remove a file */
int bb_unlink(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_unlink(path=\"%s\")\n",
	    path);
    bb_fullpath(fpath, path);

    retstat = unlink(fpath);
    if (retstat < 0)
	retstat = bb_error("bb_unlink unlink");

    return retstat;
}

/** Remove a directory */
int bb_rmdir(const char *path)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_rmdir(path=\"%s\")\n",
	    path);
    bb_fullpath(fpath, path);

    retstat = rmdir(fpath);
    if (retstat < 0)
	retstat = bb_error("bb_rmdir rmdir");

   //Update the database table to set is_deleted to 1
    update_isDeleted(fpath);
    log_msg("\nUpdate Database Table After rmdir!\n");
    return retstat;
}

/** Create a symbolic link */
// The parameters here are a little bit confusing, but do correspond
// to the symlink() system call.  The 'path' is where the link points,
// while the 'link' is the link itself.  So we need to leave the path
// unaltered, but insert the link into the mounted directory.
int bb_symlink(const char *path, const char *link)
{
    int retstat = 0;
    char flink[PATH_MAX];

    log_msg("\nbb_symlink(path=\"%s\", link=\"%s\")\n",
	    path, link);
    bb_fullpath(flink, link);

    retstat = symlink(path, flink);
    if (retstat < 0)
	retstat = bb_error("bb_symlink symlink");

    return retstat;
}

/** Rename a file */
// both path and newpath are fs-relative
int bb_rename(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char fnewpath[PATH_MAX];

    log_msg("\nbb_rename(fpath=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    bb_fullpath(fpath, path);
    bb_fullpath(fnewpath, newpath);

    retstat = rename(fpath, fnewpath);
    if (retstat < 0)
	retstat = bb_error("bb_rename rename");

    return retstat;
}

/** Create a hard link to a file */
int bb_link(const char *path, const char *newpath)
{
    int retstat = 0;
    char fpath[PATH_MAX], fnewpath[PATH_MAX];

    log_msg("\nbb_link(path=\"%s\", newpath=\"%s\")\n",
	    path, newpath);
    bb_fullpath(fpath, path);
    bb_fullpath(fnewpath, newpath);

    retstat = link(fpath, fnewpath);
    if (retstat < 0)
	retstat = bb_error("bb_link link");

    return retstat;
}

/** Change the permission bits of a file */
int bb_chmod(const char *path, mode_t mode)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_chmod(fpath=\"%s\", mode=0%03o)\n",
	    path, mode);
    bb_fullpath(fpath, path);

    retstat = chmod(fpath, mode);
    if (retstat < 0)
	retstat = bb_error("bb_chmod chmod");

    return retstat;
}

/** Change the owner and group of a file */
int bb_chown(const char *path, uid_t uid, gid_t gid)

{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_chown(path=\"%s\", uid=%d, gid=%d)\n",
	    path, uid, gid);
    bb_fullpath(fpath, path);

    retstat = chown(fpath, uid, gid);
    if (retstat < 0)
	retstat = bb_error("bb_chown chown");

    return retstat;
}

/** Change the size of a file */
int bb_truncate(const char *path, off_t newsize)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_truncate(path=\"%s\", newsize=%lld)\n",
	    path, newsize);
    bb_fullpath(fpath, path);

    retstat = truncate(fpath, newsize);
    if (retstat < 0)
	bb_error("bb_truncate truncate");

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
	void* output;
	int err;
	int rc;
	char *zErrMsg = 0;
	char fpath[PATH_MAX];
	bb_fullpath(fpath, path);
	int* is_local;

	drbClient* cli = BB_DATA->client;
	sqlite3* db1 = BB_DATA->sqlite_conn;

    int retstat = 0;
    int fd;
    int* i =0;
    directory* dir;

    //Search to see if the directory is already in the database
    begin_transaction(db1);
    log_msg("\nBegin transaction function has been called!\n");
    dir = search_directory(db1, fpath);

    if( dir == SQLITE_ROW ){
    	i++;
    	log_msg("\nA Record Has Been Found In The Database!\n");
    }else if( dir ==SQLITE_DONE ){
    	log_msg("\nThere's No Such Directory on local or on Dropbox.\n");
    	printf("\nError: No such directory. Please retry.\n");
    	free(log_msg);
    	return err;
    }

    // If the file is not on local, Download it from Dropbox
    if( dir->is_local == 0){
    	log_msg("\nThe File Is Currently Not On Local Disks!\n");
    	log_msg("\nbb_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);
    bb_fullpath(fpath, path);
    FILE *file = fopen(fpath, "w"); // Write it in this file
        output = NULL;
        err = drbGetFile(cli, &output,
                         DRBOPT_PATH, fpath,
                         DRBOPT_IO_DATA, file,
                         DRBOPT_IO_FUNC, fwrite,
                         DRBOPT_END);
        fclose(file);
        log_msg("\nThe File Has Been Downloaded! Checking For Error...\n");

        if (err != DRBERR_OK) {
            printf("Get File error (%d): %s\n", err, (char*)output);
            free(output);
        } else {
        	//Get result as well as update isLocal value in the database table
            update_isLocal(db1, fpath);
            log_msg("\nis_local Has Been Successfully Updated!\n");
            displayMetadata(output, "Get File Result");
            drbDestroyMetadata(output, true);
            log_msg("\nThe File Has Been Downloaded Successfully! \n");

        }
    }
    // If the file is local, just open it.
    // If the file is not local, after download it onto local storage
    // Open the file.
    fd = open(fpath, fi->flags);

    if (fd < 0)
	retstat = bb_error("bb_open open");

    fi->fh = fd;
    log_fi(fi);
    log_msg("\nOpen File!\n");

    // Get timestamp when the file was open
    update_atime(fpath);
    log_msg("\natime Has Been Updated!\n");
    commit_transaction(db1);
    log_msg("\Database Transaction Has Successfully Complete!\n");
    free_directory(dir);
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
    int retstat = 0;
    char fpath[PATH_MAX];
    bb_fullpath(fpath, path);

    log_msg("\nbb_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0)
	retstat = bb_error("bb_read read");

    // Get timestamp when the file was open
    update_atime(fpath);

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
		int rc;
		char *zErrMsg = 0;
		char fpath[PATH_MAX];

//		drbClient* cli = BB_DATA->client;
		sqlite3* db1 = BB_DATA->sqlite_conn;

    int retstat = 0;
    // Get full path to update the database
    bb_fullpath(fpath, path);

    log_msg("\nbb_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi
	    );
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    retstat = pwrite(fi->fh, buf, size, offset);
    if (retstat < 0)
	retstat = bb_error("bb_write pwrite");

    // Get timestamp when the file was open
    begin_transaction(db1);
    update_isModified(db1, fpath);
    update_atime(fpath);
    update_mtime(fpath);
    commit_transaction(db1);

//    free(sql_update_is_modified);

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
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

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
    int retstat = 0;

    log_msg("\nbb_release(path=\"%s\", fi=0x%08x)\n",
	  path, fi);
    log_fi(fi);

    // We need to close the file.  Had we allocated any resources
    // (buffers etc) we'd need to free them here as well.
    retstat = close(fi->fh);

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
    log_fi(fi);

    // some unix-like systems (notably freebsd) don't have a datasync call
#ifdef HAVE_FDATASYNC
    if (datasync)
	retstat = fdatasync(fi->fh);
    else
#endif
	retstat = fsync(fi->fh);

    if (retstat < 0)
	bb_error("bb_fsync fsync");

    return retstat;
}

#ifdef HAVE_SYS_XATTR_H
/** Set extended attributes */
int bb_setxattr(const char *path, const char *name, const char *value, size_t size, int flags)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_setxattr(path=\"%s\", name=\"%s\", value=\"%s\", size=%d, flags=0x%08x)\n",
	    path, name, value, size, flags);
    bb_fullpath(fpath, path);

    retstat = lsetxattr(fpath, name, value, size, flags);
    if (retstat < 0)
	retstat = bb_error("bb_setxattr lsetxattr");

    return retstat;
}

/** Get extended attributes */
int bb_getxattr(const char *path, const char *name, char *value, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_getxattr(path = \"%s\", name = \"%s\", value = 0x%08x, size = %d)\n",
	    path, name, value, size);
    bb_fullpath(fpath, path);

    retstat = lgetxattr(fpath, name, value, size);
    if (retstat < 0)
	retstat = bb_error("bb_getxattr lgetxattr");
    else
	log_msg("    value = \"%s\"\n", value);

    return retstat;
}

/** List extended attributes */
int bb_listxattr(const char *path, char *list, size_t size)
{
    int retstat = 0;
    char fpath[PATH_MAX];
    char *ptr;

    log_msg("bb_listxattr(path=\"%s\", list=0x%08x, size=%d)\n",
	    path, list, size
	    );
    bb_fullpath(fpath, path);

    retstat = llistxattr(fpath, list, size);
    if (retstat < 0)
	retstat = bb_error("bb_listxattr llistxattr");

    log_msg("    returned attributes (length %d):\n", retstat);
    for (ptr = list; ptr < list + retstat; ptr += strlen(ptr)+1)
	log_msg("    \"%s\"\n", ptr);

    return retstat;
}

/** Remove extended attributes */
int bb_removexattr(const char *path, const char *name)
{
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_removexattr(path=\"%s\", name=\"%s\")\n",
	    path, name);
    bb_fullpath(fpath, path);

    retstat = lremovexattr(fpath, name);
    if (retstat < 0)
	retstat = bb_error("bb_removexattr lrmovexattr");

    return retstat;
}
#endif


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

/**
 * Create and open a file
 *
 * If the file does not exist, first create it with the specified
 * mode, and then open it.
 *
 * If this method is not implemented or under Linux kernel
 * versions earlier than 2.6.15, the mknod() and open() methods
 * will be called instead.
 *
 * Introduced in version 2.5
 */
int bb_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
	int rc;
	char *zErrMsg = 0;
	int retstat = 0;
    char fpath[PATH_MAX];
    int fd;

//    drbClient* cli = BB_DATA->client;
    sqlite3* db1 = BB_DATA->sqlite_conn;

    log_msg("\nbb_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
	    path, mode, fi);
    bb_fullpath(fpath, path);

    char* parent_path;
    char* path2 = strdup(fpath);
    parent_path = basename(path2);
    //Create SQL for insert new file directory information into table Directory
    char* sql1 = "INSERT INTO Directory WITH VALUES(";
    char* sql2 = fpath;
    char* sql3 = ", ";
    char* sql4 = "2, 0, 0, 0, 1, 1, 1, 0, 1, 0);";
    char* sql_create_file_dir ="";
//    		(char*)malloc(5+strlen(sql1)+strlen(sql2)+strlen(sql4)+strlen(parent_path));
    sql_create_file_dir = strncpy(sql_create_file_dir, sql1, strlen(sql1)+1);
    sql_create_file_dir = strncpy(sql_create_file_dir, sql2, strlen(sql2+1));
    sql_create_file_dir = strncpy(sql_create_file_dir, sql3, strlen(sql3)+1);
    sql_create_file_dir = strncpy(sql_create_file_dir, parent_path, strlen(parent_path)+1);
    sql_create_file_dir = strncpy(sql_create_file_dir, sql3, strlen(sql3)+1);
    sql_create_file_dir = strncpy(sql_create_file_dir, sql4, strlen(sql4)+1);

    char* sql_begin = "BEGIN TRANSACTION";


    fd = creat(fpath, mode);
    if (fd < 0)
	retstat = bb_error("bb_create creat");

    fi->fh = fd;

    log_fi(fi);

    //Begin transaction to database
    rc = sqlite3_exec(db1, sql_begin, 0, 0, &zErrMsg);
    //If SQLITE is busy, retry twice, if still busy then abort
    for(int i=0;i<2;i++){
    	if(rc == SQLITE_BUSY){
    		delay(50);
    		rc = sqlite3_exec(db1, sql_begin, 0, 0, &zErrMsg);
    	}else{
    		break;
    	}
    }

    // Insert into database table
    rc = sqlite3_exec(db1, sql_create_file_dir, 0, 0, &zErrMsg);
    if(rc != SQLITE_OK){
 	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
 	   sqlite3_free(zErrMsg);
    }

    update_atime(fpath);
    update_mtime(fpath);

    rc = sqlite3_exec(db1, "COMMIT", 0, 0, &zErrMsg);
    if(rc != SQLITE_OK){
  	   fprintf(stderr, "SQL error: %s\n", zErrMsg);
  	   sqlite3_free(zErrMsg);
     }

    return retstat;
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
	if (dir == NULL){
		free(parent_path);
		free(parent_path_in_sqlite);
		return ENOENT;
	}

	if (dir->type != 1){
		free(parent_path);
		free(parent_path_in_sqlite);
		return ENOTDIR;
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

					//Set is_local to 1 for parent folder
					update_isLocal(BB_DATA->sqlite_conn, path_in_sqlite);

					//Save metadata into sqlite for sub file/folders
					drbMetadataList* list = metadata->contents;
					int list_size = list->size;
					for (int i = 0; i < list_size; ++i){
						drbMetadata* sub_metadata = list->array[i];
						directory* sub_dir = directory_from_dbx(sub_metadata);
						err_sqlite = insert_directory(BB_DATA->sqlite_conn, sub_dir);
						free_directory(sub_dir);

						if (err_sqlite != 0){
							ret = EIO;
							break;
						}
					}

					//Finish transaction
					if (err_trans == 0){
						if (err_sqlite == 0){
							commit_transaction(BB_DATA->sqlite_conn);
						}
						else{
							rollback_transaction(BB_DATA->sqlite_conn);
						}
					}
				} else {
					ret = ENOENT;
				}
				release_dbx_metadata(metadata);
			} else {
				log_msg("ibc_opendir - Failed to drbGetMetadata. Error Code: (%d).\n", err_dbx);
				ret = EIO;
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
			if (filler(buf, sub_dirs[i]->entry_name, NULL, 0) != 0) {
				ret = ENOMEM;
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

	if (dir != NULL){
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
	} else {
		ret = EBADF;
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
	  //readlink = bb_readlink,

	  // no .getdir -- that's deprecated
	  /*getdir = NULL,
	  mknod = bb_mknod,
	  mkdir = bb_mkdir,
	  unlink = bb_unlink,
	  rmdir = bb_rmdir,
	  symlink = bb_symlink,
	  rename = bb_rename,
	  link = bb_link,
	  chmod = bb_chmod,
	  chown = bb_chown,
	  truncate = bb_truncate,
	  utime = bb_utime,
	  open = bb_open,
	  read = bb_read,
	  write = bb_write,*/

	  /** Just a placeholder, don't set */ // huh???
	  /*statfs = bb_statfs,
	  flush = bb_flush,
	  release = bb_release,
	  fsync = bb_fsync,*/
	/*
	#ifdef HAVE_SYS_XATTR_H
	  setxattr = bb_setxattr,
	  getxattr = bb_getxattr,
	  listxattr = bb_listxattr,
	  removexattr = bb_removexattr,
	#endif
	*/
	  .opendir = ibc_opendir,
	  .readdir = ibc_readdir,
	  .releasedir = ibc_releasedir,
	  //fsyncdir = bb_fsyncdir,
	  .init = ibc_init,
	  .destroy = ibc_destroy,
	  .access = ibc_access,
	  //create = bb_create,
	  //ftruncate = bb_ftruncate,
	  .fgetattr = ibc_fgetattr
};

void bb_usage()
{
    fprintf(stderr, "usage:  bbfs [FUSE and mount options] rootDir mountPoint\n");
    abort();
}

static int callback(void *NotUsed, int argc, char **argv, char **azColName){
  int i;
  for(i=0; i<argc; i++){
    printf("%s = %s\n", azColName[i], argv[i] ? argv[i] : "NULL");
  }
  printf("-------------------------\n");
  return 0;
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
	char *t_key    = "8pfo7r8fjml1xo6i"; // iihdh3t3dcld9svd < access token key
	char *t_secret = "m4glqxs42dcop4i";  // 0fw3qvfrqo1dlxx < access token secret
	//char *t_key    = "iihdh3t3dcld9svd"; //< access token key
	//char *t_secret = "0fw3qvfrqo1dlxx";  //< access token secret

	// Global initialisation
	drbInit();

	// Create a Dropbox client
	drbClient* cli = drbCreateClient(c_key, c_secret, t_key, t_secret);
    // Set default arguments to not repeat them on each API call
	drbSetDefault(cli, DRBOPT_ROOT, DRBVAL_ROOT_AUTO, DRBOPT_END);

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
    argv[argc-2] = argv[argc-1];
    argv[argc-1] = NULL;
    argc--;

    bb_data->logfile = log_open("bbfs.log");
    bb_data->client = cli;

    sqlite3 *sqlite_conn = init_db("dir.db");
    bb_data->sqlite_conn = sqlite_conn;
    //test_sqlite_insert(sqlite_conn);

    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &bb_oper, bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);

    return fuse_stat;
}

void test_sqlite_insert(sqlite3* db){

	directory* data = new_directory(
			"a",
			"b",
			"c",
			"d",
			1,
			2,
			3,
			4,
			0,
			0,
			0,
			0,
			5,
			"e"
			);
	insert_directory(db, data);
	free_directory(data);

	char* fpathtest = "a";
	update_isLocal(db, fpathtest);
	directory* dir = search_directory(db, "a");

	if (dir != NULL){
		log_msg("\nSuccessfully get all the metadata of file %s\n", dir->full_path);
		log_msg("Successfully get all the metadata of file %s\n", dir->entry_name);
		log_msg("Successfully get all the metadata of file %lld\n", dir->mtime);
		log_msg("Successfully get all the metadata of file %d\n", dir->is_local);
	}
	free_directory(dir); //Dont' forget to release memory to avoid memory leak


	//Insert another record with the same parent folder
	data = new_directory(
					"a2",
					"b",
					"c",
					"d",
					1,
					2,
					3,
					4,
					0,
					0,
					0,
					0,
					5,
					"e"
					);
	insert_directory(db, data);
	free_directory(data); //Dont' forget to release memory to avoid memory leak

	 data = new_directory(
	    			"",
	    			".",
	    			"",
	    			"",
	    			2,
	    			0,
	    			0,
	    			0,
	    			0,
	    			0,
	    			0,
	    			0,
	    			0,
	    			""
	    			);
	    	insert_directory(db, data);
	    	free_directory(data);


	//Query all sub files/folder under a same parent folder
	int rs_cnt = 0;
	directory** dirs = search_subdirectories(db, "b", &rs_cnt);
	if (dirs != NULL){
		log_msg("\n");
		for (int i = 0; i < rs_cnt; ++i)
		log_msg("Successfully get all the metadata of file %s\n", dirs[i]->full_path);
	}
	free_directories(dirs, rs_cnt); //Dont' forget to release memory to avoid memory leak

}
