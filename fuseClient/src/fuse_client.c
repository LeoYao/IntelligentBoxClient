

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

//#define HAVE_SYS_XATTR_H

#ifdef HAVE_SYS_XATTR_H
#include <sys/xattr.h>
#endif

#include <dropbox_log_utils.h>
#include <common_utils.h>


#include <sqlite3.h>

/*
* From LY:
* The below include is just for showing how to work with sqlite.
* It should be removed in formal code
**/
#include <test_sqlite.h>

 	char *c_key    = "d9m9s1iylifpqsx";  //< consumer key
	char *c_secret = "x2pfq4vkf5bytnq";  //< consumer secret

	// User key and secret. Leave them NULL or set them with your AccessToken.
	//char *t_key    = "8pfo7r8fjml1xo6i"; // iihdh3t3dcld9svd < access token key
	//char *t_secret = "m4glqxs42dcop4i";  // 0fw3qvfrqo1dlxx < access token secret
	char *t_key    = "iihdh3t3dcld9svd"; //< access token key
	char *t_secret = "0fw3qvfrqo1dlxx";  //< access token secret

	// Create a Dropbox client
	drbClient* cli = drbCreateClient(c_key, c_secret, t_key, t_secret);
	void* output;
	int err;
	sqlite3 *db1;
	int rc;
	char *zErrMsg = 0;

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
    int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_mkdir(path=\"%s\", mode=0%3o)\n",
	    path, mode);
    bb_fullpath(fpath, path);

    retstat = mkdir(fpath, mode);
    if (retstat < 0)
	retstat = bb_error("bb_mkdir mkdir");

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
 */
int bb_open(const char *path, struct fuse_file_info *fi)
{
    int retstat = 0;
    int fd;
    char fpath[PATH_MAX];
    //Prepare the sql statement
    char* sql1 = "SELECT is_local FROM Directory WHERE full_path =";
    char* sql2 = fpath;
    char* sql = (char*)malloc(1+strlen(sql1)+strlen(sql2));
    strcpy(sql, sql1);
    strcpy(sql, sql2);
    rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);

    if(callback=0){
    log_msg("\nbb_open(path\"%s\", fi=0x%08x)\n",
	    path, fi);
    bb_fullpath(fpath, path);
    FILE *file = fopen(fpath, "w"); // Write it in this file
        output = NULL;
        err = drbGetFile(cli, &output,
                         DRBOPT_PATH, "/hello.txt",
                         DRBOPT_IO_DATA, file,
                         DRBOPT_IO_FUNC, fwrite,
                         DRBOPT_END);
        fclose(file);

        if (err != DRBERR_OK) {
            printf("Get File error (%d): %s\n", err, (char*)output);
            free(output);
        } else {
            displayMetadata(output, "Get File Result");
            drbDestroyMetadata(output, true);
        }
    }
    else{
    fd = open(fpath, fi->flags);
    if (fd < 0)
	retstat = bb_error("bb_open open");

    fi->fh = fd;
    log_fi(fi);

    return retstat;
    }
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

    log_msg("\nbb_read(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi);
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    retstat = pread(fi->fh, buf, size, offset);
    if (retstat < 0)
	retstat = bb_error("bb_read read");

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
    int retstat = 0;

    log_msg("\nbb_write(path=\"%s\", buf=0x%08x, size=%d, offset=%lld, fi=0x%08x)\n",
	    path, buf, size, offset, fi
	    );
    // no need to get fpath on this one, since I work from fi->fh not the path
    log_fi(fi);

    retstat = pwrite(fi->fh, buf, size, offset);
    if (retstat < 0)
	retstat = bb_error("bb_write pwrite");

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
void *bb_init(struct fuse_conn_info *conn)
{
    log_msg("\nbb_init()\n");

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
void bb_destroy(void *userdata)
{
	struct bb_state *private_data = userdata;
	drbDestroyClient(private_data->client);
	drbCleanup();

    log_msg("\nbb_destroy(userdata=0x%08x)\n", userdata);
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
int bb_access(const char *path, int mask)
{
	log_msg("\nbb_access: %s\n", path);
	return 0;

    /*int retstat = 0;
    char fpath[PATH_MAX];

    log_msg("\nbb_access(path=\"%s\", mask=0%o)\n",
	    path, mask);
    bb_fullpath(fpath, path);

    retstat = access(fpath, mask);

    if (retstat < 0)
	retstat = bb_error("bb_access access");

    return retstat;*/
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
    int retstat = 0;
    char fpath[PATH_MAX];
    int fd;

    log_msg("\nbb_create(path=\"%s\", mode=0%03o, fi=0x%08x)\n",
	    path, mode, fi);
    bb_fullpath(fpath, path);

    fd = creat(fpath, mode);
    if (fd < 0)
	retstat = bb_error("bb_create creat");

    fi->fh = fd;

    log_fi(fi);

    return retstat;
}


/** Open directory
 *
 * This method should check if the open operation is permitted for
 * this  directory
 *
 * Introduced in version 2.3
 */
int bb_opendir(const char *path, struct fuse_file_info *fi)
{
	log_msg("\nbb_opendir: %s\n", path);
	//return 0;


	void* output = NULL;
	int err = drbGetMetadata(BB_DATA->client, &output,
						 DRBOPT_PATH, path,
						 DRBOPT_LIST, true,
						 //                     DRBOPT_FILE_LIMIT, 100,
						 DRBOPT_END);
	if (err != DRBERR_OK) {
		log_msg("bb_opendir - drbGetMetadata error (%d): %s\n", err, (char*)output);
		free(output);
		return ENOENT;
	} else {
		log_msg("bb_opendir - drbGetMetadata: succeed in reading meta data (%d).\n", err);
		fi->fh = (intptr_t)output;
	}

	return 0;
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
int bb_readdir(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset,
	       struct fuse_file_info *fi)
{

	log_msg("\nbb_readdir: %s\n", path);

	drbMetadata* meta = (drbMetadata*)fi->fh;
	for (int i = 0; i < meta->contents->size; i++) {
		drbMetadata* entry = meta->contents->array[i];
		int lastSlashPos = getLastSlashPosition(entry->path);
		if (filler(buf, entry->path + lastSlashPos, NULL, 0) != 0) {
				log_msg("    ERROR bb_readdir filler:  buffer full\n");
				return -ENOMEM;
		}
		log_msg("bb_readdir - filler: %s.\n", entry->path + lastSlashPos);
	}

	drbDestroyMetadata(meta, true);

	return 0;


}

/** Release directory
 *
 * Introduced in version 2.3
 */
int bb_releasedir(const char *path, struct fuse_file_info *fi)
{
	log_msg("\nbb_releasedir: %s\n", path);
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
    log_msg("\nbb_getattr: %s\n", path);

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

	void * output = NULL;

	memset(statbuf, 0, sizeof(struct stat)); //clear memory

	int err = drbGetMetadata(BB_DATA->client, &output,
						 DRBOPT_PATH, path,
						 DRBOPT_LIST, true,
						 //                     DRBOPT_FILE_LIMIT, 100,
						 DRBOPT_END);
	if (err != DRBERR_OK) {
		log_msg("Metadata error (%d): %s\n", err, (char*)output);
		free(output);
	} else {
		drbMetadata* meta = (drbMetadata*)output;
		//displayMetadata(meta, "Metadata");
		statbuf->st_size = *(meta->bytes);
		statbuf->st_uid = getuid();
		statbuf->st_gid = getgid();

		if (*(meta->isDir))
		{
			statbuf->st_mode = S_IFDIR | 0755;

			//increase nlink if there are subfolders
			int subdirCount = 0;
			for (int i = 0; i < meta->contents->size; i++) {
				drbMetadata* entry = meta->contents->array[i];
				if (*(entry->isDir))
				{
					++subdirCount;
				}
			}
			statbuf->st_nlink = 2 + subdirCount;
		}
		else
		{
			statbuf->st_mode = S_IFREG | 0755;
			statbuf->st_nlink = 1;
		}

		drbDestroyMetadata(meta, true);
		log_stat(statbuf);
	}

	return err;
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
int bb_fgetattr(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
	/*
    int retstat = 0;

    log_msg("\nbb_fgetattr(path=\"%s\", statbuf=0x%08x, fi=0x%08x)\n",
	    path, statbuf, fi);
    log_fi(fi);

    // On FreeBSD, trying to do anything with the mountpoint ends up
    // opening it, and then using the FD for an fgetattr.  So in the
    // special case of a path of "/", I need to do a getattr on the
    // underlying root directory instead of doing the fgetattr().
    if (!strcmp(path, "/"))
	return bb_getattr(path, statbuf);

    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
	retstat = bb_error("bb_fgetattr fstat");

    log_stat(statbuf);

    return retstat;
*/

    log_msg("\nbb_fgetattr: %s", path);
    void * output = NULL;
	int err = drbGetMetadata(BB_DATA->client, &output,
						 DRBOPT_PATH, path,
						 DRBOPT_LIST, true,
						 //                     DRBOPT_FILE_LIMIT, 100,
						 DRBOPT_END);
	if (err != DRBERR_OK) {
		printf("Metadata error (%d): %s\n", err, (char*)output);
		free(output);
	} else {
		drbMetadata* meta = (drbMetadata*)output;
		//displayMetadata(meta, "Metadata");
		statbuf->st_size = *(meta->size);

		drbDestroyMetadata(meta, true);
	}
	return err;
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
	  .opendir = bb_opendir,
	  .readdir = bb_readdir,
	  .releasedir = bb_releasedir,
	  //fsyncdir = bb_fsyncdir,
	  .init = bb_init,
	  .destroy = bb_destroy,
	  .access = bb_access,
	  //create = bb_create,
	  //ftruncate = bb_ftruncate,
	  //fgetattr = bb_fgetattr
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

//Initiating database
int init_sqlite(){

		rc = sqlite3_open_v2("dir.db", &db1, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
	if( rc ){
		fprintf(stderr, "Can't open database: %s\n", sqlite3_errmsg(db1));
		sqlite3_close(db1);
		return(1);
	}

	// Create a table Directory for storage several metadata on the files
	// Or on Dropbox. If table already exists, ignore the SQL.
	char *sql = "create table if not exists DIRECTORY (full_path varchar(4000) PRIMARY KEY, parent_folder_full_path varchar(4000), entry_name varchar(255), old_full_path varchar(4000), type integer, size integer, mtime datetime, atime datetime, is_locked integer, is_modified integer, is_local integer, is_deleted integer, in_use_count integer);";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
		if( rc!=SQLITE_OK ){
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}
	sql = "create table if not exists LOCK (dummy char(1));";
	rc = sqlite3_exec(db1, sql, 0, 0, &zErrMsg);
		if( rc!=SQLITE_OK ){
			fprintf(stderr, "SQL error: %s\n", zErrMsg);
			sqlite3_free(zErrMsg);
		}

}



int main(int argc, char *argv[])
{
	/*
	* From LY:
	* The below function call is just for showing how to work with sqlite.
	* It should be removed in formal code
	**/
	test_sqlite();

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

    bb_data->logfile = log_open();

    bb_data->client = cli;

    // turn over control to fuse
    fprintf(stderr, "about to call fuse_main\n");
    fuse_stat = fuse_main(argc, argv, &bb_oper, bb_data);
    fprintf(stderr, "fuse_main returned %d\n", fuse_stat);
    init_sqlite();

    return fuse_stat;
}

