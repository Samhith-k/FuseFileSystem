/*
 * file: homework.c
 * description: skeleton file for CS 5600 system
 *
 * CS 5600, Computer Systems, Northeastern
 */

#define FUSE_USE_VERSION 27
#define _FILE_OFFSET_BITS 64

#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>
#include <sys/stat.h>

#include "fs5600.h"
#include <linux/stat.h>

/* if you don't understand why you can't use these system calls here,
 * you need to read the assignment description another time
 */
#define stat(a, b) error do not use stat()
#define open(a, b) error do not use open()
#define read(a, b, c) error do not use read()
#define write(a, b, c) error do not use write()

#define MAX_PATH_LEN 10
#define MAX_NAME_LEN 27
#define BLOCK_SIZE 4096

/* disk access. All access is in terms of 4KB blocks; read and
 * write functions return 0 (success) or -EIO.
 */
extern int block_read(void *buf, int lba, int nblks);
extern int block_write(void *buf, int lba, int nblks);

unsigned char g_bitmap[4096];
struct fs_super superblock;
struct fs_inode g_root_node;

/* bitmap functions
 */
void bit_set(unsigned char *map, int i)
{
    map[i / 8] |= (1 << (i % 8));
}
void bit_clear(unsigned char *map, int i)
{
    map[i / 8] &= ~(1 << (i % 8));
}
int bit_test(unsigned char *map, int i)
{
    return map[i / 8] & (1 << (i % 8));
}

/* init - this is called once by the FUSE framework at startup. Ignore
 * the 'conn' argument.
 * recommended actions:
 *   - read superblock
 *   - allocate memory, read bitmaps and inodes
 */
void *fs_init(struct fuse_conn_info *conn)
{
    // Declare the superblock variable

    if (block_read(&superblock, 0, 1) < 0)
    {
        fprintf(stderr, "Error: Failed to read superblock\n");
        return NULL;
    }

    if (block_read(g_bitmap, 1, 1) < 0)
    {
        fprintf(stderr, "Error: Failed to read bitmap\n");
        return NULL;
    };
    if (block_read(&g_root_node, 2, 1) < 0)
    {
        fprintf(stderr, "Error: Failed to read inodes\n");
        return NULL;
    };

    /* your code here */
    return NULL;
}

/* Note on path translation errors:
 * In addition to the method-specific errors listed below, almost
 * every method can return one of the following errors if it fails to
 * locate a file or directory corresponding to a specified path.
 *
 * ENOENT - a component of the path doesn't exist.
 * ENOTDIR - an intermediate component of the path (e.g. 'b' in
 *           /a/b/c) is not a directory
 */

/* note on splitting the 'path' variable:
 * the value passed in by the FUSE framework is declared as 'const',
 * which means you can't modify it. The standard mechanisms for
 * splitting strings in C (strtok, strsep) modify the string in place,
 * so you have to copy the string and then free the copy when you're
 * done. One way of doing this:
 *
 *    char *_path = strdup(path);
 *    int inum = translate(_path);
 *    free(_path);
 */

/* getattr - get file or directory attributes. For a description of
 *  the fields in 'struct stat', see 'man lstat'.
 *
 * Note - for several fields in 'struct stat' there is no corresponding
 *  information in our file system:
 *    st_nlink - always set it to 1
 *    st_atime, st_ctime - set to same value as st_mtime
 *
 * success - return 0
 * errors - path translation, ENOENT
 * hint - factor out inode-to-struct stat conversion - you'll use it
 *        again in readdir
 */

int parse(char *path, char **argv)
{
    int i;
    for (i = 0; i < MAX_PATH_LEN; i++)
    {
        if ((argv[i] = strtok(path, "/")) == NULL)
            break;
        if (strlen(argv[i]) > MAX_NAME_LEN)
            argv[i][MAX_NAME_LEN] = '\0';
    }
    return i;
}

#define ROOT_INUM 2
#define NDIRECT 10
#define MAX_DIR_ENTRIES 128
#define BLOCK_SIZE 4096
#define INODE_TABLE_START 2

int translate(int pathc, char **pathv)
{
    int inum = ROOT_INUM;
    for (int i = 0; i < pathc; i++)
    {
        struct fs_inode inode;
        if (block_read(&inode, inum, 1) < 0)
            return -EIO;
        if (!(inode.mode & S_IFDIR))
            return -ENOTDIR;
        int found = 0;

        for (int j = 0; j < NDIRECT; j++)
        {
            if (inode.ptrs[j] == 0)
                continue;
            struct fs_dirent dirents[MAX_DIR_ENTRIES];
            if (block_read(dirents, inode.ptrs[j], 1) < 0)
            {
                return -EIO;
            }
            for (int k = 0; k < MAX_DIR_ENTRIES; k++)
            {
                if (!dirents[k].valid)
                    continue;
                if (strcmp(dirents[k].name, pathv[i]) == 0)
                {
                    inum = dirents[k].inode;
                    found = 1;
                    break;
                }
            }
            if (found)
                break;
        }
        if (!found)
            return -ENOENT;
    }
    return inum;
}

int fs_getattr(const char *path, struct stat *sb)
{
    char *c_path = strdup(path);
    if (c_path == NULL)
    {
        return -ENOMEM;
    }
    char *tokens[MAX_PATH_LEN];
    int num_tokens = parse(c_path, tokens);
    int inum = translate(num_tokens, tokens);
    free(c_path);
    if (inum < 0)
        return inum;
    struct fs_inode inode;
    if (block_read(&inode, inum, 1) < 0)
    {
        return -EOPNOTSUPP;
    }
    sb->st_mode = inode.mode;
    sb->st_size = inode.size;
    sb->st_nlink = 1;
    sb->st_atime = sb->st_ctime = sb->st_mtime = 0;
    return 0;
}

/* readdir - get directory contents.
 *
 * call the 'filler' function once for each valid entry in the
 * directory, as follows:
 *     filler(buf, <name>, <statbuf>, 0)
 * where <statbuf> is a pointer to a struct stat
 * success - return 0
 * errors - path resolution, ENOTDIR, ENOENT
 *
 * hint - check the testing instructions if you don't understand how
 *        to call the filler function
 */
int fs_readdir(const char *path, void *ptr, fuse_fill_dir_t filler,
               off_t offset, struct fuse_file_info *fi)
{
    char *c_path = strdup(path);
    if (c_path == NULL)
    {
        return -ENOMEM;
    }
    char *tokens[MAX_PATH_LEN];
    int num_tokens = parse(c_path, tokens);
    int inum = translate(num_tokens, tokens);
    free(c_path);
    if (inum < 0)
        return inum;
    struct fs_inode dir_inode;
    if (block_read(&dir_inode, inum, 1) < 0)
    {
        return -EIO;
    }
    if (!(dir_inode.mode & S_IFDIR))
        return -ENOTDIR;
    for (int j = 0; j < NDIRECT; j++)
    {
        if (dir_inode.ptrs[j] == 0)
            continue;
        struct fs_dirent dirents[MAX_DIR_ENTRIES];
        if (block_read(dirents, dir_inode.ptrs[j], 1) < 0)
            return -EIO;

        for (int k = 0; k < MAX_DIR_ENTRIES; k++)
        {
            if (!dirents[k].valid)
                continue;
            struct stat st;
            struct fs_inode entry_inode;
            if (block_read(&entry_inode, dirents[k].inode, 1) < 0)
                return -EIO;
            st.st_mode = entry_inode.mode;
            st.st_size = entry_inode.size;
            st.st_nlink = 1;
            st.st_atime = st.st_ctime = st.st_mtime = 0;
            if (filler(ptr, dirents[k].name, &st, 0) != 0)
                return -ENOMEM;
        }
    }

    /* your code here */
    return 0;
}

/* create - create a new file with specified permissions
 *
 * success - return 0
 * errors - path resolution, EEXIST
 *          in particular, for create("/a/b/c") to succeed,
 *          "/a/b" must exist, and "/a/b/c" must not.
 *
 * Note that 'mode' will already have the S_IFREG bit set, so you can
 * just use it directly. Ignore the third parameter.
 *
 * If a file or directory of this name already exists, return -EEXIST.
 * If there are already 128 entries in the directory (i.e. it's filled an
 * entire block), you are free to return -ENOSPC instead of expanding it.
 */
int fs_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* mkdir - create a directory with the given mode.
 *
 * WARNING: unlike fs_create, @mode only has the permission bits. You
 * have to OR it with S_IFDIR before setting the inode 'mode' field.
 *
 * success - return 0
 * Errors - path resolution, EEXIST
 * Conditions for EEXIST are the same as for create.
 */
int fs_mkdir(const char *path, mode_t mode)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* unlink - delete a file
 *  success - return 0
 *  errors - path resolution, ENOENT, EISDIR
 */
int fs_unlink(const char *path)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* rmdir - remove a directory
 *  success - return 0
 *  Errors - path resolution, ENOENT, ENOTDIR, ENOTEMPTY
 */
int fs_rmdir(const char *path)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* rename - rename a file or directory
 * success - return 0
 * Errors - path resolution, ENOENT, EINVAL, EEXIST
 *
 * ENOENT - source does not exist
 * EEXIST - destination already exists
 * EINVAL - source and destination are not in the same directory
 *
 * Note that this is a simplified version of the UNIX rename
 * functionality - see 'man 2 rename' for full semantics. In
 * particular, the full version can move across directories, replace a
 * destination file, and replace an empty directory with a full one.
 */
int fs_rename(const char *src_path, const char *dst_path)
{
    char *src_copy = strdup(src_path);
    char *dst_copy = strdup(dst_path);
    if (!src_copy || !dst_copy)
    {
        free(src_copy);
        free(dst_copy);
        return -ENOMEM;
    }

    char *src_tokens[MAX_PATH_LEN];
    char *dst_tokens[MAX_PATH_LEN];
    int src_count = parse(src_copy, src_tokens);
    int dst_count = parse(dst_copy, dst_tokens);
    if (src_count < 1 || dst_count < 1 || src_count != dst_count)
    {
        free(src_copy);
        free(dst_copy);
        return -EINVAL;
    }
    // Check that the parent tokens match.
    for (int i = 0; i < src_count - 1; i++)
    {
        if (strcmp(src_tokens[i], dst_tokens[i]) != 0)
        {
            free(src_copy);
            free(dst_copy);
            return -EINVAL;
        }
    }

    // Save only the last (basename) tokens.
    char src_basename[MAX_NAME_LEN + 1];
    char dst_basename[MAX_NAME_LEN + 1];
    strncpy(src_basename, src_tokens[src_count - 1], MAX_NAME_LEN);
    src_basename[MAX_NAME_LEN] = '\0';
    strncpy(dst_basename, dst_tokens[dst_count - 1], MAX_NAME_LEN);
    dst_basename[MAX_NAME_LEN] = '\0';

    // We only need the parent's tokens (all but the last).
    int parent_inum = ROOT_INUM;
    if (src_count > 1)
    {
        parent_inum = translate(src_count - 1, src_tokens);
        if (parent_inum < 0)
        {
            free(src_copy);
            free(dst_copy);
            return parent_inum;
        }
    }
    free(src_copy);
    free(dst_copy);

    // Read the parent's inode.
    struct fs_inode parent_inode;
    if (block_read(&parent_inode, parent_inum, 1) < 0)
        return -EIO;
    if (!(parent_inode.mode & S_IFDIR))
        return -ENOTDIR;

    int entry_found = 0;
    // Iterate through parent's directory blocks.
    for (int i = 0; i < NDIRECT; i++)
    {
        if (parent_inode.ptrs[i] == 0)
            continue;
        struct fs_dirent dirents[MAX_DIR_ENTRIES];
        if (block_read(dirents, parent_inode.ptrs[i], 1) < 0)
            return -EIO;

        // First, verify dst_basename doesn't already exist.
        for (int j = 0; j < MAX_DIR_ENTRIES; j++)
        {
            if (dirents[j].valid && strcmp(dirents[j].name, dst_basename) == 0)
                return -EEXIST;
        }
        // Then, look for the source entry.
        for (int j = 0; j < MAX_DIR_ENTRIES; j++)
        {
            if (dirents[j].valid && strcmp(dirents[j].name, src_basename) == 0)
            {
                // Update name to dst_basename.
                strncpy(dirents[j].name, dst_basename, MAX_NAME_LEN);
                dirents[j].name[MAX_NAME_LEN] = '\0';
                if (block_write(dirents, parent_inode.ptrs[i], 1) < 0)
                    return -EIO;
                entry_found = 1;
                break;
            }
        }
        if (entry_found)
            break;
    }
    return entry_found ? 0 : -ENOENT;
}

/* chmod - change file permissions
 * utime - change access and modification times
 *         (for definition of 'struct utimebuf', see 'man utime')
 *
 * success - return 0
 * Errors - path resolution, ENOENT.
 */
int fs_chmod(const char *path, mode_t mode)
{
    char *dup_path = strdup(path);
    if (!dup_path)
        return -ENOMEM;
    char *tokens[MAX_PATH_LEN];
    int token_count = parse(dup_path, tokens);
    int inum = translate(token_count, tokens);
    free(dup_path);
    if (inum < 0)
        return inum;

    struct fs_inode inode;
    if (block_read(&inode, inum, 1) < 0)
        return -EIO;

    /* Preserve file type bits, update permission bits.
       S_IFMT masks the file type; ~S_IFMT masks the permission bits. */
    inode.mode = (inode.mode & S_IFMT) | (mode & ~S_IFMT);

    if (block_write(&inode, inum, 1) < 0)
        return -EIO;

    return 0;
}

int fs_utime(const char *path, struct utimbuf *ut)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* truncate - truncate file to exactly 'len' bytes
 * success - return 0
 * Errors - path resolution, ENOENT, EISDIR, EINVAL
 *    return EINVAL if len > 0.
 */
int fs_truncate(const char *path, off_t len)
{
    /* you can cheat by only implementing this for the case of len==0,
     * and an error otherwise.
     */
    if (len != 0)
        return -EINVAL; /* invalid argument */

    /* your code here */
    return -EOPNOTSUPP;
}

/* read - read data from an open file.
 * success: should return exactly the number of bytes requested, except:
 *   - if offset >= file len, return 0
 *   - if offset+len > file len, return #bytes from offset to end
 *   - on error, return <0
 * Errors - path resolution, ENOENT, EISDIR
 */
int fs_read(const char *path, char *buf, size_t len, off_t offset,
            struct fuse_file_info *fi)
{
    /* your code here */
    char *dup_path = strdup(path);
    if (!dup_path)
        return -ENOMEM;
    char *tokens[MAX_PATH_LEN];
    int token_count = parse(dup_path, tokens);
    int inum = translate(token_count, tokens);
    free(dup_path);
    if (inum < 0)
        return inum;

    struct fs_inode inode;
    if (block_read(&inode, inum, 1) < 0)
        return -EIO;

    /* Check if inode is a directory */
    if (inode.mode & S_IFDIR)
        return -EISDIR;

    /* If offset is beyond file length, return 0 */
    if (offset >= inode.size)
        return 0;

    /* Adjust length if reading past end of file */
    if (offset + len > inode.size)
        len = inode.size - offset;

    size_t total_read = 0;
    int start_block = offset / BLOCK_SIZE;
    int block_offset = offset % BLOCK_SIZE;
    char block_buf[BLOCK_SIZE];

    for (int i = start_block; total_read < len && i < NDIRECT; i++)
    {
        if (inode.ptrs[i] == 0)
            break;
        if (block_read(block_buf, inode.ptrs[i], 1) < 0)
            return -EIO;

        int available = BLOCK_SIZE - block_offset;
        int to_copy = (len - total_read < available) ? (len - total_read) : available;
        memcpy(buf + total_read, block_buf + block_offset, to_copy);
        total_read += to_copy;
        block_offset = 0;
    }

    return total_read;
}

/* write - write data to a file
 * success - return number of bytes written. (this will be the same as
 *           the number requested, or else it's an error)
 * Errors - path resolution, ENOENT, EISDIR
 *  return EINVAL if 'offset' is greater than current file length.
 *  (POSIX semantics support the creation of files with "holes" in them,
 *   but we don't)
 */
int fs_write(const char *path, const char *buf, size_t len,
             off_t offset, struct fuse_file_info *fi)
{
    /* your code here */
    return -EOPNOTSUPP;
}

/* statfs - get file system statistics
 * see 'man 2 statfs' for description of 'struct statvfs'.
 * Errors - none. Needs to work.
 */
int fs_statfs(const char *path, struct statvfs *st)
{
    /* needs to return the following fields (set others to zero):
     *   f_bsize = BLOCK_SIZE
     *   f_blocks = total image - (superblock + block map)
     *   f_bfree = f_blocks - blocks used
     *   f_bavail = f_bfree
     *   f_namemax = <whatever your max namelength is>
     *
     * it's OK to calculate this dynamically on the rare occasions
     * when this function is called.
     */
    /* your code here */
    unsigned int total_blocks, used_blocks = 0;
    int i, j;

    /* Set block and fragment sizes */
    st->f_bsize = BLOCK_SIZE;
    st->f_frsize = BLOCK_SIZE;

    /* Total blocks available for files is the total image blocks minus the superblock and bitmap */
    total_blocks = superblock.disk_size > 2 ? (superblock.disk_size - 2) : 0;
    st->f_blocks = total_blocks;

    /* Count the number of blocks in use by scanning the bitmap.
       g_bitmap has 4096 bytes; each bit represents one block.
    */
    for (i = 0; i < 4096; i++)
    {
        unsigned char byte = g_bitmap[i];
        for (j = 0; j < 8; j++)
        {
            if (byte & (1 << j))
                used_blocks++;
        }
    }
    if (used_blocks > total_blocks)
        used_blocks = total_blocks;
    st->f_bfree = total_blocks - used_blocks;
    st->f_bavail = st->f_bfree;

    st->f_namemax = MAX_NAME_LEN;

    /* Other fields can be set to 0, if not used by your file system */
    st->f_files = 0;
    st->f_ffree = 0;
    st->f_favail = 0;
    st->f_fsid = 0;
    st->f_flag = 0;

    return 0;
}

/* operations vector. Please don't rename it, or else you'll break things
 */
struct fuse_operations fs_ops = {
    .init = fs_init, /* read-mostly operations */
    .getattr = fs_getattr,
    .readdir = fs_readdir,
    .rename = fs_rename,
    .chmod = fs_chmod,
    .read = fs_read,
    .statfs = fs_statfs,

    .create = fs_create, /* write operations */
    .mkdir = fs_mkdir,
    .unlink = fs_unlink,
    .rmdir = fs_rmdir,
    .utime = fs_utime,
    .truncate = fs_truncate,
    .write = fs_write,
};
