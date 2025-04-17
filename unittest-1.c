/*
 * file:        unittest-1.c
 * description: Unit tests for Part 1 (read-oriented filesystem ops)
 *
 * This suite tests your fs_ops for:
 *   - getattr
 *   - readdir
 *   - read
 *   - statfs
 *   - rename
 *   - chmod
 *
 * CS 5600, Computer Systems, Northeastern
 */

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <check.h>
#include <zlib.h>
#include <fuse.h>
#include <stdlib.h>
#include <errno.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

extern struct fuse_operations fs_ops;
extern void block_init(char *file);

/* Test data for files in test.img */
struct test_file_info
{
    char *path;
    int size;
    unsigned int cksum;
    unsigned int mode;
    unsigned int uid;
    unsigned int gid;
    time_t mtime;
};

/* File information for files in test.img */
struct test_file_info test_files[] = {
    {"/file.1k", 1000, 1726121896, 0100666, 500, 500, 1565283152},
    {"/file.10", 10, 3766980606, 0100666, 500, 500, 1565283167},
    {"/dir-with-long-name/file.12k+", 12289, 2781093465, 0100666, 0, 500, 1565283167},
    {"/dir2/twenty-seven-byte-file-name", 1000, 2902524398, 0100666, 500, 500, 1565283167},
    {"/dir2/file.4k+", 4098, 1626046637, 0100777, 500, 500, 1565283167},
    {"/dir3/subdir/file.4k-", 4095, 2991486384, 0100666, 500, 500, 1565283167},
    {"/dir3/subdir/file.8k-", 8190, 724101859, 0100666, 500, 500, 1565283167},
    {"/dir3/subdir/file.12k", 12288, 1483119748, 0100666, 500, 500, 1565283167},
    {"/dir3/file.12k-", 12287, 1203178000, 0100777, 0, 500, 1565283167},
    {"/file.8k+", 8195, 1217760297, 0100666, 500, 500, 1565283167},
    {NULL} /* Sentinel */
};

/* Directory information for directories in test.img */
struct test_dir_info
{
    char *path;
    unsigned int mode;
    unsigned int uid;
    unsigned int gid;
    time_t mtime;
} test_dirs[] = {
    {"/", 040777, 0, 0, 1565283167},
    {"/dir-with-long-name", 040777, 0, 0, 1565283167},
    {"/dir2", 040777, 500, 500, 1565283167},
    {"/dir3", 040777, 0, 500, 1565283167},
    {"/dir3/subdir", 040777, 0, 500, 1565283167},
    {NULL} /* Sentinel */
};

/* Directory contents for testing readdir */
struct test_dir_contents
{
    char *path;
    int num_entries;
    char *entries[10]; /* Max 10 entries per dir for this test */
} test_dir_contents[] = {
    {"/", 6, {"dir2", "dir3", "dir-with-long-name", "file.10", "file.1k", "file.8k+"}},
    {"/dir2", 2, {"twenty-seven-byte-file-name", "file.4k+"}},
    {"/dir3", 2, {"subdir", "file.12k-"}},
    {"/dir3/subdir", 3, {"file.4k-", "file.8k-", "file.12k"}},
    {"/dir-with-long-name", 1, {"file.12k+"}},
    {NULL} /* Sentinel */
};

/* Helper to track entries found in readdir */
struct dir_list
{
    int count;
    char seen[20][30]; /* Max 20 entries, names up to 29 chars */
};

/* Callback function for readdir testing */
int readdir_filler(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
    struct dir_list *list = (struct dir_list *)buf;

    /* Skip . and .. entries */
    if (strcmp(name, ".") == 0 || strcmp(name, "..") == 0)
        return 0;

    strncpy(list->seen[list->count], name, 29);
    list->seen[list->count][29] = '\0';
    list->count++;

    return 0;
}

/* Tests for fs_getattr */
START_TEST(test_getattr_files)
{
    struct stat sb;
    int i;

    /* Test each file from our test data */
    for (i = 0; test_files[i].path != NULL; i++)
    {
        int rv = fs_ops.getattr(test_files[i].path, &sb);

        ck_assert_msg(rv == 0, "getattr(%s) failed with %d", test_files[i].path, rv);
        ck_assert_msg(S_ISREG(sb.st_mode), "Not a regular file: %s", test_files[i].path);
        ck_assert_int_eq(sb.st_size, test_files[i].size);
        ck_assert_int_eq(sb.st_mode, test_files[i].mode);
        ck_assert_int_eq(sb.st_uid, test_files[i].uid);
        ck_assert_int_eq(sb.st_gid, test_files[i].gid);
        ck_assert_int_eq(sb.st_mtime, test_files[i].mtime);
    }
}
END_TEST

START_TEST(test_getattr_dirs)
{
    struct stat sb;
    int i;

    /* Test each directory from our test data */
    for (i = 0; test_dirs[i].path != NULL; i++)
    {
        int rv = fs_ops.getattr(test_dirs[i].path, &sb);

        ck_assert_msg(rv == 0, "getattr(%s) failed with %d", test_dirs[i].path, rv);
        ck_assert_msg(S_ISDIR(sb.st_mode), "Not a directory: %s", test_dirs[i].path);
        ck_assert_int_eq(sb.st_mode, test_dirs[i].mode);
        ck_assert_int_eq(sb.st_uid, test_dirs[i].uid);
        ck_assert_int_eq(sb.st_gid, test_dirs[i].gid);
        ck_assert_int_eq(sb.st_mtime, test_dirs[i].mtime);
    }
}
END_TEST

START_TEST(test_getattr_errors)
{
    struct stat sb;
    int rv;

    /* Test non-existent paths */
    rv = fs_ops.getattr("/not-a-file", &sb);
    ck_assert_int_eq(rv, -ENOENT);

    rv = fs_ops.getattr("/not-a-dir/file", &sb);
    ck_assert_int_eq(rv, -ENOENT);

    /* Test ENOTDIR error */
    rv = fs_ops.getattr("/file.1k/subpath", &sb);
    ck_assert_int_eq(rv, -ENOTDIR);
}
END_TEST

/* Tests for fs_readdir */
START_TEST(test_readdir)
{
    int i, j, found;

    for (i = 0; test_dir_contents[i].path != NULL; i++)
    {
        struct dir_list list = {0};
        int rv = fs_ops.readdir(test_dir_contents[i].path, &list, readdir_filler, 0, NULL);

        ck_assert_msg(rv == 0, "readdir(%s) failed with %d", test_dir_contents[i].path, rv);

        /* Check if all expected entries are found */
        ck_assert_int_eq(list.count, test_dir_contents[i].num_entries);

        /* Verify each entry exists */
        for (j = 0; j < test_dir_contents[i].num_entries; j++)
        {
            found = 0;

            for (int k = 0; k < list.count; k++)
            {
                if (strcmp(list.seen[k], test_dir_contents[i].entries[j]) == 0)
                {
                    found = 1;
                    break;
                }
            }

            ck_assert_msg(found, "Entry '%s' not found in directory '%s'",
                          test_dir_contents[i].entries[j], test_dir_contents[i].path);
        }
    }
}
END_TEST

START_TEST(test_readdir_errors)
{
    struct dir_list list = {0};
    int rv;

    /* Test non-existent directory */
    rv = fs_ops.readdir("/not-a-directory", &list, readdir_filler, 0, NULL);
    ck_assert_int_eq(rv, -ENOENT);

    /* Test file (not a directory) */
    rv = fs_ops.readdir("/file.1k", &list, readdir_filler, 0, NULL);
    ck_assert_int_eq(rv, -ENOTDIR);
}
END_TEST

/* Tests for fs_read */
START_TEST(test_read_full)
{
    int i;
    char *buf;

    for (i = 0; test_files[i].path != NULL; i++)
    {
        int size = test_files[i].size;
        buf = malloc(size + 100);      /* Extra space to detect overreads */
        memset(buf, 0xAA, size + 100); /* Fill with pattern to detect partial reads */

        int rv = fs_ops.read(test_files[i].path, buf, size + 50, 0, NULL);

        ck_assert_msg(rv == size, "read(%s) returned %d, expected %d",
                      test_files[i].path, rv, size);

        /* Verify data integrity using checksum */
        unsigned int cksum = crc32(0, (unsigned char *)buf, size);
        ck_assert_msg(cksum == test_files[i].cksum,
                      "Checksum mismatch for %s: got %u, expected %u",
                      test_files[i].path, cksum, test_files[i].cksum);

        /* Verify no overread - check pattern outside expected data */
        ck_assert_int_eq((unsigned char)buf[size + 10], 0xAA);

        free(buf);
    }
}
END_TEST

START_TEST(test_read_partial)
{
    /* Test partial read with specific chunk size */
    int chunk_size = 1000;
    char *path = "/dir3/subdir/file.12k"; /* 12288 bytes */
    int full_size = 12288;

    char *full_buf = malloc(full_size);
    char *chunk_buf = malloc(full_size);

    /* Read the whole file at once for reference */
    int rv = fs_ops.read(path, full_buf, full_size, 0, NULL);
    ck_assert_int_eq(rv, full_size);

    /* Read file in chunks */
    int offset = 0;
    while (offset < full_size)
    {
        int to_read = (offset + chunk_size > full_size) ? (full_size - offset) : chunk_size;

        rv = fs_ops.read(path, chunk_buf + offset, to_read, offset, NULL);
        ck_assert_msg(rv == to_read, "Partial read returned %d, expected %d", rv, to_read);

        offset += to_read;
    }

    /* Verify chunks combined equal the full file */
    ck_assert_msg(memcmp(full_buf, chunk_buf, full_size) == 0,
                  "Data mismatch between full read and chunked reads");

    free(full_buf);
    free(chunk_buf);
}
END_TEST

START_TEST(test_read_offset)
{
    /* Test reading with non-zero offset */
    char *path = "/file.1k"; /* 1000 bytes */
    int size = 1000;
    int offset = 500;

    char *full_buf = malloc(size);
    char *partial_buf = malloc(size - offset);

    /* Read the whole file */
    int rv = fs_ops.read(path, full_buf, size, 0, NULL);
    ck_assert_int_eq(rv, size);

    /* Read second half of file */
    rv = fs_ops.read(path, partial_buf, size, offset, NULL);
    ck_assert_int_eq(rv, size - offset);

    /* Verify partial read matches corresponding portion of full read */
    ck_assert_msg(memcmp(full_buf + offset, partial_buf, size - offset) == 0,
                  "Offset read data doesn't match portion of full read");

    free(full_buf);
    free(partial_buf);
}
END_TEST

START_TEST(test_read_errors)
{
    char buf[100];
    int rv;

    /* Test non-existent file */
    rv = fs_ops.read("/not-a-file", buf, 100, 0, NULL);
    ck_assert_int_eq(rv, -ENOENT);

    /* Test directory (not a file) */
    rv = fs_ops.read("/dir2", buf, 100, 0, NULL);
    ck_assert_int_eq(rv, -EISDIR);

    /* Test offset beyond file size - should return 0 */
    rv = fs_ops.read("/file.10", buf, 100, 20, NULL);
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Test for fs_statfs */
START_TEST(test_statfs)
{
    struct statvfs st;
    int rv = fs_ops.statfs("/", &st);

    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(st.f_bsize, 4096);
    ck_assert_int_eq(st.f_blocks, 400);
    ck_assert_int_eq(st.f_bfree, 355);
    ck_assert_int_eq(st.f_bavail, 355);
    ck_assert_int_eq(st.f_namemax, 27);
}
END_TEST

/* Tests for fs_chmod */
START_TEST(test_chmod)
{
    struct stat sb;
    int rv;

    /* Change permissions on a file */
    rv = fs_ops.chmod("/file.1k", 0644);
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.getattr("/file.1k", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_mode & 0777, 0644);
    ck_assert(S_ISREG(sb.st_mode)); /* Still a regular file */

    /* Restore original permissions */
    rv = fs_ops.chmod("/file.1k", 0666);
    ck_assert_int_eq(rv, 0);

    /* Change permissions on a directory */
    rv = fs_ops.chmod("/dir2", 0700);
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.getattr("/dir2", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_mode & 0777, 0700);
    ck_assert(S_ISDIR(sb.st_mode)); /* Still a directory */

    /* Restore original permissions */
    rv = fs_ops.chmod("/dir2", 0777);
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Tests for fs_rename */
START_TEST(test_rename)
{
    struct stat sb;
    int rv;

    /* Rename a file */
    rv = fs_ops.rename("/file.10", "/file.renamed");
    ck_assert_int_eq(rv, 0);

    /* Verify original doesn't exist */
    rv = fs_ops.getattr("/file.10", &sb);
    ck_assert_int_eq(rv, -ENOENT);

    /* Verify new name exists with same properties */
    rv = fs_ops.getattr("/file.renamed", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 10);

    /* Read from renamed file */
    char buf[20];
    rv = fs_ops.read("/file.renamed", buf, 20, 0, NULL);
    ck_assert_int_eq(rv, 10);

    /* Restore original name */
    rv = fs_ops.rename("/file.renamed", "/file.10");
    ck_assert_int_eq(rv, 0);

    /* Rename a directory */
    rv = fs_ops.rename("/dir2", "/dir.renamed");
    ck_assert_int_eq(rv, 0);

    /* Verify we can still access file within renamed directory */
    rv = fs_ops.getattr("/dir.renamed/file.4k+", &sb);
    ck_assert_int_eq(rv, 0);

    /* Restore original name */
    rv = fs_ops.rename("/dir.renamed", "/dir2");
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Main test runner */
int main(int argc, char **argv)
{
    /* Regenerate test disk each time */
    system("python gen-disk.py -q disk1.in test.img");

    block_init("test.img");
    fs_ops.init(NULL);

    Suite *s = suite_create("fs5600_part1");

    /* Add test cases */
    TCase *tc_getattr = tcase_create("getattr");
    tcase_add_test(tc_getattr, test_getattr_files);
    tcase_add_test(tc_getattr, test_getattr_dirs);
    tcase_add_test(tc_getattr, test_getattr_errors);
    suite_add_tcase(s, tc_getattr);

    TCase *tc_readdir = tcase_create("readdir");
    tcase_add_test(tc_readdir, test_readdir);
    tcase_add_test(tc_readdir, test_readdir_errors);
    suite_add_tcase(s, tc_readdir);

    TCase *tc_read = tcase_create("read");
    tcase_add_test(tc_read, test_read_full);
    tcase_add_test(tc_read, test_read_partial);
    tcase_add_test(tc_read, test_read_offset);
    tcase_add_test(tc_read, test_read_errors);
    suite_add_tcase(s, tc_read);

    TCase *tc_statfs = tcase_create("statfs");
    tcase_add_test(tc_statfs, test_statfs);
    suite_add_tcase(s, tc_statfs);

    TCase *tc_modify = tcase_create("modify");
    tcase_add_test(tc_modify, test_chmod);
    tcase_add_test(tc_modify, test_rename);
    suite_add_tcase(s, tc_modify);

    SRunner *sr = srunner_create(s);
    srunner_set_fork_status(sr, CK_NOFORK);

    srunner_run_all(sr, CK_VERBOSE);
    int n_failed = srunner_ntests_failed(sr);
    printf("%d tests failed\n", n_failed);

    srunner_free(sr);
    return (n_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}