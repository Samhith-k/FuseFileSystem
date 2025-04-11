/*
 * file: unittest-2.c
 * description: Unit tests for Part 2 (write operations)
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
#include <utime.h>

/* Mock fuse_get_context for testing */
static struct fuse_context ctx = {.uid = 500, .gid = 500};
struct fuse_context *fuse_get_context(void)
{
    return &ctx;
}

extern struct fuse_operations fs_ops;
extern void block_init(char *file);

/* Helper function to create test data */
static char *create_test_data(size_t size)
{
    char *data = malloc(size);
    if (!data)
        return NULL;

    for (size_t i = 0; i < size; i++)
    {
        data[i] = 'A' + (i % 26); /* Pattern A-Z */
    }

    return data;
}

/* Helper callback function for directory testing */
static int test_readdir_callback(void *buf, const char *name, const struct stat *stbuf, off_t off)
{
    struct
    {
        int count;
        char seen[10][30];
    } *list = buf;
    if (strcmp(name, ".") != 0 && strcmp(name, "..") != 0)
    {
        strncpy(list->seen[list->count], name, 29);
        list->count++;
    }
    return 0;
}

/****** BASIC TESTS ******/

/* Test creating and getting attributes of a file */
START_TEST(test_create_getattr)
{
    int rv;
    struct stat sb;

    /* Create a simple file */
    rv = fs_ops.create("/testfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Verify file exists with correct attributes */
    rv = fs_ops.getattr("/testfile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 0);
    ck_assert_int_eq(sb.st_mode & 0777, 0644);
    ck_assert(S_ISREG(sb.st_mode));
    ck_assert_int_eq(sb.st_uid, 500);
    ck_assert_int_eq(sb.st_gid, 500);
}
END_TEST

/* Test creating and listing directory contents */
START_TEST(test_mkdir_readdir)
{
    int rv;
    struct stat sb;
    struct
    {
        int count;
        char seen[10][30];
    } dir_list = {0};

    /* Create a directory */
    rv = fs_ops.mkdir("/testdir", 0755);
    ck_assert_int_eq(rv, 0);

    /* Verify directory exists with correct attributes */
    rv = fs_ops.getattr("/testdir", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_mode & 0777, 0755);
    ck_assert(S_ISDIR(sb.st_mode));

    /* Create some files in the directory */
    rv = fs_ops.create("/testdir/file1", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);
    rv = fs_ops.create("/testdir/file2", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Check directory contents */
    rv = fs_ops.readdir("/testdir", &dir_list, test_readdir_callback, 0, NULL);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(dir_list.count, 2);

    /* Verify both files are in the listing */
    int found_file1 = 0, found_file2 = 0;
    for (int i = 0; i < dir_list.count; i++)
    {
        if (strcmp(dir_list.seen[i], "file1") == 0)
            found_file1 = 1;
        if (strcmp(dir_list.seen[i], "file2") == 0)
            found_file2 = 1;
    }
    ck_assert_int_eq(found_file1, 1);
    ck_assert_int_eq(found_file2, 1);
}
END_TEST

/* Test writing to and reading from a file */
START_TEST(test_write_read)
{
    int rv;
    struct stat sb;
    char *test_data = create_test_data(4000); /* 4000 bytes of test data */
    char read_buffer[5000];                   /* Larger than test data */

    /* Create a file */
    rv = fs_ops.create("/writefile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write test data */
    rv = fs_ops.write("/writefile", test_data, 4000, 0, NULL);
    ck_assert_int_eq(rv, 4000);

    /* Verify file size is updated */
    rv = fs_ops.getattr("/writefile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 4000);

    /* Read back the data */
    rv = fs_ops.read("/writefile", read_buffer, 5000, 0, NULL);
    ck_assert_int_eq(rv, 4000);

    /* Verify data integrity */
    ck_assert_int_eq(memcmp(test_data, read_buffer, 4000), 0);

    free(test_data);
}
END_TEST

/* Test writing data in chunks and across block boundaries */
START_TEST(test_write_chunks)
{
    int rv;
    struct stat sb;
    char *test_data = create_test_data(8200); /* Spans at least 2 blocks */
    char read_buffer[9000];

    /* Create a file */
    rv = fs_ops.create("/chunkfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write in three chunks:
     * 1. First 4000 bytes at offset 0
     * 2. 2000 bytes at offset 4000
     * 3. 2200 bytes at offset 6000
     */
    rv = fs_ops.write("/chunkfile", test_data, 4000, 0, NULL);
    ck_assert_int_eq(rv, 4000);

    rv = fs_ops.write("/chunkfile", test_data + 4000, 2000, 4000, NULL);
    ck_assert_int_eq(rv, 2000);

    rv = fs_ops.write("/chunkfile", test_data + 6000, 2200, 6000, NULL);
    ck_assert_int_eq(rv, 2200);

    /* Verify file size */
    rv = fs_ops.getattr("/chunkfile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 8200);

    /* Read back entire file */
    rv = fs_ops.read("/chunkfile", read_buffer, 9000, 0, NULL);
    ck_assert_int_eq(rv, 8200);

    /* Verify data integrity */
    ck_assert_int_eq(memcmp(test_data, read_buffer, 8200), 0);

    free(test_data);
}
END_TEST

/* Test unlinking (deleting) a file */
START_TEST(test_unlink)
{
    int rv;
    struct stat sb;

    /* Create a file */
    rv = fs_ops.create("/unlinkme", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write some data to it */
    char *test_data = create_test_data(1000);
    rv = fs_ops.write("/unlinkme", test_data, 1000, 0, NULL);
    ck_assert_int_eq(rv, 1000);
    free(test_data);

    /* Unlink (delete) the file */
    rv = fs_ops.unlink("/unlinkme");
    ck_assert_int_eq(rv, 0);

    /* Verify file no longer exists */
    rv = fs_ops.getattr("/unlinkme", &sb);
    ck_assert_int_eq(rv, -ENOENT);
}
END_TEST

/* Test removing a directory */
START_TEST(test_rmdir)
{
    int rv;
    struct stat sb;

    /* Create a directory */
    rv = fs_ops.mkdir("/rmdirtest", 0755);
    ck_assert_int_eq(rv, 0);

    /* Verify it exists */
    rv = fs_ops.getattr("/rmdirtest", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert(S_ISDIR(sb.st_mode));

    /* Remove it */
    rv = fs_ops.rmdir("/rmdirtest");
    ck_assert_int_eq(rv, 0);

    /* Verify it's gone */
    rv = fs_ops.getattr("/rmdirtest", &sb);
    ck_assert_int_eq(rv, -ENOENT);
}
END_TEST

/* Test truncating a file */
START_TEST(test_truncate)
{
    int rv;
    struct stat sb;
    char *test_data = create_test_data(4000);
    char read_buffer[100]; /* Small buffer for after truncate */

    /* Create and write to a file */
    rv = fs_ops.create("/truncfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.write("/truncfile", test_data, 4000, 0, NULL);
    ck_assert_int_eq(rv, 4000);
    free(test_data);

    /* Verify size */
    rv = fs_ops.getattr("/truncfile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 4000);

    /* Truncate to zero */
    rv = fs_ops.truncate("/truncfile", 0);
    ck_assert_int_eq(rv, 0);

    /* Verify size is now 0 */
    rv = fs_ops.getattr("/truncfile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, 0);

    /* Read should return 0 bytes */
    rv = fs_ops.read("/truncfile", read_buffer, 100, 0, NULL);
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Test setting file times */
START_TEST(test_utime)
{
    int rv;
    struct stat sb;
    struct utimbuf ut;

    /* Create a file */
    rv = fs_ops.create("/timefile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Set specific times */
    ut.actime = 12345;
    ut.modtime = 67890;

    rv = fs_ops.utime("/timefile", &ut);
    ck_assert_int_eq(rv, 0);

    /* Verify times were set */
    rv = fs_ops.getattr("/timefile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_mtime, 67890);
    /* ctime may be set to mtime or to current time, check your implementation */
    ck_assert(sb.st_ctime == 67890 || sb.st_ctime > 0);
}
END_TEST

/****** ERROR HANDLING TESTS ******/

/* Test error handling for create */
START_TEST(test_create_errors)
{
    int rv;

    /* Test creating file with parent directory that doesn't exist */
    rv = fs_ops.create("/nonexistent/file", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a file to test duplicate name */
    rv = fs_ops.create("/duptest", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to create another file with the same name */
    rv = fs_ops.create("/duptest", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, -EEXIST);

    /* Create a file to use as intermediate path element */
    rv = fs_ops.create("/pathtest", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to create file with file as parent */
    rv = fs_ops.create("/pathtest/subfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, -ENOTDIR);
}
END_TEST

/* Test error handling for mkdir */
START_TEST(test_mkdir_errors)
{
    int rv;

    /* Test making directory with parent that doesn't exist */
    rv = fs_ops.mkdir("/nonexistent/dir", 0755);
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a directory to test duplicate */
    rv = fs_ops.mkdir("/dupdir", 0755);
    ck_assert_int_eq(rv, 0);

    /* Try to create directory with same name */
    rv = fs_ops.mkdir("/dupdir", 0755);
    ck_assert_int_eq(rv, -EEXIST);

    /* Create a file to use as intermediate path element */
    rv = fs_ops.create("/filepathtest", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to create directory with file as parent */
    rv = fs_ops.mkdir("/filepathtest/subdir", 0755);
    ck_assert_int_eq(rv, -ENOTDIR);
}
END_TEST

/* Test error handling for unlink */
START_TEST(test_unlink_errors)
{
    int rv;

    /* Try to unlink non-existent file */
    rv = fs_ops.unlink("/no-such-file");
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a directory */
    rv = fs_ops.mkdir("/dirunlink", 0755);
    ck_assert_int_eq(rv, 0);

    /* Try to unlink a directory - should fail with EISDIR */
    rv = fs_ops.unlink("/dirunlink");
    ck_assert_int_eq(rv, -EISDIR);

    /* Clean up - remove directory correctly */
    rv = fs_ops.rmdir("/dirunlink");
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Test error handling for rmdir */
START_TEST(test_rmdir_errors)
{
    int rv;

    /* Try to rmdir non-existent directory */
    rv = fs_ops.rmdir("/no-such-dir");
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a file */
    rv = fs_ops.create("/filermdir", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to rmdir a file - should fail with ENOTDIR */
    rv = fs_ops.rmdir("/filermdir");
    ck_assert_int_eq(rv, -ENOTDIR);

    /* Create a directory */
    rv = fs_ops.mkdir("/nonemptydir", 0755);
    ck_assert_int_eq(rv, 0);

    /* Create a file inside this directory */
    rv = fs_ops.create("/nonemptydir/file", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to rmdir non-empty directory - should fail with ENOTEMPTY */
    rv = fs_ops.rmdir("/nonemptydir");
    ck_assert_int_eq(rv, -ENOTEMPTY);

    /* Clean up */
    rv = fs_ops.unlink("/nonemptydir/file");
    ck_assert_int_eq(rv, 0);
    rv = fs_ops.rmdir("/nonemptydir");
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Test error handling for write */
START_TEST(test_write_errors)
{
    int rv;
    char data[100] = "test data";

    /* Try to write to non-existent file */
    rv = fs_ops.write("/no-such-file", data, 100, 0, NULL);
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a directory */
    rv = fs_ops.mkdir("/writedir", 0755);
    ck_assert_int_eq(rv, 0);

    /* Try to write to a directory */
    rv = fs_ops.write("/writedir", data, 100, 0, NULL);
    ck_assert_int_eq(rv, -EISDIR);

    /* Create a file for testing offset error */
    rv = fs_ops.create("/offsetfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write 10 bytes */
    rv = fs_ops.write("/offsetfile", data, 10, 0, NULL);
    ck_assert_int_eq(rv, 10);

    /* Try to write with offset beyond file size - should return EINVAL */
    rv = fs_ops.write("/offsetfile", data, 10, 100, NULL);
    ck_assert_int_eq(rv, -EINVAL);
}
END_TEST

/* Test error handling for truncate */
START_TEST(test_truncate_errors)
{
    int rv;

    /* Try to truncate non-existent file */
    rv = fs_ops.truncate("/no-such-file", 0);
    ck_assert_int_eq(rv, -ENOENT);

    /* Create a directory */
    rv = fs_ops.mkdir("/truncdir", 0755);
    ck_assert_int_eq(rv, 0);

    /* Try to truncate a directory */
    rv = fs_ops.truncate("/truncdir", 0);
    ck_assert_int_eq(rv, -EISDIR);

    /* Create a file */
    rv = fs_ops.create("/truncfile2", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Try to truncate to non-zero length - should return EINVAL */
    rv = fs_ops.truncate("/truncfile2", 100);
    ck_assert_int_eq(rv, -EINVAL);
}
END_TEST

/****** COMPLEX TESTS ******/

/* Test multilevel directory creation and operations */
START_TEST(test_multilevel_dirs)
{
    int rv;
    struct stat sb;

    /* Create nested directory structure */
    rv = fs_ops.mkdir("/level1", 0755);
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.mkdir("/level1/level2", 0755);
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.mkdir("/level1/level2/level3", 0755);
    ck_assert_int_eq(rv, 0);

    /* Create a file in the deepest directory */
    rv = fs_ops.create("/level1/level2/level3/deepfile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write to the file */
    char data[100] = "Deep file test data";
    rv = fs_ops.write("/level1/level2/level3/deepfile", data, strlen(data), 0, NULL);
    ck_assert_int_eq(rv, strlen(data));

    /* Verify file exists and has correct size */
    rv = fs_ops.getattr("/level1/level2/level3/deepfile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, strlen(data));

    /* Clean up - remove directories from deepest to shallowest */
    rv = fs_ops.unlink("/level1/level2/level3/deepfile");
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.rmdir("/level1/level2/level3");
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.rmdir("/level1/level2");
    ck_assert_int_eq(rv, 0);

    rv = fs_ops.rmdir("/level1");
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Test block allocation for large files */
START_TEST(test_large_file)
{
    int rv;
    struct stat sb;
    struct statvfs st_before, st_after;

    /* Get initial free blocks */
    rv = fs_ops.statfs("/", &st_before);
    ck_assert_int_eq(rv, 0);

    /* Create a large file - 20K (spans multiple blocks) */
    size_t size = 20 * 1024;
    char *large_data = create_test_data(size);

    rv = fs_ops.create("/largefile", 0644 | S_IFREG, NULL);
    ck_assert_int_eq(rv, 0);

    /* Write data in chunks to ensure block allocation works */
    for (size_t offset = 0; offset < size; offset += 4096)
    {
        size_t chunk_size = (offset + 4096 > size) ? (size - offset) : 4096;
        rv = fs_ops.write("/largefile", large_data + offset, chunk_size, offset, NULL);
        ck_assert_int_eq(rv, chunk_size);
    }

    /* Verify file size */
    rv = fs_ops.getattr("/largefile", &sb);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(sb.st_size, size);

    /* Check that blocks were allocated */
    rv = fs_ops.statfs("/", &st_after);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_gt(st_before.f_bfree, st_after.f_bfree);

    /* Free the large data */
    free(large_data);

    /* Unlink the file */
    rv = fs_ops.unlink("/largefile");
    ck_assert_int_eq(rv, 0);

    /* Verify blocks were freed */
    struct statvfs st_final;
    rv = fs_ops.statfs("/", &st_final);
    ck_assert_int_eq(rv, 0);
    ck_assert_int_eq(st_before.f_bfree, st_final.f_bfree);
}
END_TEST

/* Test stress test with many small files */
START_TEST(test_many_files)
{
    int rv;
    struct stat sb;
    char path[100];

    /* Create a directory */
    rv = fs_ops.mkdir("/manyfiles", 0755);
    ck_assert_int_eq(rv, 0);

    /* Create 50 small files */
    for (int i = 0; i < 50; i++)
    {
        sprintf(path, "/manyfiles/file%d", i);
        rv = fs_ops.create(path, 0644 | S_IFREG, NULL);
        ck_assert_int_eq(rv, 0);

        /* Write a small amount of data */
        char data[20];
        sprintf(data, "Data for file %d", i);
        rv = fs_ops.write(path, data, strlen(data), 0, NULL);
        ck_assert_int_eq(rv, strlen(data));
    }

    /* Verify all files exist */
    for (int i = 0; i < 50; i++)
    {
        sprintf(path, "/manyfiles/file%d", i);
        rv = fs_ops.getattr(path, &sb);
        ck_assert_int_eq(rv, 0);

        /* Verify the expected size (strlen of our data) */
        char expected_data[20];
        sprintf(expected_data, "Data for file %d", i);
        ck_assert_int_eq(sb.st_size, strlen(expected_data));
    }

    /* Clean up - delete all files */
    for (int i = 0; i < 50; i++)
    {
        sprintf(path, "/manyfiles/file%d", i);
        rv = fs_ops.unlink(path);
        ck_assert_int_eq(rv, 0);
    }

    /* Remove the directory */
    rv = fs_ops.rmdir("/manyfiles");
    ck_assert_int_eq(rv, 0);
}
END_TEST

/* Main function */
int main(int argc, char **argv)
{
    /* Regenerate test disk each time */
    fprintf(stderr, "[unittest-2] Initializing test2.img...\n");
    system("python gen-disk.py -q disk2.in test2.img");

    block_init("test2.img");
    fs_ops.init(NULL);

    Suite *s = suite_create("fs5600_part2");

    /* Write Operations */
    TCase *tc_write_ops = tcase_create("write_ops");
    tcase_add_test(tc_write_ops, test_create_getattr);
    tcase_add_test(tc_write_ops, test_mkdir_readdir);
    tcase_add_test(tc_write_ops, test_write_read);
    tcase_add_test(tc_write_ops, test_write_chunks);
    tcase_add_test(tc_write_ops, test_unlink);
    tcase_add_test(tc_write_ops, test_rmdir);
    tcase_add_test(tc_write_ops, test_truncate);
    tcase_add_test(tc_write_ops, test_utime);

    /* Error Handling */
    tcase_add_test(tc_write_ops, test_create_errors);
    tcase_add_test(tc_write_ops, test_mkdir_errors);
    tcase_add_test(tc_write_ops, test_unlink_errors);
    tcase_add_test(tc_write_ops, test_rmdir_errors);
    tcase_add_test(tc_write_ops, test_write_errors);
    tcase_add_test(tc_write_ops, test_truncate_errors);

    /* Complex Operations */
    tcase_add_test(tc_write_ops, test_multilevel_dirs);
    tcase_add_test(tc_write_ops, test_large_file);
    tcase_add_test(tc_write_ops, test_many_files);

    suite_add_tcase(s, tc_write_ops);

    SRunner *sr = srunner_create(s);
    srunner_set_fork_status(sr, CK_NOFORK);

    srunner_run_all(sr, CK_VERBOSE);
    int n_failed = srunner_ntests_failed(sr);
    printf("%d tests failed\n", n_failed);

    srunner_free(sr);
    return (n_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}