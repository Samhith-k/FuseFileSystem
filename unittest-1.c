/*
 * file:        testing.c
 * description: libcheck test suite for file system read functions
 */

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <check.h>
#include <errno.h>
#include <fuse.h>
#include <zlib.h>
#include <sys/stat.h>
#include <sys/statvfs.h>

#define BLOCK_SIZE 4096
#define MAX_NAME_LEN 27
#define MAX_PATH_LEN 10

/* Expected checksum for /file.txt.
   Replace 0x12345678 with the actual expected value for your test image. */
#define EXPECTED_FILE_TXT_CRC 0x12345678
/* Expected size for /file.txt (in bytes).
   Replace 100 with the actual size from your test image. */
#define EXPECTED_FILE_TXT_SIZE 100

/* A structure for tracking expected entries in a directory */
struct dir_entry
{
    const char *name;
    int seen;
};

/* Global declarations for fs_ops operations and block_init, provided by your FS code */
extern struct fuse_operations fs_ops;
extern void block_init(char *file);

/*
 * Test for fs_getattr on "/" (root directory).  We assume that the root directory
 * exists and its mode indicates a directory.
 */
START_TEST(test_getattr_root)
{
    struct stat st;
    int ret = fs_ops.getattr("/", &st);
    ck_assert_int_eq(ret, 0);
    /* Check that the mode indicates a directory.
       Here we assume S_IFDIR is defined in sys/stat.h. */
    ck_assert(st.st_mode & S_IFDIR);
}
END_TEST

/*
 * Test for fs_getattr on a known file "/file.txt". We assume that file.txt exists,
 * is not a directory, and has a known file size.
 */
START_TEST(test_getattr_file)
{
    struct stat st;
    int ret = fs_ops.getattr("/file.txt", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert(!(st.st_mode & S_IFDIR));
    ck_assert_int_eq(st.st_size, EXPECTED_FILE_TXT_SIZE);
}
END_TEST

/*
 * Test for fs_readdir on "/" (root directory).
 * We expect the root directory to include at least "file.txt" and "dir2".
 */
START_TEST(test_readdir_root)
{
    /* Define an array of expected names. Adjust these names to match your test image. */
    struct dir_entry expected[] = {
        {"file.txt", 0},
        {"dir2", 0},
        {NULL, 0}};

    /* Filler function that marks an entry as seen if found */
    int test_filler(void *ptr, const char *name, const struct stat *stbuf, off_t off)
    {
        struct dir_entry *exp = (struct dir_entry *)ptr;
        for (int i = 0; exp[i].name != NULL; i++)
        {
            if (strcmp(exp[i].name, name) == 0)
            {
                exp[i].seen = 1;
                break;
            }
        }
        return 0;
    }

    int ret = fs_ops.readdir("/", expected, test_filler, 0, NULL);
    ck_assert_int_eq(ret, 0);
    for (int i = 0; expected[i].name != NULL; i++)
    {
        ck_assert_msg(expected[i].seen, "Entry \"%s\" not found in readdir output", expected[i].name);
    }
}
END_TEST

/*
 * Test for fs_read on "/file.txt".
 * We attempt to read the entire file and then compute a CRC32 checksum on the data.
 */
START_TEST(test_read_file)
{
    /* First, get file attributes to know its size */
    struct stat st;
    int ret = fs_ops.getattr("/file.txt", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_gt(st.st_size, 0);

    size_t filesize = st.st_size;
    char *buf = malloc(filesize);
    ck_assert_ptr_nonnull(buf);

    ret = fs_ops.read("/file.txt", buf, filesize, 0, NULL);
    ck_assert_int_eq(ret, filesize);

    /* Compute CRC32 checksum over the file data */
    unsigned crc = crc32(0, (const Bytef *)buf, filesize);
    free(buf);
    ck_assert_uint_eq(crc, EXPECTED_FILE_TXT_CRC);
}
END_TEST

/*
 * Test for fs_statfs on "/" (root directory).
 * We check that f_bsize is equal to BLOCK_SIZE, and that f_namemax equals MAX_NAME_LEN.
 */
START_TEST(test_statfs)
{
    struct statvfs sv;
    int ret = fs_ops.statfs("/", &sv);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(sv.f_bsize, BLOCK_SIZE);
    ck_assert_int_eq(sv.f_namemax, MAX_NAME_LEN);
    /* We could also perform additional checks such as verifying that f_blocks and f_bfree are nonzero */
}
END_TEST

/*
 * Main test suite that registers all the tests.
 */
Suite *fs_suite(void)
{
    Suite *s = suite_create("fs5600_ReadFunctions");
    TCase *tc = tcase_create("read_tests");

    tcase_add_test(tc, test_getattr_root);
    tcase_add_test(tc, test_getattr_file);
    tcase_add_test(tc, test_readdir_root);
    tcase_add_test(tc, test_read_file);
    tcase_add_test(tc, test_statfs);

    suite_add_tcase(s, tc);
    return s;
}

int main(int argc, char **argv)
{
    /* Initialize the block device using your test image */
    block_init("test.img");
    /* Initialize the file system (reads superblock, bitmap, inodes, etc.) */
    fs_ops.init(NULL);

    Suite *s = fs_suite();
    SRunner *sr = srunner_create(s);
    srunner_set_fork_status(sr, CK_NOFORK);

    srunner_run_all(sr, CK_VERBOSE);
    int n_failed = srunner_ntests_failed(sr);
    printf("%d tests failed\n", n_failed);
    srunner_free(sr);
    return (n_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
