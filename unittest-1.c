/*
 * file: testing.c
 * description: Test suite for file system read operations using libcheck.
 *              Assumes a pre-created file system image "test.img" with known contents.
 *
 * Contents of test.img (as provided):
 *
 *   Attributes (getattr values):
 *     "/"                         uid=0,    gid=0,    mode=040777, size=4096,  ctime=1565283152, mtime=1565283167
 *     "/file.1k"                uid=500,  gid=500,  mode=0100666, size=1000,  ctime=1565283152, mtime=1565283152
 *     "/file.10"                uid=500,  gid=500,  mode=0100666, size=10,    ctime=1565283152, mtime=1565283167
 *     "/dir-with-long-name"     uid=0,    gid=0,    mode=040777, size=4096,  ctime=1565283152, mtime=1565283167
 *     "/dir-with-long-name/file.12k+" uid=0, gid=500, mode=0100666, size=12289, ctime=1565283152, mtime=1565283167
 *     "/dir2"                   uid=500,  gid=500,  mode=040777, size=8192,  ctime=1565283152, mtime=1565283167
 *     "/dir2/twenty-seven-byte-file-name" uid=500, gid=500, mode=0100666, size=1000, ctime=1565283152, mtime=1565283167
 *     "/dir2/file.4k+"          uid=500,  gid=500,  mode=0100777, size=4098,  ctime=1565283152, mtime=1565283167
 *     "/dir3"                   uid=0,    gid=500,  mode=040777, size=4096,  ctime=1565283152, mtime=1565283167
 *     "/dir3/subdir"            uid=0,    gid=500,  mode=040777, size=4096,  ctime=1565283152, mtime=1565283167
 *     "/dir3/subdir/file.4k-"   uid=500,  gid=500,  mode=0100666, size=4095,  ctime=1565283152, mtime=1565283167
 *     "/dir3/subdir/file.8k-"   uid=500,  gid=500,  mode=0100666, size=8190,  ctime=1565283152, mtime=1565283167
 *     "/dir3/subdir/file.12k"   uid=500,  gid=500,  mode=0100666, size=12288, ctime=1565283152, mtime=1565283167
 *     "/dir3/file.12k-"         uid=0,    gid=500,  mode=0100777, size=12287, ctime=1565283152, mtime=1565283167
 *     "/file.8k+"               uid=500,  gid=500,  mode=0100666, size=8195,  ctime=1565283152, mtime=1565283167
 *
 *   Directory contents:
 *     "/" : "dir2", "dir3", "dir-with-long-name", "file.10", "file.1k", "file.8k+"
 *     "/dir2" : "twenty-seven-byte-file-name", "file.4k+"
 *     "/dir3" : "subdir", "file.12k-"
 *     "/dir3/subdir" : "file.4k-", "file.8k-", "file.12k"
 *     "/dir-with-long-name" : "file.12k+"
 *
 *   File checksums (CRC32) for read tests:
 *     "/file.1k":  1726121896,  length=1000
 *     "/file.10":  3766980606,  length=10
 *
 *   statvfs values for the test image:
 *     f_bsize = 4096, f_blocks = 400, f_bfree = 355, f_namemax = 27
 */

#define _FILE_OFFSET_BITS 64
#define FUSE_USE_VERSION 26

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <fuse.h>
#include <zlib.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <check.h>

#define BLOCK_SIZE 4096
#define MAX_NAME_LEN 27
#define MAX_PATH_LEN 10

/* Expected statvfs values */
#define EXPECTED_F_BSIZE 4096
#define EXPECTED_F_BLOCKS 400
#define EXPECTED_F_BFREE 355
#define EXPECTED_F_NAMEMAX 27

/* Expected values for files */
#define EXPECTED_FILE_1K_SIZE 1000
#define EXPECTED_FILE_1K_CRC 1726121896U

#define EXPECTED_FILE_10_SIZE 10
#define EXPECTED_FILE_10_CRC 3766980606U

/* Structure for expected getattr values */
struct fs_attr_expectation
{
    const char *path;
    uid_t uid;
    gid_t gid;
    mode_t mode;
    int size;
    time_t ctime;
    time_t mtime;
};

/* Table of expected attributes */
struct fs_attr_expectation attr_table[] = {
    {"/", 0, 0, 040777, 4096, 1565283152, 1565283167},
    {"/file.1k", 500, 500, 0100666, 1000, 1565283152, 1565283152},
    {"/file.10", 500, 500, 0100666, 10, 1565283152, 1565283167},
    {"/dir-with-long-name", 0, 0, 040777, 4096, 1565283152, 1565283167},
    {"/dir-with-long-name/file.12k+", 0, 500, 0100666, 12289, 1565283152, 1565283167},
    {"/dir2", 500, 500, 040777, 8192, 1565283152, 1565283167},
    {"/dir2/twenty-seven-byte-file-name", 500, 500, 0100666, 1000, 1565283152, 1565283167},
    {"/dir2/file.4k+", 500, 500, 0100777, 4098, 1565283152, 1565283167},
    {"/dir3", 0, 500, 040777, 4096, 1565283152, 1565283167},
    {"/dir3/subdir", 0, 500, 040777, 4096, 1565283152, 1565283167},
    {"/dir3/subdir/file.4k-", 500, 500, 0100666, 4095, 1565283152, 1565283167},
    {"/dir3/subdir/file.8k-", 500, 500, 0100666, 8190, 1565283152, 1565283167},
    {"/dir3/subdir/file.12k", 500, 500, 0100666, 12288, 1565283152, 1565283167},
    {"/dir3/file.12k-", 0, 500, 0100777, 12287, 1565283152, 1565283167},
    {"/file.8k+", 500, 500, 0100666, 8195, 1565283152, 1565283167},
    {NULL, 0, 0, 0, 0, 0, 0}};

/* Structure for tracking expected directory entries */
struct dir_entry
{
    const char *name;
    int seen;
};

/* Expected directory contents */
struct
{
    const char *dir;
    struct dir_entry entries[10]; /* adjust size if necessary */
} readdir_table[] = {
    {"/", {{"dir2", 0}, {"dir3", 0}, {"dir-with-long-name", 0}, {"file.10", 0}, {"file.1k", 0}, {"file.8k+", 0}, {NULL, 0}}},
    {"/dir2", {{"twenty-seven-byte-file-name", 0}, {"file.4k+", 0}, {NULL, 0}}},
    {"/dir3", {{"subdir", 0}, {"file.12k-", 0}, {NULL, 0}}},
    {"/dir3/subdir", {{"file.4k-", 0}, {"file.8k-", 0}, {"file.12k", 0}, {NULL, 0}}},
    {"/dir-with-long-name", {{"file.12k+", 0}, {NULL, 0}}},
    {NULL, {{NULL, 0}}}};

/* Extern declarations for file system operations and block_init */
extern struct fuse_operations fs_ops;
extern void block_init(char *file);

/* ---------------------- Filler Functions ---------------------- */

/* Filler for readdir tests (for successful directory listings) */
static int test_readdir_filler(void *ptr, const char *name, const struct stat *stbuf, off_t off)
{
    struct dir_entry *exp = (struct dir_entry *)ptr;
    fprintf(stderr, "[DEBUG] readdir: found \"%s\"\n", name);
    for (int j = 0; exp[j].name != NULL; j++)
    {
        if (strcmp(exp[j].name, name) == 0)
        {
            exp[j].seen = 1;
            fprintf(stderr, "[DEBUG] Marked \"%s\" as seen.\n", exp[j].name);
            break;
        }
    }
    return 0;
}

/* Filler for readdir error tests */
static int dummy_filler(void *ptr, const char *name, const struct stat *stbuf, off_t off)
{
    (void)ptr;
    (void)name;
    (void)stbuf;
    (void)off;
    return 0;
}

/* ---------------------- Test Cases ---------------------- */

/* Test fs_getattr for all expected files/directories */
START_TEST(test_getattr_all)
{
    struct fs_attr_expectation *e = attr_table;
    for (; e->path != NULL; e++)
    {
        struct stat st;
        int ret = fs_ops.getattr(e->path, &st);
        fprintf(stderr, "[DEBUG] getattr(\"%s\") returned %d\n", e->path, ret);
        ck_assert_int_eq(ret, 0);
        ck_assert_int_eq(st.st_size, e->size);
        ck_assert_int_eq(st.st_uid, e->uid);
        ck_assert_int_eq(st.st_gid, e->gid);
        ck_assert_int_eq(st.st_ctime, e->ctime);
        ck_assert_int_eq(st.st_mtime, e->mtime);
        ck_assert((st.st_mode & S_IFMT) == (e->mode & S_IFMT));
    }
}
END_TEST

/* Test fs_getattr error conditions */
START_TEST(test_getattr_errors)
{
    struct stat st;
    int ret;

    ret = fs_ops.getattr("/not-a-file", &st);
    fprintf(stderr, "[DEBUG] getattr(\"/not-a-file\") returned %d\n", ret);
    ck_assert_int_eq(ret, -ENOENT);

    ret = fs_ops.getattr("/file.1k/file.0", &st);
    fprintf(stderr, "[DEBUG] getattr(\"/file.1k/file.0\") returned %d\n", ret);
    ck_assert_int_eq(ret, -ENOTDIR);

    ret = fs_ops.getattr("/not-a-dir/file.0", &st);
    fprintf(stderr, "[DEBUG] getattr(\"/not-a-dir/file.0\") returned %d\n", ret);
    ck_assert_int_eq(ret, -ENOENT);

    ret = fs_ops.getattr("/dir2/not-a-file", &st);
    fprintf(stderr, "[DEBUG] getattr(\"/dir2/not-a-file\") returned %d\n", ret);
    ck_assert_int_eq(ret, -ENOENT);
}
END_TEST

/* Test fs_readdir for expected directory contents */
START_TEST(test_readdir_all)
{
    for (int i = 0; readdir_table[i].dir != NULL; i++)
    {
        struct dir_entry *expected = readdir_table[i].entries;
        int ret = fs_ops.readdir(readdir_table[i].dir, expected, test_readdir_filler, 0, NULL);
        fprintf(stderr, "[DEBUG] readdir(\"%s\") returned %d\n", readdir_table[i].dir, ret);
        ck_assert_int_eq(ret, 0);
        for (int j = 0; expected[j].name != NULL; j++)
        {
            ck_assert_msg(expected[j].seen, "Entry \"%s\" not found in directory \"%s\"",
                          expected[j].name, readdir_table[i].dir);
        }
    }
}
END_TEST

/* Test fs_readdir error conditions */
START_TEST(test_readdir_errors)
{
    struct dir_entry dummy[2] = {{"dummy", 0}, {NULL, 0}};
    int ret = fs_ops.readdir("/file.1k", dummy, dummy_filler, 0, NULL);
    fprintf(stderr, "[DEBUG] readdir(\"/file.1k\") returned %d (expected -ENOTDIR)\n", ret);
    ck_assert_int_eq(ret, -ENOTDIR);

    ret = fs_ops.readdir("/not-a-dir", dummy, dummy_filler, 0, NULL);
    fprintf(stderr, "[DEBUG] readdir(\"/not-a-dir\") returned %d (expected -ENOENT)\n", ret);
    ck_assert_int_eq(ret, -ENOENT);
}
END_TEST

/* Test fs_read: single big read for "/file.10" */
START_TEST(test_read_single_big)
{
    struct stat st;
    int ret = fs_ops.getattr("/file.10", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(st.st_size, EXPECTED_FILE_10_SIZE);

    char *buf = malloc(st.st_size);
    ck_assert_ptr_nonnull(buf);

    ret = fs_ops.read("/file.10", buf, st.st_size, 0, NULL);
    fprintf(stderr, "[DEBUG] read(\"/file.10\") returned %d bytes\n", ret);
    ck_assert_int_eq(ret, st.st_size);

    unsigned crc = crc32(0, (const Bytef *)buf, st.st_size);
    fprintf(stderr, "[DEBUG] CRC32 for \"/file.10\": 0x%x (expected 0x%x)\n", crc, EXPECTED_FILE_10_CRC);
    free(buf);
    ck_assert_uint_eq(crc, EXPECTED_FILE_10_CRC);
}
END_TEST

/* Test fs_read: multiple small reads (N=17) for "/file.1k" */
START_TEST(test_read_multiple_small)
{
    struct stat st;
    int ret = fs_ops.getattr("/file.1k", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(st.st_size, EXPECTED_FILE_1K_SIZE);

    size_t filesize = st.st_size;
    char *buf = malloc(filesize);
    ck_assert_ptr_nonnull(buf);
    size_t total_read = 0;
    size_t chunk = 17;
    while (total_read < filesize)
    {
        size_t to_read = (filesize - total_read < chunk) ? (filesize - total_read) : chunk;
        ret = fs_ops.read("/file.1k", buf + total_read, to_read, total_read, NULL);
        fprintf(stderr, "[DEBUG] read(\"/file.1k\", offset=%zu, len=%zu) returned %d bytes\n", total_read, to_read, ret);
        ck_assert_int_eq(ret, to_read);
        total_read += ret;
    }
    ck_assert_int_eq(total_read, filesize);
    unsigned crc = crc32(0, (const Bytef *)buf, filesize);
    fprintf(stderr, "[DEBUG] CRC32 for \"/file.1k\": 0x%x (expected 0x%x)\n", crc, EXPECTED_FILE_1K_CRC);
    free(buf);
    ck_assert_uint_eq(crc, EXPECTED_FILE_1K_CRC);
}
END_TEST

/* Test fs_statfs */
START_TEST(test_statfs)
{
    struct statvfs sv;
    int ret = fs_ops.statfs("/", &sv);
    fprintf(stderr, "[DEBUG] statfs(\"/\") returned %d\n", ret);
    ck_assert_int_eq(ret, 0);
    fprintf(stderr, "[DEBUG] statfs: f_bsize=%lu, f_blocks=%lu, f_bfree=%lu, f_namemax=%lu\n",
            sv.f_bsize, sv.f_blocks, sv.f_bfree, sv.f_namemax);
    ck_assert_int_eq(sv.f_bsize, EXPECTED_F_BSIZE);
    ck_assert_int_eq(sv.f_blocks, EXPECTED_F_BLOCKS);
    ck_assert_int_eq(sv.f_bfree, EXPECTED_F_BFREE);
    ck_assert_int_eq(sv.f_namemax, EXPECTED_F_NAMEMAX);
}
END_TEST

/* Test fs_chmod: change permissions for "/file.10" and "/dir2" */
START_TEST(test_chmod)
{
    struct stat st;
    int ret;

    ret = fs_ops.chmod("/file.10", 0100644);
    fprintf(stderr, "[DEBUG] chmod(\"/file.10\", 0100644) returned %d\n", ret);
    ck_assert_int_eq(ret, 0);
    ret = fs_ops.getattr("/file.10", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert((st.st_mode & S_IFMT) == 0100000);
    ck_assert((st.st_mode & 0777) == 0644);

    ret = fs_ops.chmod("/dir2", 040755);
    fprintf(stderr, "[DEBUG] chmod(\"/dir2\", 040755) returned %d\n", ret);
    ck_assert_int_eq(ret, 0);
    ret = fs_ops.getattr("/dir2", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert((st.st_mode & S_IFMT) == S_IFDIR);
    ck_assert((st.st_mode & 0777) == 0755);
}
END_TEST

/* Test fs_rename: rename a file and a directory.
   Note: These tests modify the disk image, so regenerate the image before running.
*/
START_TEST(test_rename)
{
    int ret;
    struct stat st;

    ret = fs_ops.rename("/file.10", "/file.10.renamed");
    fprintf(stderr, "[DEBUG] rename(\"/file.10\", \"/file.10.renamed\") returned %d\n", ret);
    ck_assert_int_eq(ret, 0);
    ret = fs_ops.getattr("/file.10.renamed", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert_int_eq(st.st_size, EXPECTED_FILE_10_SIZE);
    ret = fs_ops.getattr("/file.10", &st);
    ck_assert_int_eq(ret, -ENOENT);

    ret = fs_ops.rename("/dir2", "/dir2-renamed");
    fprintf(stderr, "[DEBUG] rename(\"/dir2\", \"/dir2-renamed\") returned %d\n", ret);
    ck_assert_int_eq(ret, 0);
    ret = fs_ops.getattr("/dir2-renamed", &st);
    ck_assert_int_eq(ret, 0);
    ck_assert((st.st_mode & S_IFMT) == S_IFDIR);
    ret = fs_ops.getattr("/dir2-renamed/twenty-seven-byte-file-name", &st);
    ck_assert_int_eq(ret, 0);
}
END_TEST

/* ---------------------- Suite Setup ---------------------- */
Suite *fs_suite(void)
{
    Suite *s = suite_create("fs5600_FSReadAndModFunctions");
    TCase *tc = tcase_create("fs_tests");
    tcase_add_test(tc, test_getattr_all);
    tcase_add_test(tc, test_getattr_errors);
    tcase_add_test(tc, test_readdir_all);
    tcase_add_test(tc, test_readdir_errors);
    tcase_add_test(tc, test_read_single_big);
    tcase_add_test(tc, test_read_multiple_small);
    tcase_add_test(tc, test_statfs);
    tcase_add_test(tc, test_chmod);
    tcase_add_test(tc, test_rename);
    suite_add_tcase(s, tc);
    return s;
}

/* ---------------------- Main Function ---------------------- */
int main(int argc, char **argv)
{
    /* Regenerate the test image if needed. Uncomment the following line if required.
       system("python gen-disk.py -q disk1.in test.img"); */
    fprintf(stderr, "[DEBUG] Initializing test image...\n");
    block_init("test.img");
    fs_ops.init(NULL);

    Suite *s = fs_suite();
    SRunner *sr = srunner_create(s);
    srunner_set_fork_status(sr, CK_NOFORK);
    srunner_run_all(sr, CK_VERBOSE);
    int n_failed = srunner_ntests_failed(sr);
    fprintf(stderr, "[DEBUG] Total tests failed: %d\n", n_failed);
    srunner_free(sr);
    return (n_failed == 0) ? EXIT_SUCCESS : EXIT_FAILURE;
}
