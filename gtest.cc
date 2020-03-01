#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include "BigQ.h"
#include "gmock/gmock.h"

using ::testing::Mock;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

using namespace std;

char cur_dir[PATH_MAX];
char dbfile_dir[PATH_MAX];
char table_path[PATH_MAX];
char catalog_path[PATH_MAX];
char tempfile_path[PATH_MAX];


int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(cur_dir, sizeof(cur_dir)) != NULL) {
        clog <<"current working dir:" << cur_dir << endl;
        strcpy(dbfile_dir,cur_dir);
        strcpy(table_path,cur_dir);
        strcpy(catalog_path,cur_dir);
        strcpy(tempfile_path,cur_dir);
        strcat(dbfile_dir,"/test/test.bin");
        strcat(table_path,"/test/nation.tbl");
        strcat(catalog_path,"/test/catalog");
        strcat(tempfile_path,"/test/tempfile");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


class BigQTest : public ::testing::Test {
public:
    DBFile *dbFile;
    Schema nation (catalog_path, "nation");

protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        dbFile=new DBFile();
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        delete dbFile;
        remove(dbfile_dir);
    }
};


//test DBFile::Create
TEST_F(DbTest,dumpSortedList1){
//    dbFile->Create(dbfile_dir,heap,NULL);
}