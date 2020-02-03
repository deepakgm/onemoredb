#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include "gmock/gmock.h"

using ::testing::Mock;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

using namespace std;

char dbfile_dir[PATH_MAX];

int main(int argc, char **argv) {
    if (getcwd(dbfile_dir, sizeof(dbfile_dir)) != NULL) {
        clog <<"current working dir:" << dbfile_dir << endl;
        strcat(dbfile_dir,"/test/test.bin");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


class DBFileTest : public ::testing::Test {
public:
    DBFile *dbFile;
protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        dbFile=new DBFile();
        remove(dbfile_dir);
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        delete dbFile;
        remove(dbfile_dir);
    }
};

//TEST_F(DBFileTest,openTest1){
//    //given
//    int res=0;
////    when
////    clog << "here" << f_path<< endl;
//    res=dbFile->Open("/home/gurpreet/Desktop/temp/t");
//    //assert
//    ASSERT_EQ(res,0);
//}
//
//TEST_F(DBFileTest,openTest2){
//    //given
//    int res=0;
////    when
//    res=dbFile->Open(nullptr);
//    //assert
//    ASSERT_EQ(res,0);
//}

TEST_F(DBFileTest,createTest1){
    //given
    int res=0;
//    when
//    clog << "here" << f_path<< endl;
    res=dbFile->Create(dbfile_dir,heap, nullptr);
    //assert
    ASSERT_EQ(res,0);
}

TEST_F(DBFileTest,createTest2){
    //given
    int res=0;
//    when
//    clog << "here" << f_path<< endl;
    res=dbFile->Create(dbfile_dir,sorted, nullptr);
    //assert
    ASSERT_EQ(res,1);
}



