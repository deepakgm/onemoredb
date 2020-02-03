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

const char *dbfile_dir = "/home/gurpreet/Desktop/dbi/onemoredb/test";
const char *tpch_dir ="/home/gurpreet/Desktop/test/"; // dir where dbgen tpch files (extension *.tbl) can be found
const char *catalog_path = "/home/gurpreet/Desktop/dbi/onemoredb/catalog"; // full path of the catalog file

//class MockDBFile : public DBFile
//{
//public:
//    MOCK_METHOD0(void,MoveFirst, ());
//
//};

int main(int argc, char **argv) {
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
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        delete dbFile;
    }
};

TEST_F(DBFileTest,openTest1){
    //given
    int res=0;
//    when
//    clog << "here" << f_path<< endl;
    res=dbFile->Open("/home/gurpreet/Desktop/temp/t");
    //assert
    ASSERT_EQ(res,0);
}

TEST_F(DBFileTest,openTest2){
    //given
    int res=0;
//    when
    res=dbFile->Open(nullptr);
    //assert
    ASSERT_EQ(res,0);
}

TEST_F(DBFileTest,createTest1){
    //given
    int res=0;
//    when
//    clog << "here" << f_path<< endl;
    res=dbFile->Create("/home/gurpreet/Desktop/temp/t",heap, nullptr);
    //assert
    ASSERT_EQ(res,0);
}

TEST_F(DBFileTest,createTest2){
    //given
    int res=0;
//    when
//    clog << "here" << f_path<< endl;
    res=dbFile->Create("/home/gurpreet/Desktop/temp/t",sorted, nullptr);
    //assert
    ASSERT_EQ(res,1);
}

//TEST_F(DBFileTest,openTest3){
//    MockDBFile mockDbFile;
//
////    EXPECT_CALL(mockDbFile,MoveFirst() )
////            .Times(1);
////    mockDbFile.MoveFirst();
//
//}


