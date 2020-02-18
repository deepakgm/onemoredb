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
protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        dbFile=new DBFile();
        dbFile->Create(dbfile_dir,heap,NULL);
        Schema nation (catalog_path, "nation");
        dbFile->Load(nation,table_path);
//        dbFile->MoveFirst();

    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
       delete dbFile;
       remove(dbfile_dir);
    }
};


//test BigQ::dumpSortedList
TEST_F(BigQTest,dumpSortedList1){
    vector<Record*> recordList;
    recordList.reserve(10);
    Record temp;

    while (dbFile->GetNext (temp) == 1) {
        Record* record = new Record();
        record->Copy(&temp);
        recordList.emplace_back(record);
    }

    BigQ bigQ;
    bigQ.file.Open(0, tempfile_path);

    ASSERT_EQ(bigQ.blockNum,0);
    bigQ.dumpSortedList(recordList);
    ASSERT_EQ(bigQ.blockNum,1);
}


//test BigQ::dumpSortedList
TEST_F(BigQTest,dumpSortedList2){
    vector<Record*> recordList;
    recordList.reserve(10);
    Record temp;

    while (dbFile->GetNext (temp) == 1) {
        Record* record = new Record();
        record->Copy(&temp);
        recordList.emplace_back(record);
    }

    BigQ bigQ;
    bigQ.file.Open(0, tempfile_path);

    ASSERT_EQ(bigQ.file.GetLength(),0);
    bigQ.dumpSortedList(recordList);
    ASSERT_EQ(bigQ.file.GetLength(),1);
}


//test BigQ::dumpSortedList
TEST_F(BigQTest,dumpSortedList3){
    vector<Record*> recordList;
    recordList.reserve(10);
    Record temp;

    while (dbFile->GetNext (temp) == 1) {
        Record* record = new Record();
        record->Copy(&temp);
        recordList.emplace_back(record);
    }

    BigQ bigQ;
    bigQ.file.Open(0, tempfile_path);

    ASSERT_NE(recordList.size(),0);
    bigQ.dumpSortedList(recordList);
    ASSERT_EQ(recordList.size(),0);
}

// close
TEST_F(BigQTest,close1){
    BigQ bigQ;
    bigQ.file.Open(0, tempfile_path);
    Pipe* output=new Pipe(100);
    bigQ.outPipe=output;
    bigQ.close();
    ASSERT_EQ(bigQ.file.Close(),0);
}


// open
TEST_F(BigQTest,open1){
    BigQ bigQ;
    bigQ.init();
    int file_length=bigQ.file.GetLength();
    ASSERT_EQ(file_length,1);
}