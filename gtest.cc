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

char dbfile_dir[PATH_MAX];
char table_path[PATH_MAX];
char catalog_path[PATH_MAX];
char tempfile_path[PATH_MAX];


int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(dbfile_dir, sizeof(dbfile_dir)) != NULL) {
        clog <<"current working dir:" << dbfile_dir << endl;
        strcpy(table_path,dbfile_dir);
        strcpy(catalog_path,dbfile_dir);
        strcat(dbfile_dir,"/test/test.bin");
        strcat(table_path,"/test/nation.tbl");
        strcat(catalog_path,"/test/catalog");
        strcat(catalog_path,"/test/tempfile");
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
    Pipe* inPipe;
    Pipe* outPipe;
protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        dbFile=new DBFile();
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
    OrderMaker orderMaker;
    BigQ bigQ (*inPipe, *outPipe, orderMaker, 2);
    bigQ.file.Open(0, tempfile_path);
    bigQ.file.AddPage(new Page(), -1);

    bigQ.dumpSortedList(recordList);
    cout<< "here" <<endl;

//    inPipe->ShutDown();
}









//// test DBFile::Create method for positive scenario
//TEST_F(BigQTest,createTest1){
//    int res=0;
//    res=dbFile->Create(dbfile_dir,heap, NULL);
//    ASSERT_EQ(res,0);
//}
//
//// test DBFile::Create method for negative scenario (with unsupported file_type)
//TEST_F(BigQTest,createTest2){
//    int res=0;
//    res=dbFile->Create(dbfile_dir,sorted, NULL);
//    ASSERT_EQ(res,1);
//}
//
//// test DBFile::Open method for negative scenario (with non-existing file)
//TEST_F(BigQTest,openTest1){
//    int res=0;
//    res=dbFile->Open(dbfile_dir);
//    ASSERT_EQ(res,1);
//}
//
//// test DBFile::Open method for positive scenario
//TEST_F(BigQTest,openTest2){
//    int res=0;
//    dbFile->Create(dbfile_dir,heap,NULL);
//    res=dbFile->Open(dbfile_dir);
//    ASSERT_EQ(res,0);
//}
//
//// test DBFile::Open method for positive scenario
//TEST_F(BigQTest,closeTest1){
//    int res=0;
//    dbFile->Create(dbfile_dir,heap,NULL);
//    res=dbFile->Close();
//    ASSERT_EQ(res,0);
//}
//
////// test DBFile::Close method for negative scenario (with DbFile that is not opened)
////TEST_F(BigQTest,closeTest2){
////    int res=0;
////    res=dbFile->Close();
////    //assert
////    ASSERT_EQ(res,1);
////}
//
//// test isDirty flag behaviour before and after executing DBFile::Add
//TEST_F(BigQTest,addTest){
//    bool res=false;
//
//    dbFile->Create(dbfile_dir,heap,NULL);
//
//    res=dbFile->GetIsDirty();
//    ASSERT_EQ(res, false);
//
//    Record record;
//    Schema schema ("catalog", "nation");
//
//    FILE *tableFile = fopen(table_path, "r");
//    record.SuckNextRecord(&schema,tableFile);
//
//    dbFile->Add(record);
//    res=dbFile->GetIsDirty();
//    ASSERT_EQ(res, true);
//}
//
//
//// test DBFile::GetNext method with no data
//TEST_F(BigQTest,getNextTest1){
//    int res=false;
//
//    dbFile->Create(dbfile_dir,heap,NULL);
//
//    Record record;
//    Schema schema ("catalog", "nation");
//
//
//    res=dbFile->GetNext(record);
//    ASSERT_EQ(res, 0);
//}
//
//// test DBFile::GetNext method with data
//TEST_F(BigQTest,getNextTest2){
//    int res=false;
//
//    dbFile->Create(dbfile_dir,heap,NULL);
//
//    Record record;
//    Schema schema (catalog_path, "nation");
//
//    FILE *tableFile = fopen(table_path, "r");
//    record.SuckNextRecord(&schema,tableFile);
//
//    dbFile->Add(record);
//    res=dbFile->GetNext(record);
//    ASSERT_EQ(res, 1);
//}
