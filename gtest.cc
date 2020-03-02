#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include "BigQ.h"
#include "gmock/gmock.h"
#include "Meta.h"
#include <cstring>

using ::testing::Mock;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

using namespace std;

char cur_dir1[PATH_MAX];
char dbfile_dir[PATH_MAX];
char table_path[PATH_MAX];
char catalog_path[PATH_MAX];
char tempfile_path[PATH_MAX];


int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(cur_dir1, sizeof(cur_dir1)) != NULL) {
        clog <<"current working dir:" << cur_dir1 << endl;
        strcpy(dbfile_dir,cur_dir1);
        strcpy(table_path,cur_dir1);
        strcpy(catalog_path,cur_dir1);
        strcpy(tempfile_path,cur_dir1);
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
    DBFile *dbFile,*dbFile1;
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

//test MetaContent
TEST_F(BigQTest,metacontent1){
    //orderMaker object
    OrderMaker* orderMaker=new(OrderMaker);
    orderMaker->numAtts=1;
    orderMaker->whichTypes[0]=String;
    orderMaker->whichAtts[0]=6;
    //startup
    SortInfo* sortInfo=new SortInfo;
    sortInfo->runLength=2;
    sortInfo->myOrder=orderMaker;
    //generic dbfile
    GenericDBFile *myInternalVar;
    myInternalVar = new Heap();
    //create metafile and initialize obj
    myInternalVar->Create(tempfile_path, heap, sortInfo);

    MetaInfo metaInfo = GetMetaInfo();
    fType type_file;
    type_file = metaInfo.fileType;
//    cout<<type_file<<endl;
    ASSERT_EQ(type_file,heap);
//    bigQ.dumpSortedList(recordList);
//    ASSERT_EQ(bigQ.blockNum,1);
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

//mode test before writing/reading
TEST_F(BigQTest,modetest1){
    OrderMaker* orderMaker=new(OrderMaker);
    orderMaker->numAtts=1;
    orderMaker->whichTypes[0]=String;
    orderMaker->whichAtts[0]=6;
    //startup
    SortInfo* sortInfo=new SortInfo;
    sortInfo->runLength=2;
    sortInfo->myOrder=orderMaker;
    //generic dbfile
    GenericDBFile *myInternalVar;
    myInternalVar = new Sorted();
    //create metafile and initialize obj
    myInternalVar->Create(tempfile_path, heap, sortInfo);
    myInternalVar->mode = reading;
    myInternalVar->writingMode();
    int output = myInternalVar->mode;
    //since less records all in 1 page -> therefore returns 0
    ASSERT_EQ(output,1);
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

//test BigQ::dumpSortedList
TEST_F(BigQTest,binarySearch1){
    extern struct AndList *final;
    Record literal;
    Record temp;
    CNF sort_pred;
    Schema nation (catalog_path, (char*)"nation");
    sort_pred.GrowFromParseTree (final, &nation, literal);

    OrderMaker* orderMaker=new(OrderMaker);
    orderMaker->numAtts=1;
    orderMaker->whichTypes[0]=String;
    orderMaker->whichAtts[0]=6;
    //startup
    SortInfo* sortInfo=new SortInfo;
    sortInfo->runLength=2;
    sortInfo->myOrder=orderMaker;
    //generic dbfile
    GenericDBFile *myInternalVar;
    myInternalVar = new Sorted();
    //create metafile and initialize obj
    myInternalVar->Create(tempfile_path, sorted, sortInfo);
    myInternalVar->Load(nation,table_path);
//    cout<<"ok"<<endl;
//    myInternalVar->MoveFirst();
//    cout<<"ok"<<endl;
    Page readingPage;
    readingPage = *(new Page());
    readingPage.EmptyItOut();
    off_t curPageIndex = -1;
    mType mode;
    myInternalVar->mode = reading;
//    cout<<"ok"<<endl;
    int output = myInternalVar->GetNext(temp, sort_pred, literal);
    //since less records all in 1 page -> therefore returns 0
    ASSERT_EQ(output,0);
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
