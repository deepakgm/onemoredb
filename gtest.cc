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
    Pipe* inPipe;
    Pipe* outPipe;
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
    Schema nation (catalog_path, "nation");
    recordList.at(5)->Print(&nation);

//    OrderMaker orderMaker;
//    BigQ bigQ (*inPipe, *outPipe, orderMaker, 2);
//    bigQ.file.Open(0, tempfile_path);
//    bigQ.file.AddPage(new Page(), -1);

//    bigQ.dumpSortedList(recordList);
    cout<< "here" <<endl;
//    inPipe->ShutDown();
}

//workerThread()
TEST_F(BigQTest,workerThread1){

}


//phaseOne

//phasetwo