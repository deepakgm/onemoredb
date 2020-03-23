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
#include "testkit.h"
#include "RelOp.h"

using ::testing::Mock;
using ::testing::Invoke;
using ::testing::Return;
using ::testing::_;

using namespace std;

char cur_dir1[PATH_MAX];
char dbfile_dir1[PATH_MAX];
char table_path[PATH_MAX];
char catalog_path1[PATH_MAX];
char tempfile_path[PATH_MAX];


int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(cur_dir1, sizeof(cur_dir1)) != NULL) {
        clog << "current working dir:" << cur_dir1 << endl;
        strcpy(dbfile_dir1, cur_dir1);
        strcpy(table_path, cur_dir1);
        strcpy(catalog_path1, cur_dir1);
        strcpy(tempfile_path, cur_dir1);
        strcat(dbfile_dir1, "/test/test.bin");
        strcat(table_path, "/test/nation.tbl");
        strcat(catalog_path1, "/test/catalog");
        strcat(tempfile_path, "/test/tempfile");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


class RelOpTest : public ::testing::Test {
public:
    DBFile *dbFile, *dbFile1;
protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        dbFile = new DBFile();
        dbFile->Create(dbfile_dir1, heap, NULL);
        Schema nation(catalog_path1, "nation");
        dbFile->Load(nation, table_path);
//        dbFile->MoveFirst();

    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        delete dbFile;
        remove(dbfile_dir1);
    }
};

//Select file test
TEST_F(RelOpTest, selectFileTest1) {
    char *pred_str = "(n_nationkey > 0)";


    Schema *nation = new Schema(catalog_path1, "nation");

    CNF *cnf = new CNF();
    Record *literal = new Record();

    get_cnf(pred_str, nation, *cnf, *literal);
    clog << "created predicate" << endl;
    SelectFile *selectFile = new SelectFile();

    Pipe *pipe1 = new Pipe(100);
    selectFile->Run(*dbFile, *pipe1, *cnf, *literal);

    Record *record = new Record();
    int count = 0;
    while (pipe1->Remove(record))
        count++;

    ASSERT_EQ(count, 24);

    delete (literal);
    delete (cnf);
    delete (pipe1);
    delete (record);
}

//Select pipe test
TEST_F(RelOpTest, selectPipeTest1) {
    char *pred_str = "(n_nationkey > 0)";

    Schema *nation = new Schema(catalog_path1, "nation");

    CNF *cnf = new CNF();
    Record *literal = new Record();

    get_cnf(pred_str, nation, *cnf, *literal);
    clog << "created predicate" << endl;

    Pipe *inPipe = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    SelectPipe *selectPipe = new SelectPipe();
    selectPipe->Run(*inPipe, *outPipe, *cnf, *literal);
    inPipe->ShutDown();

    int count = 0;
    while (outPipe->Remove(record))
        count++;

    ASSERT_EQ(count, 24);

    delete (cnf);
    delete (inPipe);
    delete (outPipe);
    delete (record);
}


//Projection test
TEST_F(RelOpTest, projectionTest1) {

    Pipe *inPipe = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    int *keepMe = new int[2];
    keepMe[0] = 0;
    keepMe[1] = 2;

    Project *project = new Project();
    project->Run(*inPipe, *outPipe, keepMe, 4, 2);
    inPipe->ShutDown();

    int count = 0;
    while (outPipe->Remove(record))
        count++;

    int attrCount = ((int *) record->bits)[1] / sizeof(int) - 1;

    ASSERT_EQ(count, 25);

    ASSERT_EQ(attrCount, 2);
}


//duplicate removal test
TEST_F(RelOpTest, duplicateTest1) {

    Pipe *inPipe = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    dbFile->MoveFirst();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    Attribute IA = {"int", Int};
    Schema mySchema("mySchema", 1, &IA);

    DuplicateRemoval *duplicateRemoval = new DuplicateRemoval();
    duplicateRemoval->Use_n_Pages(1);
    duplicateRemoval->Run(*inPipe, *outPipe, mySchema);
    inPipe->ShutDown();

    int count = 0;
    while (outPipe->Remove(record))
        count++;

    ASSERT_EQ(count, 25);
}


//join test
TEST_F(RelOpTest, joinTest1) {

    Pipe *inPipe1 = new Pipe(100);
    Pipe *inPipe2 = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe1->Insert(record);
        Record *record = new Record();
    }

    dbFile->MoveFirst();
    while (dbFile->GetNext(*record)) {
        inPipe2->Insert(record);
        Record *record = new Record();
    }


    char *pred_str = "(n_nationkey = n_nationkey)";

    Schema *nation1 = new Schema(catalog_path1, "nation");
    Schema *nation2 = new Schema(catalog_path1, "nation");

    CNF *cnf = new CNF();
    Record *literal = new Record();

    get_cnf (pred_str, nation1, nation2, *cnf, *literal);

    clog << "created predicate" << endl;

    Join *join = new Join();
    join->Use_n_Pages(1);
    join->Run(*inPipe1,*inPipe2, *outPipe, *cnf,*literal);
    inPipe1->ShutDown();
    inPipe2->ShutDown();

    int count = 0;
    while (outPipe->Remove(record))
        count++;

    int attrCount = ((int *) record->bits)[1] / sizeof(int) - 1;

    ASSERT_EQ(count, 24);

    ASSERT_EQ(attrCount, 8);
}


//sum test
TEST_F(RelOpTest, sumTest1) {

    Pipe *inPipe = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    char *pred_str = "(n_nationkey)";

    Schema *nation = new Schema(catalog_path1, "nation");

    CNF *cnf = new CNF();
    Record *literal = new Record();
    Function func;

//    get_cnf (pred_str, &join_sch, func);
    get_cnf (pred_str, nation,func);
//    func.Print();

    clog << "created predicate" << endl;

    Sum *sum = new Sum();
    sum->Use_n_Pages(1);
    sum->Run(*inPipe,*outPipe,func);
    inPipe->ShutDown();


    int count = 0;
    while (outPipe->Remove(record))
        count++;

    int attrCount = ((int *) record->bits)[1] / sizeof(int) - 1;

    ASSERT_EQ(count,1);
    ASSERT_EQ(attrCount,1);
}


//groupby test
TEST_F(RelOpTest, groupByTest1) {

    Pipe *inPipe = new Pipe(100);
    Pipe *outPipe = new Pipe(100);

    Record *record = new Record();
    while (dbFile->GetNext(*record)) {
        inPipe->Insert(record);
        Record *record = new Record();
    }

    char *pred_str = "(n_regionkey)";

    Schema *nation = new Schema(catalog_path1, "nation");

    CNF *cnf = new CNF();
    Record *literal = new Record();
    Function func;

    get_cnf (pred_str, nation,func);

    clog << "created predicate" << endl;

    OrderMaker orderMaker (nation);

    orderMaker.numAtts=1;
    orderMaker.whichAtts[0]=2;
    orderMaker.whichTypes[0]=Int;
    
    
    GroupBy *groupBy = new GroupBy();
    groupBy->Use_n_Pages(1);

    groupBy->Run(*inPipe,*outPipe,orderMaker,func);
    inPipe->ShutDown();

//    Attribute IA = {"int", Int};
//    Attribute SA = {"string", String};
//    Attribute att3[] = {IA, SA, IA,SA,IA};
//    Attribute att3[] = {IA, IA};
//    Schema out_sch ("out_sch", 2, att3);


    int count = 0;
    while (outPipe->Remove(record)){
        count++;
//        record->Print(&out_sch);
    }

    int attrCount = ((int *) record->bits)[1] / sizeof(int) - 1;

    ASSERT_EQ(count,5);
    ASSERT_EQ(attrCount,2);

}
