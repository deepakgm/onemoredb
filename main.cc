#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"
#include <chrono>
#include "bpt.h"

using namespace std;


bool compare(string a,string b){
    return a>b;
}

void print(string a){
    cout<<a<<endl;
    return;
}

int main() {

//6 4
//    for part table
//    strcpy(table_path,"/home/deepak/Desktop/dbi/tpch-dbgen/1GB/nation.tbl");
    Schema nation ("/home/deepak/Desktop/dbi/onemoredb/catalog", "nation");



//    bpt::bplus_tree database("/home/deepak/Desktop/dbi/onemoredb/temp/tmp11", true);
//    for (int i = 0; i <= 10000; i++) {
//        if (i % 1000 == 0)
//            printf("%d\n", i);
//        char key[16] = { 0 };
//        sprintf(key, "%d", i);
//        string aa="12345678901234567890";
//
//        database.insert(key, (char *)aa.c_str());
//    }
//
//
//    database.search("1", reinterpret_cast<bpt::value_t *>(1));

//cout << sizeof(string)<<endl;
DBFile* dbFile=new DBFile();

SortInfo* sortInfo=new SortInfo();
OrderMaker* omg=new OrderMaker(&nation);
sortInfo->myOrder=omg;


dbFile->Create("/home/deepak/Desktop/dbi/onemoredb/temp/tmpf1",tree, (void *)sortInfo);
dbFile->myInternalVar->Load(nation,"/home/deepak/Desktop/dbi/tpch-dbgen/10MB/nation.tbl");



//
//dbFile->MoveFirst();
Record tempRecord;
while(dbFile->myInternalVar->GetNext(tempRecord)==1){
    tempRecord.Print(&nation);
}
//dbFile->myInternalVar->GetNext(tempRecord);
//dbFile->myInternalVar->GetNext
//
//
//
//
//
//
//
//
//
//
//
//
// (tempRecord);
//tempRecord.Print(&nation);
//    dbFile->myInternalVar->GetNext(tempRecord);
//    tempRecord.Print(&nation);
//    dbFile->myInternalVar->GetNext(tempRecord);
//    tempRecord.Print(&nation);


//    FILE *tableFile = fopen("/home/deepak/Desktop/dbi/tpch-dbgen/10MB/lineitem.tbl", "r");
//
//    tempRecord.SuckNextRecord(&nation, tableFile);
//    tempRecord.SuckNextRecord(&nation, tableFile);
//    tempRecord.SuckNextRecord(&nation, tableFile);
//    tempRecord.SuckNextRecord(&nation, tableFile);
//    tempRecord.SuckNextRecord(&nation, tableFile);
//
//    tempRecord.Print(&nation);
//
//    string str(tempRecord.bits);
//    char *curPos = tempRecord.bits + sizeof (int);
//    int len = ((int *) curPos)[0];
////
//    cout <<len<<endl;
//
//    Record newRecord;
//    newRecord.CopyBits(tempRecord.bits,160);
////    newRecord.Print(&nation);
//
    cout << "done" <<endl;
}