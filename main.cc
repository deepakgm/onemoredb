#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"

using namespace std;

int main() {
     MetaInfo metaInfo;
     metaInfo=GetMetaInfo();
//    metaInfo.sortInfo->myOrder->Print();
//     cout << "got meta " <<  metaInfo.sortInfo->myOrder->numAtts<< endl;
//     SortInfo* sortInfo=new SortInfo;
//     sortInfo->runLength=2;
//     OrderMaker* orderMaker=new(OrderMaker);
//     orderMaker->numAtts=2;
//     orderMaker->whichTypes[0]=Int;
//    orderMaker->whichTypes[0]=Double;
//     //    orderMaker->whichAtts=new int[20];
//     orderMaker->whichAtts[0]=1;
//    orderMaker->whichAtts[1]=2;
//     //    orderMaker.whichTypes=["Int","asa"]
//    sortInfo->myOrder=orderMaker;
//     WriteMetaInfo("somepath",sorted, sortInfo);
//GetMetaInfo();


//    cout << metaInfo.binFilePath<< endl ;

//    char cur_dir[PATH_MAX];
//    char dbfile_dir[PATH_MAX];
//    char table_path[PATH_MAX];
//    char catalog_path[PATH_MAX];
//    char tempfile_path[PATH_MAX];
//    if (getcwd(cur_dir, sizeof(cur_dir)) != NULL) {
//        clog <<"current working dir:" << cur_dir << endl;
//        strcpy(dbfile_dir,cur_dir);
//        strcpy(table_path,cur_dir);
//        strcpy(catalog_path,cur_dir);
//        strcpy(tempfile_path,cur_dir);
//        strcat(dbfile_dir,"/test/test.bin");
//        strcat(table_path,"/test/nation.tbl");
//        strcat(catalog_path,"/test/catalog");
//        strcat(tempfile_path,"/test/tempfile");
//    } else {
//        cerr << "error while getting curent dir" << endl;
//        return 1;
//    }

//    DBFile* dbFile=new DBFile();
//    dbFile->Create(dbfile_dir,heap,NULL);
//    Schema nation (catalog_path, (char*)"nation");
//    dbFile->Load(nation,table_path);
//
//    dbFile->MoveFirst();
//
//
//    Record record;
//    dbFile->GetNext(record);
//    record.Print(&nation);
//
//    dbFile->Close();
}
