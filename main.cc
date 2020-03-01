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
    SortInfo* sortInfo=new SortInfo;
    sortInfo->runLength=2;
    OrderMaker* orderMaker=new(OrderMaker);

    char cur_dir[PATH_MAX];
    char dbfile_dir[PATH_MAX];
    char table_path[PATH_MAX];
    char catalog_path[PATH_MAX];
    char tempfile_path[PATH_MAX];
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


//    for part table
    strcpy(table_path,"/Users/apple/Desktop/dbi/tpch-dbgen/1GB/part.tbl");
    Schema nation (catalog_path, (char*)"part");
    orderMaker->numAtts=1;
    orderMaker->whichTypes[0]=Int;
    orderMaker->whichAtts[0]=0;
    sortInfo->myOrder=orderMaker;

//  for nation table
//    Schema nation (catalog_path, (char*)"nation");
//    orderMaker->numAtts=1;
//    orderMaker->whichTypes[0]=String;
//    orderMaker->whichAtts[0]=1;
//    sortInfo->myOrder=orderMaker;



    DBFile* dbFile=new DBFile();

//    dbFile->Open(dbfile_dir);

    dbFile->Create(dbfile_dir,sorted,sortInfo);
    dbFile->Load(nation,table_path);
    dbFile->MoveFirst();

    Record record;
    int counter = 0;
    while (dbFile->GetNext (record) == 1) {
        if(counter==0){
            cout<< "first record:" <<endl;
            record.Print(&nation);
        }
        counter ++;
    }
    cout<< "last record:" <<endl;
    record.Print(&nation);

    cout << " scanned " << counter << " recs \n";
    dbFile->Close();
}
