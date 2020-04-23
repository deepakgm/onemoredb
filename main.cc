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


bool compare(string a, string b) {
    return a > b;
}

void print(string a) {
    cout << a << endl;
    return;
}

int main() {


    DBFile *dbFile = new DBFile();

    SortInfo *sortInfo = new SortInfo();

    Schema nation("/home/deepak/Desktop/dbi/onemoredb/catalog", "nation");
    OrderMaker *omg = new OrderMaker(&nation);
    sortInfo->myOrder = omg;


    dbFile->Create("/home/deepak/Desktop/dbi/onemoredb/temp/tmpf1", tree, (void *) sortInfo);
    dbFile->myInternalVar->Load(nation, "/home/deepak/Desktop/dbi/tpch-dbgen/1GB/nation.tbl");

    cout << "reading the data.." << endl;

    Record tempRecord;
    while (dbFile->myInternalVar->GetNext(tempRecord) == 1) {
        tempRecord.Print(&nation);
    }
    cout << "done" << endl;
}