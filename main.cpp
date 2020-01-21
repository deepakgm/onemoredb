#include <iostream>
#include "File.h"
#include "DBFile.h"
using namespace std;


int main() {
    cout << "Hello, World!" << endl;

    DBFile* dbFile=new DBFile();
    Schema lineitem ("/home/gurpreet/CLionProjects/P2/catalog", "lineitem");

//    dbFile->Create("/home/gurpreet/Desktop/temp/t",heap, nullptr);
//    dbFile->Load(lineitem,"/home/gurpreet/CLionProjects/P2/lineitem.tbl");

    dbFile->Open("/home/gurpreet/Desktop/temp/t");

    Record tempRecord;
    dbFile->GetNext(tempRecord);
//    dbFile->MoveFirst();
//    dbFile->Ge(tempRecord);
    tempRecord.Print(&lineitem);



    return 0;
}
