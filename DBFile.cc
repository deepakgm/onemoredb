#include <cstdlib>
#include <iostream>
#include <fstream>
#include <string.h>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"


using namespace std;

const string meta_file_loc = "/home/gurpreet/Desktop/dbi/onemoredb/metafile";
const char *db_dump_loc = "/home/gurpreet/Desktop/temp/t";
bool isDirty= false;

DBFile::DBFile() {
    f = new File();
    curPage = new Page;
    isDirty= false;
}

DBFile::~DBFile() {
    delete (curPage);
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type != heap) {
        cout << "file type is undefined!!" << endl;
        return 1;
    }
    f->Open(0, strdup(f_path));
    return 0;
}

int DBFile::Open(const char *f_path) {
    f->Open(1, strdup(f_path));
    cout << "file length: " << f->GetLength()<<endl;;

    if (0 != f->GetLength()) {
        f->GetPage(curPage, f->GetLength() - 2);
    }
    return 0;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {

//    isDirty= true;

    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == nullptr) {
        cerr << "invalid table file" << endl;
        exit(-1);
    }

    Record tempRecord;
    int recordCount = 0;
    int pageCount = 0;

    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1) {
        recordCount++;

        int isFull = curPage->Append(&tempRecord);
        isDirty=true;
        if (isFull == 0) {
            f->AddPage(curPage, pageCount++);
            curPageIndex++;
            curPage->EmptyItOut();
            curPage->Append(&tempRecord);
        }
    }
    f->AddPage(curPage, pageCount++);
    isDirty= false;
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;

//    FILE *tableFile = fopen(loadpath, "r");
//    if (tableFile == nullptr) {
//        cerr << "invalid table file" << endl;
//        exit(-1);
//    }
//
//    Record tempRecord;
//    int recordCount = 0;
//    Page tempPage;
//    int pageCount = 0;
//
//    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1) {
//        recordCount++;
//
//        int isFull = tempPage.Append(&tempRecord);
//        if (isFull == 0) {
//            f->AddPage(&tempPage, pageCount++);
//            tempPage.EmptyItOut();
//            tempPage.Append(&tempRecord);
//        }
//    }
//
//    f->AddPage(&tempPage, pageCount++);
//    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}


void DBFile::MoveFirst() {
    curPageIndex = (off_t) 0;
    if (0 != f->GetLength()) {
        cout<<curPageIndex<<endl;
        f->GetPage(curPage, curPageIndex);
    } else {
        curPage->EmptyItOut();
    }
}

int DBFile::Close() {
    if(isDirty)
        f->AddPage(curPage,f->GetLength() );
    auto r = f->Close();
    delete f;
    return r;
}

void DBFile::Add(Record &rec) {
//    if (writePage->Append(&rec)==0) {
//        f->AddPage(writePage,f->GetLength() - 1);
//        writePage->EmptyItOut();
//        writePage->Append(&rec);
//    } else {
//        //            f->AddPage(&tempPage, f->GetLength() - 2); // same final page
//        //todo verify not full then also it writes to cur page
//    }



    Page tempPage;
    cout << "getting page " << f->GetLength() << endl;
    if (0 != f->GetLength()) {
        f->GetPage(&tempPage, f->GetLength() - 2);
        if (0 == tempPage.Append(&rec)) {
            tempPage.EmptyItOut();
            tempPage.Append(&rec);
            f->AddPage(&tempPage, f->GetLength() - 1);
        } else {
            //            f->AddPage(&tempPage, f->GetLength() - 2); // same final page
            //todo verify not full then also it writes to cur page
        }
    } else {
        if (1 == tempPage.Append(&rec)) {
            f->AddPage(&tempPage, 0);
        } else {
            cerr << "New file created; but fails appending" << endl;
            exit(-1);
        }
    }
}

int DBFile::GetNext(Record &fetchme) {
    if(curPage->GetFirst(&fetchme) == 0){
           ++curPageIndex;
        if(curPageIndex <= f->GetLength()-2){
            f->GetPage(curPage,curPageIndex);
            curPage->GetFirst(&fetchme);
            return 1;
        }else{
            //read buffer has the one not in page
            //add to page and then read again
            return 0;
        }
    }else{
        return 1;
    }

    if (f == NULL)
        return 1;
//    cout << "current index is:"+to_string(curPageIndex)<<endl;
    if (0 == curPage->GetFirst(&fetchme)) // 0 is empty
    { // page is empty, get next page, if available, and return a record from it.
        // cout << "page " << curPageIndex + 1 << " was depleted." << endl;
        curPageIndex++;

//        clog << "attempting to read page " << curPageIndex  << " out of " << (f->GetLength() - 1) << "... " << endl;++curPageIndex;
        if (curPageIndex + 1 <= (f->GetLength() - 1)) {
            f->GetPage(curPage, curPageIndex-1);
            int ret = curPage->GetFirst(&fetchme);
            if (ret != 1) {
                cout << "something is not right!!";
                exit(-1);
            }
            return 1;
        } else {
//        cout << "curPageIndex"  << curPageIndex << " "<<f->GetLength()<< endl;
//            cout << "something is not right!!";
            return 0;
        }
    } else {
//        clog << "fetched a record from current page"<<endl;
        return 1;
    }
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    while(GetNext(fetchme))
        if(comparisonEngine.Compare(&fetchme,&literal,&cnf)){
            return 1;
        }
    return 0;


    //    ComparisonEngine comp;
//    Record temp;
//
//    while (this->GetNext(temp)) {
//        if (comp.Compare(&temp, &literal, &cnf)) {
////            fetchme.Consume(&temp);
//            return 1;
//        }
//    }
//    return 0;
}


//11,0 12,35   2,3 1,1 2,3