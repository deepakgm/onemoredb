#include <cstdlib>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "DBFile.h"
#include "Meta.h"

using namespace std;

bool isDirty = false;

DBFile::DBFile() {
    f = new File();
    curPage = new Page;
    isDirty = false;
}

DBFile::~DBFile() {
    delete (curPage);
}

bool DBFile::GetIsDirty() {
    return isDirty;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
//    if (f_type == heap) {
//        myInternalVar = new DBFile::Heap();
//    }else if (f_type == sorted) {
//        myInternalVar = new DBFile::Sorted();
//    }
    return 1;

//    return myInternalVar->Create(f_path, startup);
}

//int DBFile::Heap::Create (const char *f_path, void *startup) {
//    file.Open(0, strdup(f_path));
//    readingPage.EmptyItOut();
//    writingPage.EmptyItOut();
//    file.AddPage(&writingPage, -1);
//    WriteMetaInfo(f_path,heap,startup);
//    MoveFirst();
//    return 1;
//}
//
//int DBFile::Sorted::Create (const char *f_path, void *startup) {
//    file.Open(0, strdup(f_path));
//    readingPage.EmptyItOut();
//    writingPage.EmptyItOut();
//    file.AddPage(&writingPage, -1);
//    WriteMetaInfo(f_path,sorted,startup);
//    return 1;
//}

//int DBFile::Open (const char *f_path) {
//    // Firstly open .header file and read the first line to make sure whether it's a heap file or sorted file.
//    MetaInfo metaInfo=GetMetaInfo();
//
//    if (metaInfo.fileType == heap) {
////        myInternalVar = new Heap();
//    }
//    else if(metaInfo.fileType==sorted){
////        myInternalVar = new Sorted();
//    }else{
//        return 0;
//    }
//    return myInternalVar->Open(f_path);
//}
//
//int DBFile::Heap::Open (const char *fpath) {
//    file.Open(1, strdup(fpath));  // Open existing DBFile
//    readingPage.EmptyItOut();
//    writingPage.EmptyItOut();
//    MoveFirst();
//    return 1;
//}
//
//int DBFile::Sorted::Open (const char *f_path) {
//    string input = "";
////    myOrder.numAtts = 0;
////    getline(f, input);
////    while (f >> myOrder.whichAtts[myOrder.numAtts] && !f.eof()) {
////        int type = 0;
////        f >> type;
////        if (type == 0) myOrder.whichTypes[myOrder.numAtts] = Int;
////        else if (type == 1) myOrder.whichTypes[myOrder.numAtts] = Double;
////        else myOrder.whichTypes[myOrder.numAtts] = String;
////        ++myOrder.numAtts;
////    }
////    f.close();
////    file.Open(1, f_path);
//    writingPage.EmptyItOut();
//    readingPage.EmptyItOut();
//    return 1;
//}

//todo deal with page loads that are called in between add and gets
void DBFile::Load(Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == NULL) {
        cerr << "invalid table file" << endl;
        exit(-1);
    }

    Record tempRecord;
    int recordCount = 0;
    int pageCount = f->GetLength();

    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1) {
        recordCount++;

        int isFull = curPage->Append(&tempRecord);
        isDirty = true;
        if (isFull == 0) {
            f->AddPage(curPage, pageCount++);
            curPageIndex++;
            curPage->EmptyItOut();
            curPage->Append(&tempRecord);
        }
    }
    f->AddPage(curPage, pageCount++);
    isDirty = false;
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}


void DBFile::MoveFirst() {
    if (isDirty) {
        f->AddPage(curPage, f->GetLength() - 1);
        isDirty = false;
    }

    curPageIndex = (off_t) 0;
    if (0 != f->GetLength()) { ;
        f->GetPage(curPage, 0);
    } else {
        curPage->EmptyItOut();
    }
}

int DBFile::Close() {
    if (isDirty)
        f->AddPage(curPage, f->GetLength() - 1);
    if(f->Close()>=0){
        delete f;
        return 0;
    }
    delete f;
    return 1;
}

void DBFile::Add(Record &record) {
    isDirty = true;
    int isFull = curPage->Append(&record);
    if (isFull == 0) {
        if (f->GetLength() == 0)
            f->AddPage(curPage, 0);
        else {
            f->AddPage(curPage, f->GetLength() - 1);
        }
        curPage->EmptyItOut();
        curPage->Append(&record);
    }
}

int DBFile::GetNext(Record &fetchme) {
    if (isDirty) {
        f->AddPage(curPage, f->GetLength() - 1);
        isDirty = false;
    }

    if (curPage->GetFirst(&fetchme) == 0) {
        ++curPageIndex;
        if (curPageIndex <= f->GetLength() - 2) {
            f->GetPage(curPage, curPageIndex);
            curPage->GetFirst(&fetchme);
            return 1;
        } else {
            return 0;
        }
    } else {
        return 1;
    }
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    while (GetNext(fetchme))
        if (comparisonEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}
