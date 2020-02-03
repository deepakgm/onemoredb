#include <cstdlib>
#include <iostream>
#include <string.h>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "DBFile.h"

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
    clog << "opening file of length: " << f->GetLength() << endl;;

    if (0 != f->GetLength()) {
        f->GetPage(curPage, f->GetLength() - 2);
    }
    return 0;
}

//todo deal with page loads that are called in between add and gets
void DBFile::Load(Schema &f_schema, const char *loadpath) {
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
    auto r = f->Close();
    delete f;
    return r;
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
        f->AddPage(curPage, f->GetLength()-1);
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

//11,0 12,35   2,3 1,1 2,3