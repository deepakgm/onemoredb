#include <cstdlib>
#include <iostream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"

using namespace std;

DBFile::DBFile() {
    f = new File();
}

int DBFile::Create(char *f_path, fType f_type, void *startup) {
    if (f_type != heap)
        exit(-1);
//    f = new File();
    f->Open(0, f_path);
    //todo deal with startup
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    if (0 == tableFile)
        exit(-1);
    Record tempRecord;
    Page tempPage;
    int recordCounter = 0; // counter for debug
    int pageCounter = 0; // counter for debug

    while (1 == tempRecord.SuckNextRecord(&f_schema, tableFile)) { // there is another record available
        recordCounter++;
        if (recordCounter % 10000 == 0) {
            cerr << recordCounter << "\n";
        }
        // use tempRecord, and put into tempPage. Later if page is full, write to file,
        int full = tempPage.Append(&tempRecord);
        if (0 == full) {
            // page was full
            f->AddPage(&tempPage, pageCounter++);
            tempPage.EmptyItOut();
            tempPage.Append(&tempRecord);
        }
    }
    { // make sure to add the last page
        f->AddPage(&tempPage, pageCounter++);
        cout << "Read and converted " << recordCounter <<
             " records, into " << pageCounter << " pages." << endl;
    }
}

int DBFile::Open(char *f_path) {
    f->Open(1, f_path);
    MoveFirst();
    return 0;
}

void DBFile::MoveFirst() {
    // consider keeping an index value, rather than holding the page itself.
    curPageIndex = (off_t) 0;
    if (0 != f->GetLength()) {
        f->GetPage(&curPage, curPageIndex);
    } else {
        curPage.EmptyItOut();
    }
}

int DBFile::Close() {
}

void DBFile::Add(Record &rec) {
    Page tempPage; //
    // cout << "getting page " << f.GetLength() << endl;
    if (0 != f->GetLength()) {
        f->GetPage(&tempPage, f->GetLength() - 2); // get the last page with stuff in it.
        if (0 == tempPage.Append(&rec)) // if the page is full
        {
            // f.AddPage(&tempPage,f.GetLength()-1); // don't add page, it's already there.
            tempPage.EmptyItOut();
            tempPage.Append(&rec);
            f->AddPage(&tempPage, f->GetLength() - 1); // new final page
        } else // the page is not full (this is probably the more common case and we should flip the if/else order
        {
            f->AddPage(&tempPage, f->GetLength() - 2); // same final page
        }
    } else // special case, we have a fresh file.
    {
        if (1 == tempPage.Append(&rec)) {
            f->AddPage(&tempPage, 0); // new final page
        } else  // ought to have been a fresh page, if it's full, can't do anything anyway.
        {
            exit(-1);
        }
    }
}

int DBFile::GetNext(Record &fetchme) {
    if (0 == curPage.GetFirst(&fetchme)) // 0 is empty
    { // page is empty, get next page, if available, and return a record from it.
        // cout << "page " << curPageIndex + 1 << " was depleted." << endl;
        ++curPageIndex;
        // clog << "attempting to read page " << curPageIndex + 1  << " out of "
        //      << (f.GetLength() - 1) << "... " << endl;
        if (curPageIndex + 1 <= f->GetLength() - 1) {
            // cout << "successful" << endl;
            f->GetPage(&curPage, curPageIndex);
            int ret = curPage.GetFirst(&fetchme);
            if (ret != 1) {
                cout << "something is not right!!";
                exit(-1);
            }
            return 1;
        }
        return 0;
    }

    return 1;
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
   //todo
    return 0;
}
