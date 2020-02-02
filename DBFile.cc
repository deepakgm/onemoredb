#include <cstdlib>
#include <iostream>
#include <fstream>
#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "DBFile.h"
#include "Defs.h"


using namespace std;

const string meta_file_loc="/home/gurpreet/Desktop/dbi/onemoredb/metafile";
const string db_dump_loc="/home/gurpreet/Desktop/temp/t";

DBFile::DBFile() {
    f = new File();
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type != heap){
        cout<< "file type is undefined!!" << endl;
        exit(-1);
    }
    f->Open(0, (char*)f_path);
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

int DBFile::Open(const  char *f_path) {
    if(f_path== nullptr){
        FILE *f = fopen(meta_file_loc.c_str(), "rb");
        if(f==NULL){
            cerr<< "metafile not found"<< endl;
            return 1;
        }
        fseek(f, 0, SEEK_END);
        long fsize = ftell(f);
        fseek(f, 0, SEEK_SET);

        f_path= (char *)malloc(fsize + 1);
        fread((char *)f_path, fsize, 1, f);
        fclose(f);
        clog<< "db_dump_loc read from meta:"+string(f_path)<< endl;
    }else{
        ofstream newFile(meta_file_loc);
        if(newFile.is_open())
        {
            newFile << db_dump_loc;
            clog << "db_dump_loc written to metafile" << endl;
        }else{
           cerr << "unable to open the metafle" << endl;
           return 1;
        }
//        delete f_path;
        newFile.close();
    }
    f->Open(1, (char*)f_path);
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
    auto r=f->Close();
    delete f;
    return r;
}

void DBFile::Add(Record &rec) {
    Page tempPage; //
     cout << "getting page " << f->GetLength() << endl;
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
//    cout << "current index is:"+to_string(curPageIndex)<<endl;
    if (0 == curPage.GetFirst(&fetchme)) // 0 is empty
    { // page is empty, get next page, if available, and return a record from it.
        // cout << "page " << curPageIndex + 1 << " was depleted." << endl;
        curPageIndex++;

//        clog << "attempting to read page " << curPageIndex  << " out of " << (f->GetLength() - 1) << "... " << endl;++curPageIndex;
    if (curPageIndex + 1 <= (f->GetLength() - 1)) {
            f->GetPage(&curPage, curPageIndex);
            int ret = curPage.GetFirst(&fetchme);
            if (ret != 1) {
                cout << "something is not right!!";
                exit(-1);
            }
            return 1;
        }else{
//        cout << "curPageIndex"  << curPageIndex << " "<<f->GetLength()<< endl;
//            cout << "something is not right!!";
        return 0;
        }
    }else{
//        clog << "fetched a record from current page"<<endl;
        return 1;
    }
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    ComparisonEngine comp;
    Record temp;

    while(this->GetNext(temp)){
        if (comp.Compare(&temp, &literal, &cnf)) {
            fetchme.Consume(&temp);
            return 1;
        }
    }
    return 0;
}


//11,0 12,35   2,3 1,1 2,3