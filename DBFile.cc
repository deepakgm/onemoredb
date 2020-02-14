#include <cstdlib>
#include <iostream>
#include <string.h>
#include <unistd.h>
#include <fstream>
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "DBFile.h"

using namespace std;

bool isDirty = false;

char metafile_path[4096];

DBFile::DBFile() {
    f = new File();
    curPage = new Page;
    isDirty = false;
    string bin_path;

    if (getcwd(metafile_path, sizeof(metafile_path)) != NULL) {
        clog << "current working dir:" << metafile_path << endl;
        strcat(metafile_path, "/metafile");
    } else {
        cerr << "error while getting curent dir" << endl;
        exit(-1);
    }
}

DBFile::~DBFile() {
    delete (curPage);
}

bool DBFile::GetIsDirty() {
    return isDirty;
}

void WriteToMeta(const char *bin_path) {
    FILE *out;
    if ((out = fopen(metafile_path, "w")) != NULL) {
        fprintf(out, bin_path);
        fclose(out);
    }
}

char *GetFromMeta() {
    string bin_path;
    ifstream metafile;
    metafile.open(metafile_path);
    getline(metafile, bin_path);
    clog << bin_path << endl;
    metafile.close();
    char *cstr = new char[bin_path.length() + 1];
    strcpy(cstr, bin_path.c_str());
    return cstr;
}

int DBFile::Create(const char *f_path, fType f_type, void *startup) {
    if (f_type != heap) {
        cout << "file type is undefined!!" << endl;
        return 1;
    }
    f->Open(0, strdup(f_path));
    clog << "writing bin_path to metafile:" << f_path << endl;
    WriteToMeta(f_path);
    return 0;
}

int DBFile::Open(const char *f_path) {
    if (f_path == NULL) {
        clog << "fetching bin_path from metafile.." << endl;
        f_path = GetFromMeta();
        clog << "binpath: " << f_path << endl;
    } else {
        clog << "writing bin_path to metafile.." << endl;
        WriteToMeta(f_path);
    }

    if (FILE *file = fopen(f_path, "r")) {
        fclose(file);
    } else {
        return 1;
    }
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

//11,0 12,35   2,3 1,1 2,3