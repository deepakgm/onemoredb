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

//todo
DBFile::DBFile() {
    isDirty = false;
}

//todo
DBFile::~DBFile() {
}

GenericDBFile::GenericDBFile() {
    readingPage = *(new Page());
    writingPage = *(new Page());
    myOrder = new(OrderMaker);
}

bool DBFile::GetIsDirty() {
    return isDirty;
}

int GenericDBFile::Create(const char *f_path, fType type, void *startup) {
    file.Open(0, strdup(f_path));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    WriteMetaInfo(f_path, type, startup);
    if (startup != NULL) {
        myOrder = ((SortInfo *) startup)->myOrder;
        runLength = ((SortInfo *) startup)->runLength;
    }
    MoveFirst();
    return 1;
}

int DBFile::Create(const char *f_path, fType type, void *startup) {
    if (type == heap) {
        myInternalVar = new Heap();
    } else if (type == sorted) {
        myInternalVar = new Sorted();
    }
    return myInternalVar->Create(f_path, type, startup);
}

int DBFile::Open(const char *f_path) {
    MetaInfo metaInfo = GetMetaInfo();

    if (metaInfo.fileType == heap) {
        myInternalVar = new Heap();
    } else if (metaInfo.fileType == sorted) {
        myInternalVar = new Sorted();
    } else {
        return 0;
    }
    return myInternalVar->Open(f_path);
}

int GenericDBFile::Open(const char *fpath) {
    MetaInfo metaInfo = GetMetaInfo();
    myOrder = metaInfo.sortInfo->myOrder;
    file.Open(1, strdup(fpath));
    writingPage.EmptyItOut();
    readingPage.EmptyItOut();
    return 1;
}

void Heap::readingMode() {
    if (mode == reading) return;
    mode = reading;
    file.AddPage(&writingPage, file.GetLength() - 1);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
}

void Heap::writingMode() {
    if (mode == writing) return;
    mode = writing;
}

void Sorted::writingMode() {
    if (mode == writing) return;
    mode = writing;
    bigQ = new BigQ(in, out, *myOrder, runLength);
}

//todo change
void Sorted::readingMode() {
    if (mode == reading) return;
    mode = reading;
    MoveFirst();
    in.ShutDown();
    writingPage.EmptyItOut();

    bool pipeEmpty = false, fileEmpty = false; // Help determine which queue run out first.

    Record recFromPipe;
    Record recFromPage;

    if (!out.Remove(&recFromPipe)) pipeEmpty = true;

    if (!GetNext(recFromPage)) fileEmpty = true;

    // Run two way merge only if two input queues are not empty.
    while (!pipeEmpty && !fileEmpty) {
        if (compEng.Compare(&recFromPipe, &recFromPage, myOrder) <= 0) {
            // Write smaller record to writing buffer. If writing buffer if full, add this page to file and start to write to a new empty page.
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
            // If one queue is empty, end merging.
            if (!out.Remove(&recFromPipe)) pipeEmpty = true;
        } else {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
            if (!GetNext(recFromPage)) fileEmpty = true;
        }
    }
    // Determine which queue runs out and append records of the other queue to the end or new file.
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&recFromPage)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPage);
        }
        while (GetNext(recFromPage)) {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
        }
    } else if (!pipeEmpty) {
        if (!writingPage.Append(&recFromPipe)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPipe);
        }
        while (out.Remove(&recFromPipe)) {
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
        }
    }
    if (!pipeEmpty || !fileEmpty) file.AddPage(&writingPage, file.GetLength() - 1);

    delete bigQ;
}


void DBFile::Load(Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

//todo handle bulk load
void Heap::Load(Schema &f_schema, const char *loadpath) {
    writingMode();
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == NULL) {
        cerr << "invalid table file" << endl;
        exit(-1);
    }

    Record tempRecord;
    long recordCount = 0;
    long pageCount = file.GetLength() - 1;

    int isNotFull = 0;
    while (tempRecord.SuckNextRecord(&f_schema, tableFile) == 1) {
        recordCount++;
        isNotFull = writingPage.Append(&tempRecord);
        if (!isNotFull) {
            file.AddPage(&writingPage, ++pageCount);
            curPageIndex++;
            writingPage.EmptyItOut();
            writingPage.Append(&tempRecord);
        }
    }
    if (!isNotFull)
        file.AddPage(&writingPage, 0);
    fclose(tableFile);
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}

void Sorted::Load(Schema &schema, const char *loadpath) {
    FILE *textFile = fopen(loadpath, "r");
    int numofRecords = 0;
    if (textFile == NULL) {
        cerr << "invalid load_path" << loadpath << endl;
        exit(1);
    } else {
        Record tempRecord = Record();
        writingMode();
        while (tempRecord.SuckNextRecord(&schema, textFile)) {
            in.Insert(&tempRecord);
            ++numofRecords;
        }
        fclose(textFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::MoveFirst() {
    myInternalVar->MoveFirst();
}

void GenericDBFile::MoveFirst() {
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;
}

int DBFile::Close() {
    return myInternalVar->Close();
}

int GenericDBFile::Close() {
    readingMode();
    return file.Close() > 0 ? 1 : 0;
}

void DBFile::Add(Record &record) {
    myInternalVar->Add(record);
}

void Heap::Add(Record &record) {
    writingMode();
    int isFull = writingPage.Append(&record);
    if (isFull == 0) {
        if (file.GetLength() == 0)
            file.AddPage(&writingPage, 0);
        else {
            file.AddPage(&writingPage, file.GetLength() - 1);
        }
        writingPage.EmptyItOut();
        writingPage.Append(&record);
    }
}

void Sorted::Add(Record &rec) {
    writingMode();
    in.Insert(&rec);
}

int DBFile::GetNext(Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

int Heap::GetNext(Record &fetchme) {
    if (mode == writing) {
        readingMode();
    }
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        } else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (mode == writing) {
        readingMode();
    }
    while (GetNext(fetchme))
        if (compEng.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}

//todo change
int Sorted::GetNext(Record &fetchme) {
    if (mode == writing) {
        readingMode();
    }
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        } else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

//todo change
int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (mode == writing) {
        readingMode();
    }
    while (GetNext(fetchme))
        if (compEng.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}
