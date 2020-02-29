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
    if (f_type == heap) {
        myInternalVar = new DBFile::Heap();
    } else if (f_type == sorted) {
        myInternalVar = new DBFile::Sorted();
    }
    return myInternalVar->Create(f_path, startup);
}

int DBFile::Heap::Create(const char *f_path, void *startup) {
    file.Open(0, strdup(f_path));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    WriteMetaInfo(f_path, heap, startup);
    MoveFirst();
    return 1;
}

int DBFile::Sorted::Create(const char *f_path, void *startup) {
    file.Open(0, strdup(f_path));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    WriteMetaInfo(f_path, sorted, startup);
    return 1;
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

int DBFile::Heap::Open(const char *fpath) {
    file.Open(1, strdup(fpath));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    MoveFirst();
    return 1;
}

int DBFile::Sorted::Open(const char *fpath) {
    MetaInfo metaInfo = GetMetaInfo();
    myOrder = metaInfo.sortInfo->myOrder;
    file.Open(1, strdup(fpath));
    writingPage.EmptyItOut();
    readingPage.EmptyItOut();
    return 1;
}

void DBFile::Load(Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

//todo handle bulk load
void DBFile::Heap::Load (Schema &myschema, const char *loadpath) {
    FILE *textFile = fopen(loadpath,"r");
    int numofRecords = 0;
    if(textFile == NULL){
        cerr << "invalid load_path"<<loadpath << endl;
        exit(1);
    }else{
        Record record = Record();
        while(record.SuckNextRecord(&myschema,textFile)) {
            Add(record);
            ++numofRecords;
        }
        fclose(textFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::Heap::readingMode() {
    if (mode == reading) return;
    mode = reading;
    file.AddPage(&writingPage, file.GetLength() - 1);
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
}

void DBFile::Heap::writingMode() {
    if (mode == writing) return;
    mode = writing;
}

void DBFile::Sorted::writingMode() {
    // cout << "writingMode" << endl;
    if (mode == writing) return;
    mode = writing;
    bigQ = new BigQ(in, out, *myOrder, runLength);
}
//todo change
void DBFile::Sorted::readingMode() {
    // cout << "readingMode" << endl;
    if (mode == reading) return;
    if (mode == query) {
        mode = reading;
        return;
    }
    mode = reading;
    MoveFirst();
    // Shutdown in pipe so that BigQ will start the second stage which will merge all runs into a sorded order, and we can retrieve sorted records from out pipe.
    in.ShutDown();
    File tmpFile;
    tmpFile.Open(0, "mergingFile.tmp");
    writingPage.EmptyItOut();
    tmpFile.AddPage(&writingPage, -1);

    bool pipeEmpty = false, fileEmpty = false; // Help determine which queue run out first.

    Record left;
    if (!out.Remove(&left)) pipeEmpty = true;

    Record right;
    if (!GetNext(right)) fileEmpty = true;

    // Run two way merge only if two input queues are not empty.
    while (!pipeEmpty && !fileEmpty) {
        if (compEng.Compare(&left, &right, myOrder) <= 0) {
            // Write smaller record to writing buffer. If writing buffer if full, add this page to file and start to write to a new empty page.
            if (!writingPage.Append(&left)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&left);
            }
            // If one queue is empty, end merging.
            if (!out.Remove(&left)) pipeEmpty = true;
        }
        else {
            if (!writingPage.Append(&right)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&right);
            }
            if (!GetNext(right)) fileEmpty = true;
        }
    }
    // Determine which queue runs out and append records of the other queue to the end or new file.
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&right)) {
            tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&right);
        }
        while (GetNext(right)) {
            if (!writingPage.Append(&right)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&right);
            }
        }
    }
    else if (!pipeEmpty && fileEmpty) {
        if (!writingPage.Append(&left)) {
            tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&left);
        }
        while (out.Remove(&left)) {
            if (!writingPage.Append(&left)) {
                tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&left);
            }
        }
    }
    if (!pipeEmpty || !fileEmpty) tmpFile.AddPage(&writingPage, tmpFile.GetLength() - 1);

    tmpFile.Close();
    remove(fpath.c_str());
    rename("mergingFile.tmp", fpath.c_str());

    delete bigQ;
}

void DBFile::Sorted::Load (Schema &myschema, const char *loadpath) {
    FILE *textFile = fopen(loadpath,"r");
    int numofRecords = 0;
    if(textFile == NULL){
        cerr << "invalid load_path"<<loadpath << endl;
        exit(1);
    }else{
        Record record = Record();
        while(record.SuckNextRecord(&myschema,textFile)) {
            Add(record);
            ++numofRecords;
        }
        fclose(textFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::MoveFirst () {
    myInternalVar->MoveFirst();
}

void DBFile::Heap::MoveFirst () {
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;
}

void DBFile::Sorted::MoveFirst () {
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;
}

int DBFile::Close () {
    return myInternalVar->Close();
}

//todo change
int DBFile::Heap::Close () {
    readingMode();
    return file.Close() ? 1 : 0;
}

//todo change
int DBFile::Sorted::Close () {
//    readingMode();
    return file.Close() ? 1 : 0;
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

void DBFile::Heap::Add (Record &addme) {
    writingMode();
    if (!writingPage.Append(&addme)) {
        file.AddPage(&writingPage, file.GetLength() - 1);
        writingPage.EmptyItOut();
        writingPage.Append(&addme);
    }
}

void DBFile::Sorted::Add (Record &rec) {
    writingMode();
    in.Insert(&rec);
}

int DBFile::GetNext (Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

//todo change
int DBFile::Heap::GetNext (Record &fetchme) {
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // If meets the end of file, return failure.
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        }
        else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

//todo change
int DBFile::Heap::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    int fetchState = 0;

    while(GetNext(fetchme)) {
        if(compEng.Compare(&fetchme, &literal, &cnf)){
            // Accepted record and break while.
            fetchState = 1;
            break;
        }
    }

    return fetchState;
}

//todo change
int DBFile::Sorted::GetNext (Record &fetchme) {
    // cout << "GetNext" << endl;
    if (mode == writing) {
        readingMode();
    }
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // If meets the end of file, return failure.
    while (!readingPage.GetFirst(&fetchme)) {
        if (page_index == file.GetLength() - 2) {
            return 0;
        }
        else {
            file.GetPage(&readingPage, ++page_index);
        }
    }
    return 1;
}

//todo change
int DBFile::Sorted::GetNext (Record &fetchme, CNF &cnf, Record &literal) {
    // cout << "GetNextByCNF" << endl;
    if (mode != query) {
        readingMode();
        mode = query;

        // Construct query OrderMaker
        delete myQueryOrder;
        delete literalQueryOrder;
        myQueryOrder = new OrderMaker();
        literalQueryOrder = new OrderMaker();
        for (int i = 0; i < myOrder->numAtts; ++i) {
            bool found = false;
            int literalIndex = 0;
            for (int j = 0; j < cnf.numAnds && !found; ++j) {
                for (int k = 0; k < cnf.orLens[j] && !found; ++k) {
                    if (cnf.orList[j][k].operand1 == Literal) {
                        if (cnf.orList[j][k].operand2 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt2 && cnf.orList[j][k].op == Equals) {
                            found = true;

                            myQueryOrder->whichAtts[myQueryOrder->numAtts] = myOrder->whichAtts[i];
                            myQueryOrder->whichTypes[myQueryOrder->numAtts] = myOrder->whichTypes[i];
                            ++myQueryOrder->numAtts;

                            literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                            literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                            ++literalQueryOrder->numAtts;
                        }
                        ++literalIndex;
                    }
                    else if (cnf.orList[j][k].operand2 == Literal) {
                        if (cnf.orList[j][k].operand1 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt1 && cnf.orList[j][k].op == Equals) {
                            found = true;

                            myQueryOrder->whichAtts[myQueryOrder->numAtts] = myOrder->whichAtts[i];
                            myQueryOrder->whichTypes[myQueryOrder->numAtts] = myOrder->whichTypes[i];
                            ++myQueryOrder->numAtts;

                            literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                            literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                            ++literalQueryOrder->numAtts;
                        }
                        ++literalIndex;
                    }
                }
            }
            if (!found) break;
        }

        // Binary search
        if (file.GetLength() == 1) return 0;
        else if (myQueryOrder->numAtts == 0) {
            page_index = 0;
            file.GetPage(&readingPage, 0);
        }
        else {
            off_t left = 0, right = file.GetLength() - 2;
            while (left < right - 1) {
                page_index = left + (right - left) / 2;
                file.GetPage(&readingPage, page_index);
                GetNext(fetchme);
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    right = page_index;
                }
                else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    right = page_index - 1;
                }
                else {
                    left = page_index;
                }
            }
            page_index = left;
            file.GetPage(&readingPage, page_index);
            while (GetNext(fetchme)) {
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    if(compEng.Compare(&fetchme, &literal, &cnf)){
                        // Accepted record and break while.
                        return 1;
                    }
                }
                else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    return 0;
                }
            }
        }
    }

    int fetchState = 0;
    while(GetNext(fetchme)) {
        if (myQueryOrder->numAtts > 0 && compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) != 0) {
            break;
        }
        if(compEng.Compare(&fetchme, &literal, &cnf)){
            // Accepted record and break while.
            fetchState = 1;
            break;
        }
    }

    return fetchState;
}
