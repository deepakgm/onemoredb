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
//    MoveFirst();
    in.ShutDown();
    File tempFile;
    char* tempFilePath=GetTempPath();
    tempFile.Open(0, tempFilePath);
    writingPage.EmptyItOut();
    tempFile.AddPage(&writingPage, -1);

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
                tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
            // If one queue is empty, end merging.
            if (!out.Remove(&recFromPipe)) pipeEmpty = true;
        } else {
            if (!writingPage.Append(&recFromPage)) {
                tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
            if (!GetNext(recFromPage)) fileEmpty = true;
        }
    }
    // Determine which queue runs out and append records of the other queue to the end or new file.
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&recFromPage)) {
            tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPage);
        }
        while (GetNext(recFromPage)) {
            if (!writingPage.Append(&recFromPage)) {
                tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
        }
    } else if (!pipeEmpty && fileEmpty) {
        if (!writingPage.Append(&recFromPipe)) {
            tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPipe);
        }
        while (out.Remove(&recFromPipe)) {
            if (!writingPage.Append(&recFromPipe)) {
                tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
        }
    }
    if (!pipeEmpty || !fileEmpty) tempFile.AddPage(&writingPage, tempFile.GetLength() - 1);

    tempFile.Close();
    remove(fpath.c_str());
    rename(tempFilePath, fpath.c_str());

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
    long pageCount = file.GetLength()-1;

    int isNotFull =0;
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
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}

void Sorted::Load(Schema &myschema, const char *loadpath) {
    FILE *textFile = fopen(loadpath, "r");
    int numofRecords = 0;
    if (textFile == NULL) {
        cerr << "invalid load_path" << loadpath << endl;
        exit(1);
    } else {
        Record record = Record();
        while (record.SuckNextRecord(&myschema, textFile)) {
            Add(record);
            ++numofRecords;
        }
        fclose(textFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::MoveFirst() {
    myInternalVar->MoveFirst();
}

void Heap::MoveFirst() {
    readingMode();
    readingPage.EmptyItOut();
    page_index = -1;
}

void Sorted::MoveFirst() {
    readingMode();
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

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    while (GetNext(fetchme))
        if (compEng.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}

//todo change
int Sorted::GetNext(Record &fetchme) {
    // cout << "GetNext" << endl;
    if (mode == writing) {
        readingMode();
    }
    // Get record from reading buffer. In case of empty, read a new page to buffer.
    // If meets the end of file, return failure.
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
                        if (cnf.orList[j][k].operand2 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt2 &&
                            cnf.orList[j][k].op == Equals) {
                            found = true;

                            myQueryOrder->whichAtts[myQueryOrder->numAtts] = myOrder->whichAtts[i];
                            myQueryOrder->whichTypes[myQueryOrder->numAtts] = myOrder->whichTypes[i];
                            ++myQueryOrder->numAtts;

                            literalQueryOrder->whichAtts[literalQueryOrder->numAtts] = literalIndex;
                            literalQueryOrder->whichTypes[literalQueryOrder->numAtts] = cnf.orList[j][k].attType;
                            ++literalQueryOrder->numAtts;
                        }
                        ++literalIndex;
                    } else if (cnf.orList[j][k].operand2 == Literal) {
                        if (cnf.orList[j][k].operand1 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt1 &&
                            cnf.orList[j][k].op == Equals) {
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
        } else {
            off_t left = 0, right = file.GetLength() - 2;
            while (left < right - 1) {
                page_index = left + (right - left) / 2;
                file.GetPage(&readingPage, page_index);
                GetNext(fetchme);
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    right = page_index;
                } else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    right = page_index - 1;
                } else {
                    left = page_index;
                }
            }
            page_index = left;
            file.GetPage(&readingPage, page_index);
            while (GetNext(fetchme)) {
                if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) == 0) {
                    if (compEng.Compare(&fetchme, &literal, &cnf)) {
                        return 1;
                    }
                } else if (compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) > 0) {
                    return 0;
                }
            }
        }
    }

    int fetchState = 0;
    while (GetNext(fetchme)) {
        if (myQueryOrder->numAtts > 0 && compEng.Compare(&fetchme, myQueryOrder, &literal, literalQueryOrder) != 0) {
            break;
        }
        if (compEng.Compare(&fetchme, &literal, &cnf)) {
            // Accepted record and break while.
            fetchState = 1;
            break;
        }
    }

    return fetchState;
}
