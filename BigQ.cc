#include <climits>
#include <unistd.h>
#include <cstring>
#include "BigQ.h"

using namespace std;

BigQ::BigQ(Pipe &in, Pipe &out, OrderMaker &orderMaker, int runlen) {
    inPipe = &in;
    outPipe = &out;
    runlen = runlen;
    maker = &orderMaker;
    pthread_create(&worker_thread, NULL, workerThread, (void *) this);
}

BigQ::~BigQ() {
}

void BigQ::phaseOne() {
    Record record;
    Compare comparator=Compare(*maker);
    const long maxSize = PAGE_SIZE * runlen;
    long curSize=0;

    vector<Record*> recordList;
    int page_index = 0;  // Used to indicate which page to store next record read from pipe.
    Page page;  // Used to temporarily store records.

    while (inPipe->Remove(&record))
    {
        Record* rec = new Record();
        rec->Copy(&record);  // Pushed into vector to sort
        curSize+=rec->GetLength();

        if(curSize>=maxSize){
            curSize=0;
                sort(recordList.begin(), recordList.end(), comparator);
                dumpSortedList(recordList);
        }
        recordList.emplace_back(rec);
    }

    if (recordList.empty()) {
        outPipe->ShutDown();
        file.Close();
        pthread_exit(NULL);
    }

    sort(recordList.begin(), recordList.end(), comparator);
    dumpSortedList(recordList);
}

void BigQ::phaseTwo() {
    vector<Page> tempPage(blockNum);
    priority_queue<IndexedRecord*, vector<IndexedRecord*>, IndexedRecordCompare> priorityQueue(*maker);

    for(int i = 0; i < blockNum; i++){
        file.GetPage(&tempPage[i], blockStartOffset[i]++);
        IndexedRecord* indexedRecord = new IndexedRecord();
        indexedRecord->blockIndex = i;
        tempPage[i].GetFirst(&(indexedRecord->record));
        priorityQueue.emplace(indexedRecord);
    }

    while(!priorityQueue.empty()){
        IndexedRecord* indexedRecord = priorityQueue.top();
        priorityQueue.pop();
        int blockIndex = indexedRecord->blockIndex;
        outPipe->Insert(&(indexedRecord->record));

        if(tempPage[blockIndex].GetFirst(&(indexedRecord->record))){
            priorityQueue.emplace(indexedRecord);
        }
        else if(blockStartOffset[blockIndex] < blockEndOffset[blockIndex]){
            file.GetPage(&tempPage[blockIndex], blockStartOffset[blockIndex]++);
            tempPage[blockIndex].GetFirst(&(indexedRecord->record));
            priorityQueue.emplace(indexedRecord);
        }else{
            delete indexedRecord;
        }
    }
}


void BigQ::dumpSortedList(vector<Record *> &recordList) {
    Page outPage;
    blockStartOffset.push_back(file.GetLength() - 1);
    for (auto rec : recordList) {
        if (!outPage.Append(rec)) {
            file.AddPage(&outPage, file.GetLength() - 1);
            outPage.EmptyItOut();
            outPage.Append(rec);
            delete rec;
            rec = NULL;
        }
    }
    file.AddPage(&outPage, file.GetLength() - 1);
    blockEndOffset.push_back(file.GetLength() - 1);
    ++blockNum;
    recordList.clear();
}

void *BigQ::workerThread(void *arg) {
    BigQ *bigQ = (BigQ *) arg;
    char tempFilePath[PATH_MAX];
    if (getcwd(tempFilePath, sizeof(tempFilePath)) != NULL) {
        strcat(tempFilePath, "/temp/myfile");
    } else {
        cerr << "error while getting curent dir" << endl;
        exit(-1);
    }

    bigQ->file.Open(0, tempFilePath);
    bigQ->file.AddPage(new Page(), -1);

    bigQ->phaseOne();
    bigQ->phaseTwo();

    bigQ->outPipe->ShutDown();
    bigQ->file.Close();
    remove(tempFilePath);
    pthread_exit(NULL);
}