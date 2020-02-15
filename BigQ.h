#ifndef BIGQ_H
#define BIGQ_H
#include <pthread.h>
#include <iostream>
#include "Pipe.h"
#include "File.h"
#include "Record.h"
#include "Comparison.h"

#include <vector>
#include <queue>
#include <algorithm>

class BigQ
{

public:

    BigQ (Pipe &in, Pipe &out, OrderMaker &orderMaker, int runlen);
    ~ BigQ();

    static void* workerThread(void* arg);

private:
    Pipe* inPipe;
    Pipe* outPipe;
    OrderMaker* maker;
    int runlen;
    File file;
    int blockNum = 0;

    vector<off_t> blockStartOffset;
    vector<off_t> blockEndOffset;

    pthread_t worker_thread;

    void dumpSortedList(std::vector<Record*>& recordList);
    void phaseOne();
    void phaseTwo();

    class Compare{
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;

    public:
        Compare(OrderMaker& sortorder): CmpOrder(sortorder) {}
        bool operator()(Record* a, Record* b){return CmpEng.Compare(a, b, &CmpOrder) < 0;}
    };

    class IndexedRecord{
    public:
        Record record;
        int blockIndex;
    };

    class IndexedRecordCompare{
        ComparisonEngine comparisonEngine;
        OrderMaker& orderMaker;

    public:
        IndexedRecordCompare(OrderMaker& sortorder): orderMaker(sortorder) {}

        bool operator()(IndexedRecord* a, IndexedRecord* b){return comparisonEngine.Compare(&(a->record), &(b->record), &orderMaker) > 0;}
    };
};


#endif