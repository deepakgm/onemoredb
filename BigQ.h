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
    void phaseOne();
    void phaseTwo();
    File file;  // Used to store sorted run.
    int run_num = 0;  // Number of runs.

    vector<off_t> blockStartOffset;
    vector<off_t> blockEndOffset;

    pthread_t worker_thread;
    int pageOffset=0;
    int totalRecords=0;

    void dumpSortedList(std::vector<Record*>& recordList);  // Used for internal recordList and storing run to file
    string randomStrGen(int length);


    // Used for internal sort.
    class Compare
    {
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;

    public:

        Compare(OrderMaker& sortorder): CmpOrder(sortorder) {}
        bool operator()(Record* a, Record* b){return CmpEng.Compare(a, b, &CmpOrder) < 0;}
    };

    // When removing an element from priority queue, we need to know which run it belongs to.
    class SortRec
    {

    public:

        Record rec;
        int run_index;
    };

    // Used for priority queue.
    class SortRecCmp
    {
        ComparisonEngine CmpEng;
        OrderMaker& CmpOrder;

    public:

        SortRecCmp(OrderMaker& sortorder): CmpOrder(sortorder) {}

        bool operator()(SortRec* a, SortRec* b)
        {
            return CmpEng.Compare(&(a->rec), &(b->rec), &CmpOrder) > 0;
        }
    };
};


#endif