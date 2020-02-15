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

        // Keep reading records from pipe and append to current page until current page is full.
        if (!page.Append(&record))
        {
            // If this is the last page, then start recordList process.
            if (++page_index == runlen)
            {
                sort(recordList.begin(), recordList.end(), comparator);
                dumpSortedList(recordList);

                // Restore default states.
                page_index = 0;
            }
            page.EmptyItOut();
            page.Append(&record);
        }

        recordList.emplace_back(rec);
    }

    // If recordList is empty, there isn't any record read from input pipe. Exit immediately
    if (recordList.empty()) {
        outPipe->ShutDown();
        file.Close();
//        remove(tempFilePath);
        pthread_exit(NULL);
    }
    // Sort last records that don't fill up a page.
    sort(recordList.begin(), recordList.end(), comparator);
    dumpSortedList(recordList);
}

void BigQ::phaseTwo() {
    vector<Page> tempPage(run_num);
    priority_queue<SortRec*, vector<SortRec*>, SortRecCmp> priorityQueue(*maker);

    for(int i = 0; i < run_num; i++){
        file.GetPage(&tempPage[i], blockStartOffset[i]++);
        SortRec* sortRec = new SortRec();
        sortRec->run_index = i;
        tempPage[i].GetFirst(&(sortRec->rec));
        priorityQueue.emplace(sortRec);
    }

    while(!priorityQueue.empty()){
        SortRec* sortRec = priorityQueue.top();
        priorityQueue.pop();
        int run_index = sortRec->run_index;
        outPipe->Insert(&(sortRec->rec));

        if(tempPage[run_index].GetFirst(&(sortRec->rec))){
            priorityQueue.emplace(sortRec);
        }
        else if(blockStartOffset[run_index] < blockEndOffset[run_index]){
            //Finsh this page, Get record from another page
            file.GetPage(&tempPage[run_index], blockStartOffset[run_index]++);
            tempPage[run_index].GetFirst(&(sortRec->rec));
            priorityQueue.emplace(sortRec);
        }else{
            delete sortRec;
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
    ++run_num;
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