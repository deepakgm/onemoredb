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

int BigQ :: dumpSortedList(vector<Record> & recordList){
    int startOffSet = pageOffset;
    Page page;
    Record record;
    for (vector<Record>::iterator it = recordList.begin(); it < recordList.end(); it++)
    {
        totalRecords++;
        record.Consume(&(*it));
        if(0 == page.Append(&record))
        {
            file.AddPage(&page, pageOffset++);
            page.EmptyItOut();
            page.Append(&record);
        }
    }
    file.AddPage(&page, pageOffset++);

    off_t pageEnd = pageOffset;
    // cout << "inserted " <<  pageEnd - pageStart << " pages" << endl;
    runLocations.push_back(make_pair(startOffSet,pageEnd));
    return (int)(pageEnd-startOffSet);
}

void BigQ::phaseOne() {
    vector<Record *> sorting;
    Record record;

    const long maxSize = PAGE_SIZE * runlen;
    long curSize=0;

    while (inPipe->Remove(&record)) {
        Record *rec = new Record();
        rec->Copy(&record);

        curSize+=rec->GetLength();

        if(curSize>=maxSize){
            curSize=0;
            sortAndSaveRun(sorting);
        }

//        if (!page.Append(&record)) {
//            // If this is the last page, then start sorting process.
//            if (++page_index == bigQ->runlen) {
//                bigQ->sortAndSaveRun(sorting);
//
//                // Restore default states.
//                page_index = 0;
//            }
//            page.EmptyItOut();
//            page.Append(&record);
//        }

        sorting.emplace_back(rec);
    }

    // If sorting is empty, there isn't any record read from input pipe. Exit immediately
    if (sorting.empty()) {
        outPipe->ShutDown();
        file.Close();
//        remove(tempFilePath);
        pthread_exit(NULL);
    }
    sortAndSaveRun(sorting);

}



//void BigQ::PhaseTwoPriorityQueue(void){
//    cout << endl << endl << "Priority Queue Merge of sorted runs" << endl;
//    {
//        vector<Run> runs;
//        runs.reserve(runCount);
//        // cout << "initializing runs" << endl;
//        for (int i = 0; i < runCount; i++)
//        {
//            // cout << "Run " << i;
//            runs.push_back(Run(i,runLocations[i].first,runLocations[i].second, &partiallySortedFile));
//            // cout << " initialized" << endl;
//        }
//
//        for (int i = 0; i < runCount; i++)
//        {
//            // runs[i].print();
//        }
//
//        std::priority_queue<TaggedRecord, vector<TaggedRecord>, TaggedRecordCompare> mins (sortorder);
//
//        // initialize minimums
//        // for each run, get the first guy.
//        // cout << "initializing minimums" << endl;
//        // minimums.reserve(runCount);
//        for (int i = 0; i < runCount; i++)
//        {
//            // cout << "minimum " << i;
//            Record tr;
//            runs[i].getNextRecord(tr);
//            // minimums.push_back(tr);
//            // cout << "push" << endl;
//            mins.push(TaggedRecord(tr,i));
//            // cout << "initialized " << endl;
//        }
//        // now find the minimum guy and put it in the pipe
//        // do this totalRecords times
//        // cout << "putting stuff in the pipe" << endl;
//        // Compare c = Compare(sortorder);
//        {
//            int runsLeft = runCount;
//            int recordsOut = 0;
//            for (int r = totalRecords ; r > 0; r--)
//            {
//                TaggedRecord TRtr(mins.top());
//                Record tr(TRtr.r);
//
//                int run = TRtr.getRun();
//
//                recordsOut++;
//                out.Insert(&tr);
//                mins.pop();
//                bool valid = runs[run].getNextRecord(tr);
//                if (valid)
//                {
//                    mins.push(TaggedRecord(tr,run));
//                }
//                else
//                {
//                    // cout << "run empty, got to get rid of it" << endl;
//                    runsLeft--;
//                }
//            }
//            assert(recordsOut == totalRecords);
//            // cout << "runs left = "<< runsLeft << endl;
//            assert (0 == runsLeft);
//        }
//    }
//    // cout << runCount << " runs in " << partiallySortedFile.GetLength() << " total pages" << endl;
//    // cout << "runlen of " << runlen << endl;
//    // cout << "phase two complete" << endl;
//}

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

    // Used to temporarily store records read from pipe and measure runlen before sorting.
    vector<Record*> sorting;  //  Used to temporarily store all records and then sort.
    int page_index = 0;  // Used to indicate which page to store next record read from pipe.

    Record record;  // Used to record read from pipe.
    Page page;  // Used to temporarily store records.

    while (bigQ->inPipe->Remove(&record))
    {
        Record* rec = new Record();
        rec->Copy(&record);  // Pushed into vector to sort

        // Keep reading records from pipe and append to current page until current page is full.
        if (!page.Append(&record))
        {
            // If this is the last page, then start sorting process.
            if (++page_index == bigQ->runlen)
            {
                bigQ->sortAndSaveRun(sorting);

                // Restore default states.
                page_index = 0;
            }
            page.EmptyItOut();
            page.Append(&record);
        }

        sorting.emplace_back(rec);
    }

    // If sorting is empty, there isn't any record read from input pipe. Exit immediately
    if (sorting.empty()) {
        bigQ->outPipe->ShutDown();
        bigQ->file.Close();
        remove(tempFilePath);
        pthread_exit(NULL);
    }
    // Sort last records that don't fill up a page.
    bigQ->sortAndSaveRun(sorting);

    // construct priority queue over sorted runs and dump sorted data
    // into the out pipe

    vector<Page> tmpPage(bigQ->run_num);  // Used to store current page of each run.
    priority_queue<SortRec*, vector<SortRec*>, SortRecCmp> sortPriQueue(*bigQ->maker);

    // Get the first record of each run and push them into priority queue.
    for(int i = 0; i < bigQ->run_num; i++)
    {
        //Get the first page of run
        bigQ->file.GetPage(&tmpPage[i], bigQ->startOffset[i]++);
        //Get the first record of page
        SortRec* sortRec = new SortRec();
        sortRec->run_index = i;
        tmpPage[i].GetFirst(&(sortRec->rec));
        sortPriQueue.emplace(sortRec);
    }

    // pop top element from priority queue and insert it into output pipe. Take record from the same run as popped element
    while(!sortPriQueue.empty())
    {
        //Read first record of page and insert
        SortRec* sortRec = sortPriQueue.top();
        sortPriQueue.pop();
        int run_index = sortRec->run_index;
        bigQ->outPipe->Insert(&(sortRec->rec));

        //Read other records from page
        if(tmpPage[run_index].GetFirst(&(sortRec->rec)))
        {
            sortPriQueue.emplace(sortRec);
        }
        else if(bigQ->startOffset[run_index] < bigQ->endOffset[run_index])
        {
            //Finsh this page, Get record from another page
            bigQ->file.GetPage(&tmpPage[run_index], bigQ->startOffset[run_index]++);
            tmpPage[run_index].GetFirst(&(sortRec->rec));
            sortPriQueue.emplace(sortRec);
        }
        else
        {
            delete sortRec;
        }
    }

    // finally shut down the out pipe
    bigQ->outPipe->ShutDown();
    bigQ->file.Close();
    remove(tempFilePath);
    pthread_exit(NULL);
}

void BigQ::sortAndSaveRun(vector<Record *> &recordList) {
    sort(recordList.begin(), recordList.end(), Compare(*maker));
    Page outPage;
    startOffset.push_back(file.GetLength() - 1);
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
    endOffset.push_back(file.GetLength() - 1);
    ++run_num;
    recordList.clear();
}

string BigQ::randomStrGen(int length) {
    static string charset = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890";
    string result;
    result.resize(length);

    for (int i = 0; i < length; i++) {
        result[i] = charset[rand() % charset.length()];
    }

    return result;
}