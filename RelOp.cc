#include "RelOp.h"

void SelectFile::Run(DBFile &inFile, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inFile, outPipe, selOp, literal);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *SelectFile::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    while (opArgs->inFile->GetNext(temp)) {
        if (opArgs->compEng->Compare(&temp, opArgs->literal, opArgs->selOp)) {
            opArgs->outPipe->Insert(&temp);
        }
    }
    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void SelectFile::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectFile::Use_n_Pages(int runlen) {
    n_pages = runlen;
}


void SelectPipe::Run(Pipe &inPipe, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, selOp, literal);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *SelectPipe::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    while (opArgs->inPipe->Remove(&temp)) {
        if (opArgs->compEng->Compare(&temp, opArgs->literal, opArgs->selOp)) {
            opArgs->outPipe->Insert(&temp);
        }
    }
    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}


void SelectPipe::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void SelectPipe::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, keepMe, numAttsInput, numAttsOutput);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *Project::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

//    cout << opArgs->keepMe[1] << " " << opArgs->numAttsInput ;
    while (opArgs->inPipe->Remove(&temp)) {
        temp.Project(opArgs->keepMe, opArgs->numAttsOutput, opArgs->numAttsInput);
        opArgs->outPipe->Insert(&temp);
    }
    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Project::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Project::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void Join::Run(Pipe &inPipeL, Pipe &inPipeR, Pipe &outPipe, CNF &selOp, Record &literal) {
    OpArgs *opArgs = new OpArgs(inPipeL, inPipeR, outPipe, selOp, literal,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *Join::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;

    Record *tmpL = new Record();
    Record *tmpR = new Record();

    vector<Record *> leftList, rightList;
    OrderMaker orderMakerL, orderMakerR;

    if (opArgs->selOp->GetSortOrders(orderMakerL, orderMakerR)) {

        int leftAttrCount = 0, rightAttrCount = 0, totalAttrCount = 0;
        int * keepMe = NULL;
        Pipe* outPipeL= new Pipe(200);
        Pipe* outPipeR=new Pipe(200);
        BigQ bigQL = BigQ(*(opArgs->inPipeL), *outPipeL, orderMakerL, opArgs->n_pages);
        BigQ bigQR = BigQ(*(opArgs->inPipeR), *outPipeR, orderMakerR, opArgs->n_pages);
        bool leftEmpty = false, rightEmpty = false;

        if (!outPipeL->Remove(tmpL)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            leftAttrCount = ((int *) tmpL->bits)[1] / sizeof(int) - 1;
//            cout << "here .. "<<leftAttrCount<<endl;
        }

        if (!outPipeR->Remove(tmpR)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            rightAttrCount = ((int *) tmpR->bits)[1] / sizeof(int) - 1;
//            cout << "here .. "<<rightAttrCount<<endl;
        }

        totalAttrCount = leftAttrCount + rightAttrCount;
        keepMe = new int[totalAttrCount];
        for (int i = 0; i < leftAttrCount; ++i) {
            keepMe[i] = i;
        }
        for (int i = leftAttrCount; i < totalAttrCount; ++i) {
            keepMe[i] = i - leftAttrCount;
        }

        leftList.emplace_back(tmpL);
        tmpL = new Record();
        rightList.emplace_back(tmpR);
        tmpR = new Record();

        if (!outPipeL->Remove(tmpL)) {
            leftEmpty = true;
        }
        if (!outPipeR->Remove(tmpR)) {
            rightEmpty = true;
        }

        bool leftBigger = false, rightBigger = false;

        while (!leftEmpty && !rightEmpty) {

            if (!leftBigger) {
                while (!opArgs->compEng->Compare(leftList.back(), tmpL, &orderMakerL)) {
                    leftList.emplace_back(tmpL);
                    tmpL = new Record();
                    if (!outPipeL->Remove(tmpL)) {
                        leftEmpty = true;
                        break;
                    }
                }
            }

            if (!rightBigger) {
                while (!opArgs->compEng->Compare(rightList.back(), tmpR, &orderMakerR)) {
                    rightList.emplace_back(tmpR);
                    tmpR = new Record();
                    if (!outPipeR->Remove(tmpR)) {
                        rightEmpty = true;
                        break;
                    }
                }
            }

            if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                flushList(rightList);
                leftBigger = true;
                rightBigger = false;
            } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                flushList(leftList);
                leftBigger = false;
                rightBigger = true;
            } else {
                for (auto recL : leftList) {
                    for (auto recR : rightList) {
                        if (opArgs->compEng->Compare(recL, recR, opArgs->literal, opArgs->selOp)) {
                            Record mergedRec, cpRecR;
                            cpRecR.Copy(recR);
                            mergedRec.MergeRecords(recL, &cpRecR, leftAttrCount, rightAttrCount, keepMe, totalAttrCount,
                                                   leftAttrCount);
                            opArgs->outPipe->Insert(&mergedRec);
                        }
                    }
                }

                flushList(leftList);
                flushList(rightList);
                leftBigger = false;
                rightBigger = false;
            }

            if (!leftEmpty && !leftBigger) {
                leftList.emplace_back(tmpL);
                tmpL = new Record();
                if (!outPipeL->Remove(tmpL)) {
                    leftEmpty = true;
                }
            }
            if (!rightEmpty && !rightBigger) {
                rightList.emplace_back(tmpR);
                tmpR = new Record();
                if (!outPipeR->Remove(tmpR)) {
                    rightEmpty = true;
                }
            }
        }

        if (!leftList.empty() && !rightList.empty()) {
            while (!leftEmpty) {
                if (!leftBigger) {
                    while (!opArgs->compEng->Compare(leftList.back(), tmpL, &orderMakerL)) {
                        leftList.emplace_back(tmpL);
                        tmpL = new Record();
                        if (!outPipeL->Remove(tmpL)) {
                            leftEmpty = true;
                            break;
                        }
                    }
                }
                if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                    flushList(rightList);
                    leftBigger = true;
                    rightBigger = false;
                    break;
                } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                    flushList(leftList);
                    leftBigger = false;
                    rightBigger = true;
                } else {
                    for (auto recL : leftList) {
                        for (auto recR : rightList) {
                            if (opArgs->compEng->Compare(recL, recR, opArgs->literal, opArgs->selOp)) {
                                Record mergedRec, cpRecR;
                                cpRecR.Copy(recR);
                                mergedRec.MergeRecords(recL, &cpRecR, leftAttrCount, rightAttrCount, keepMe,
                                                       totalAttrCount, leftAttrCount);
                                opArgs->outPipe->Insert(&mergedRec);
                            }
                        }
                    }

                    flushList(leftList);
                    flushList(rightList);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }

                if (!leftEmpty && !leftBigger) {
                    leftList.emplace_back(tmpL);
                    tmpL = new Record();
                    if (!outPipeL->Remove(tmpL)) {
                        leftEmpty = true;
                    }
                }
            }
            while (!rightEmpty) {
                if (!rightBigger) {
                    while (!opArgs->compEng->Compare(rightList.back(), tmpR, &orderMakerR)) {
                        rightList.emplace_back(tmpR);
                        tmpR = new Record();
                        if (!outPipeR->Remove(tmpR)) {
                            rightEmpty = true;
                            break;
                        }
                    }
                }
                if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) > 0) {
                    flushList(rightList);
                    leftBigger = true;
                    rightBigger = false;
                } else if (opArgs->compEng->Compare(leftList.back(), &orderMakerL, rightList.back(), &orderMakerR) < 0) {
                    flushList(leftList);
                    leftBigger = false;
                    rightBigger = true;
                    break;
                } else {
                    for (auto recL : leftList) {
                        for (auto recR : rightList) {
                            if (opArgs->compEng->Compare(recL, recR, opArgs->literal, opArgs->selOp)) {
                                Record mergedRec, cpRecR;
                                cpRecR.Copy(recR);
                                mergedRec.MergeRecords(recL, &cpRecR, leftAttrCount, rightAttrCount, keepMe,
                                                       totalAttrCount, leftAttrCount);
                                opArgs->outPipe->Insert(&mergedRec);
                            }
                        }
                    }

                    flushList(leftList);
                    flushList(rightList);
                    leftBigger = false;
                    rightBigger = false;
                    break;
                }

                if (!rightEmpty && !rightBigger) {
                    rightList.emplace_back(tmpR);
                    tmpR = new Record();
                    if (!outPipeR->Remove(tmpR)) {
                        rightEmpty = true;
                    }
                }
            }
        }
    }
    else {
        int leftAttrCount = 0, rightAttrCount = 0, totalAttrCount = 0;
        int *keepMe = NULL;

        if (!opArgs->inPipeL->Remove(tmpL)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            leftAttrCount = *((int *) tmpL->bits);
            leftList.emplace_back(tmpL);
            tmpL = new Record();
        }

        if (!opArgs->inPipeR->Remove(tmpR)) {
            opArgs->outPipe->ShutDown();
            pthread_exit(NULL);
        } else {
            rightAttrCount = *((int *) tmpR->bits);
            rightList.emplace_back(tmpR);
            tmpR = new Record();
        }

        totalAttrCount = leftAttrCount + rightAttrCount;
        keepMe = new int[totalAttrCount];

        for (int i = 0; i < leftAttrCount; ++i) {
            keepMe[i] = i;
        }
        for (int i = leftAttrCount; i < totalAttrCount; ++i) {
            keepMe[i] = i - leftAttrCount;
        }

        while (opArgs->inPipeL->Remove(tmpL)) {
            leftList.emplace_back(tmpL);
            tmpL = new Record();
        }
        while (opArgs->inPipeR->Remove(tmpR)) {
            rightList.emplace_back(tmpR);
            tmpR = new Record();
        }
        for (auto recL : leftList) {
            for (auto recR : rightList) {
                if (opArgs->compEng->Compare(recL, recR, opArgs->literal, opArgs->selOp)) {
                    Record mergedRec;
                    mergedRec.MergeRecords(tmpL, tmpR, leftAttrCount, rightAttrCount, keepMe, totalAttrCount,
                                           leftAttrCount);
                    opArgs->outPipe->Insert(&mergedRec);
                }
            }
        }
    }

    flushList(leftList);
    flushList(rightList);
    delete tmpL;
    tmpL = NULL;
    delete tmpR;
    tmpR = NULL;


    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Join::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Join::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void DuplicateRemoval::Run(Pipe &inPipe, Pipe &outPipe, Schema &mySchema) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, mySchema,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *DuplicateRemoval::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp1;
    Record temp2;
    OrderMaker orderMaker = OrderMaker(opArgs->mySchema);


    Pipe *tempPipe = new Pipe(100);
    BigQ bigQ = BigQ(*(opArgs->inPipe), *tempPipe, orderMaker, opArgs->n_pages);


    if (tempPipe->Remove(&temp1)) {
        while (tempPipe->Remove(&temp2)) {
            if (opArgs->compEng->Compare(&temp1, &temp2, &orderMaker)) {
                opArgs->outPipe->Insert(&temp1);
                temp1.Consume(&temp2);
            }
        }
        opArgs->outPipe->Insert(&temp1);
    }

    tempPipe->ShutDown();
    delete tempPipe;

    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void DuplicateRemoval::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void DuplicateRemoval::Use_n_Pages(int runlen) {
//    n_pages = runlen;
//todo
}

