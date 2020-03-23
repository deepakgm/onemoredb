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

void *DuplicateRemoval::workerThread(void *arg){
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
    n_pages = runlen;
}

void Sum::Run(Pipe &inPipe, Pipe &outPipe, Function &computeMe) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, computeMe);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

//void *Sum::workerThread(void *arg) {
//    OpArgs *opArgs = (OpArgs *) arg;
//
//    int totalIntSum = 0;
//    double totalDoubleSum = 0.0;
//
//    int recIntValue = 0;
//    double recDoubleValue = 0.0;
//
//    Record record;
////    Function *function = function;
//    Type type;
//
//    ostringstream outResult;
//    string sumResult;
//    Record recordResult;
//
//    //type check only for first record
//    if(opArgs->inPipe->Remove(&record))
//        type = opArgs->function->Apply(record, recIntValue, recDoubleValue);
//
//    if (type == Int) {
//        totalIntSum += recIntValue;
//        while (opArgs->inPipe->Remove(&record)) {
//            totalIntSum += recIntValue;
//        }
//    }else{
//        totalDoubleSum += recDoubleValue;
//        while (opArgs->inPipe->Remove(&record)) {
//            totalDoubleSum += recDoubleValue;
//        }
//    }
////    while (inPipe->Remove(&record)) {
////        type = function->Apply(record, recIntValue, recDoubleValue);
////        if (type == Int) {
////            intSum += intAttrVal;
////        } else {
////            doubleSum += doubleAttrVal;
////        }
////    }
//
//    // create output record
//    if (type == Int) {
//        outResult << totalIntSum;
//        sumResult = outResult.str();
//        sumResult.append("|");
//
//        Attribute IA = {"int", Int};
//        Schema output_schema("output_schema", 1, &IA);
//        recordResult.ComposeRecord(&output_schema, sumResult.c_str());
//    } else {
//
//        outResult << totalDoubleSum;
//        sumResult = outResult.str();
//        sumResult.append("|");
//
//        Attribute DA = {"double", Double};
////        attr.myType = Double;
//        Schema output_schema("output_schema", 1, &DA);
//        recordResult.ComposeRecord(&output_schema, sumResult.c_str());
//    }
////    cout<<recordResult.GetLength();
//    opArgs->outPipe->Insert(&recordResult);
//    opArgs->outPipe->ShutDown();
//    pthread_exit(NULL);
//}
void *Sum::workerThread(void *arg)  {
    OpArgs *s = (OpArgs*)arg;
    Record outRec, tmpRec;
    int intSum = 0;
    int intVal = 0;
    double doubleSum = 0.0;
    double doubleVal = 0.0;

    Attribute attr;
    attr.name = "SUM";
    stringstream output;

    if (s->function->returnsInt == 1) {
        Type valType = Int;
        while (s->inPipe->Remove(&tmpRec)) {
            valType = s->function->Apply(tmpRec, intVal, doubleVal);
            intSum = intSum + intVal;
        }
        output << intSum << "|";
    }
    else if (s->function->returnsInt == 0) {
        Type valType = Double;
        while (s->inPipe->Remove(&tmpRec)) {
            valType = s->function->Apply(tmpRec, intVal, doubleVal);
            doubleSum = doubleSum + doubleVal;
        }
        attr.myType = Double;
        output << doubleSum << "|";
    }
    else {
        cerr << "Error: Invalid type in Sum operation." << endl;
        exit(1);
    }

    Schema outSch("out_shema", 1, &attr);
    outRec.ComposeRecord(&outSch, output.str().c_str());
    s->outPipe->Insert(&outRec);

    s->outPipe->ShutDown();
    pthread_exit(NULL);
}

void Sum::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void Sum::Use_n_Pages(int runlen) {
    n_pages = runlen;
//todo
}

void GroupBy::Run(Pipe &inPipe, Pipe &outPipe, OrderMaker &groupAtts, Function &computeMe) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, groupAtts, computeMe,n_pages);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *GroupBy::workerThread(void *arg) {
    OpArgs* opArgs = (OpArgs*)arg;
    
    Record outRecArr[2];
    Record *start = NULL, *end = NULL;
    Record *temp = new Record;
    Record *dump = new Record;
    Type type;
    Pipe *pipe = new Pipe(100);
    BigQ bigq(*opArgs->inPipe, *pipe, *opArgs->orderMaker, 10);
    int arrbound = (opArgs->orderMaker->numAtts)+1;
    int groupId = 0, intSum = 0, sumIntRec, attsArr[arrbound];
    double doubleSum=0, sumDblRec;

    attsArr[0] = 0;
    for(int i = 1; i < arrbound; i++)
        attsArr[i] = opArgs->orderMaker->whichAtts[i-1];

    while(pipe->Remove(&outRecArr[groupId%2]) == 1)
    {
        start = end;
        end = &outRecArr[groupId%2];
        if(start != NULL && end != NULL)
        {
            if(opArgs->compEng->Compare(start, end, opArgs->orderMaker) != 0)
            {
                opArgs->function->Apply(*start, sumIntRec, sumDblRec);
                if(opArgs->function->returnsInt == 1)
                {
                    type = Int;
                    intSum = intSum + sumIntRec;
                }
                else
                {
                    type = Double;
                    doubleSum = doubleSum + sumDblRec;
                }
                int startint = ((int *)start->bits)[1]/sizeof(int) - 1;

                Attribute attr;
                stringstream output;
                if(type==Int){
                    attr.name="int";
                    attr.myType=Int;
                    output << intSum << "|";
                }
                else{
                    attr.name="double";
                    attr.myType=Double;
                    output << doubleSum << "|";
                }
                Schema out_sch ("sum", 1, &attr);
                dump->ComposeRecord(&out_sch,output.str().c_str());
                temp->MergeRecords(dump, start, 1, startint, attsArr, arrbound, 1);
                opArgs->outPipe->Insert(temp);
                intSum = 0;
                doubleSum = 0;
            }
            else
            {
                opArgs->function->Apply(*start, sumIntRec, sumDblRec);
                if(opArgs->function->returnsInt == 1)
                {
                    type = Int;
                    intSum = intSum + sumIntRec;
                }
                else
                {
                    type = Double;
                    doubleSum = doubleSum + sumDblRec;
                }
            }
        }
        groupId++;
    }

    opArgs->function->Apply(*end, sumIntRec, sumDblRec);
    if(opArgs->function->returnsInt == 1)
    {
        type = Int;
        intSum = intSum + sumIntRec;
    }
    else
    {
        type = Double;
        doubleSum = doubleSum + sumDblRec;
    }
    int startint = ((int *)start->bits)[1]/sizeof(int) - 1;

    Attribute attr;
    stringstream output;
    if(type==Int){
        attr.name="int";
        attr.myType=Int;
        output << intSum << "|";
    }
    else{
        attr.name="double";
        attr.myType=Double;
        output << doubleSum << "|";
    }
    Schema out_sch ("sum", 1, &attr);
    dump->ComposeRecord(&out_sch,output.str().c_str());
    temp->MergeRecords(dump, end, 1, startint, attsArr, arrbound, 1);
    opArgs->outPipe->Insert(temp);

    opArgs->outPipe->ShutDown();
    pthread_exit(NULL);
}

void GroupBy::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void GroupBy::Use_n_Pages(int runlen) {
    n_pages = runlen;
}

void WriteOut::Run(Pipe &inPipe, FILE *outFile, Schema &mySchema) {
    OpArgs *opArgs = new OpArgs(inPipe, outFile, mySchema);
    pthread_create(&thread, NULL, workerThread, opArgs);
}

void *WriteOut::workerThread(void *arg) {
    OpArgs *wo = (OpArgs *)arg;
    Record rec;

    while (wo->inPipe->Remove(&rec)) {
//        wo->writeOut(tmpRec);
        int numAtts = wo->schema->GetNumAtts();
        Attribute *atts = wo->schema->GetAtts();

        // loop through all of the attributes
        for (int i = 0; i < numAtts; i++) {

            int pointer = ((int *)rec.bits)[i + 1];

            // first is integer
            if (atts[i].myType == Int) {
                int *myInt = (int *) &(rec.bits[pointer]);
                fprintf(wo->outPipe, "%d", *myInt);

                // then is a double
            }
            else if (atts[i].myType == Double) {
                double *myDouble = (double *) &(rec.bits[pointer]);
                fprintf(wo->outPipe, "%f", *myDouble);

                // then is a character string
            }
            else if (atts[i].myType == String) {
                char *myString = (char *) &(rec.bits[pointer]);
                fprintf(wo->outPipe, "%s", myString);
            }
            fprintf(wo->outPipe, "|");
        }

        fprintf(wo->outPipe, "\n");
    }
    pthread_exit(NULL);
}


void WriteOut::WaitUntilDone() {
    pthread_join(thread, NULL);
}

void WriteOut::Use_n_Pages(int n) {
    n_pages = n;
}