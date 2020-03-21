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
// todo
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
    //todo
}

void Project::Run(Pipe &inPipe, Pipe &outPipe, int *keepMe, int numAttsInput, int numAttsOutput) {
    OpArgs *opArgs = new OpArgs(inPipe, outPipe, keepMe, numAttsInput,numAttsOutput);
    cout << "here " <<endl;
    pthread_create(&thread, NULL, workerThread, opArgs);
    cout << "here " <<endl;
}

void *Project::workerThread(void *arg) {
    OpArgs *opArgs = (OpArgs *) arg;
    Record temp;

    cout << "here " <<endl;
    cout << opArgs->keepMe << " " << opArgs->numAttsOutput ;
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
//todo
}

