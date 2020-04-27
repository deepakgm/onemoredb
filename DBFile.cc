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
#include "bpt.h"
#include <stdlib.h>    // for itoa() call


using namespace std;


extern struct AndList *final;

DBFile::DBFile() {
}

DBFile::~DBFile() {
}

GenericDBFile::GenericDBFile() {
    readingPage = *(new Page());
    writingPage = *(new Page());
    myOrder = new(OrderMaker);
}



int GenericDBFile::Create(const char *f_path, fType type, void *startup) {
    file.Open(0, strdup(f_path));
    readingPage.EmptyItOut();
    writingPage.EmptyItOut();
    file.AddPage(&writingPage, -1);
    WriteMetaInfo(f_path, type, startup);
    if (startup != NULL) {
        myOrder = ((SortInfo *) startup)->myOrder;
        runLength = ((SortInfo *) startup)->runLength;
    }
//    if(type!=tree)
    MoveFirst();
    return 1;
}

int DBFile::Create(const char *f_path, fType type, void *startup) {
    if (type == heap) {
        myInternalVar = new Heap();
    } else if (type == sorted) {
        myInternalVar = new Sorted();
    } else if (type == tree) {
        myInternalVar = new BTree(f_path);
    }
    return myInternalVar->Create(f_path, type, startup);
}

int DBFile::Open(const char *f_path) {
    MetaInfo metaInfo = GetMetaInfo();

    if (f_path != metaInfo.binFilePath) {
        cout << "input file: " << f_path << endl;
        cout << "meta input file: " << metaInfo.binFilePath << endl;

        cout << "DbFile Open called without calling Create!!" << endl;
        WriteMetaInfo(f_path, heap, NULL);
        metaInfo = GetMetaInfo();
    }

    if (metaInfo.fileType == heap) {
        myInternalVar = new Heap();
    } else if (metaInfo.fileType == sorted) {
        myInternalVar = new Sorted();
    } else if (metaInfo.fileType == tree) {
        myInternalVar = new BTree(f_path);
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
    bigQ = new BigQ(inPipe, outPipe, *myOrder, runLength);
}

//todo change
void Sorted::readingMode() {
    if (mode == reading)
        return;

    mode = reading;
    MoveFirst();
    inPipe.ShutDown();
    writingPage.EmptyItOut();

    Record recFromPipe;
    Record recFromPage;
    bool pipeEmpty = outPipe.Remove(&recFromPipe) == 0;
    bool fileEmpty = GetNext(recFromPage) == 0;

    while (!pipeEmpty && !fileEmpty) {
        if (compEngine.Compare(&recFromPipe, &recFromPage, myOrder) <= 0) {
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
            pipeEmpty = outPipe.Remove(&recFromPipe) == 0;
        } else {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
            fileEmpty = GetNext(recFromPage) == 0;
        }
    }
    if (pipeEmpty && !fileEmpty) {
        if (!writingPage.Append(&recFromPage)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPage);
        }
        while (GetNext(recFromPage)) {
            if (!writingPage.Append(&recFromPage)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPage);
            }
        }
    } else if (!pipeEmpty) {
        if (!writingPage.Append(&recFromPipe)) {
            file.AddPage(&writingPage, file.GetLength() - 1);
            writingPage.EmptyItOut();
            writingPage.Append(&recFromPipe);
        }
        while (outPipe.Remove(&recFromPipe)) {
            if (!writingPage.Append(&recFromPipe)) {
                file.AddPage(&writingPage, file.GetLength() - 1);
                writingPage.EmptyItOut();
                writingPage.Append(&recFromPipe);
            }
        }
    }

    delete bigQ;

    if (!pipeEmpty || !fileEmpty)
        file.AddPage(&writingPage, file.GetLength() - 1);
}


void DBFile::Load(Schema &f_schema, const char *loadpath) {
    myInternalVar->Load(f_schema, loadpath);
}

void Heap::Load(Schema &f_schema, const char *loadpath) {
    writingMode();
    FILE *tableFile = fopen(loadpath, "r");
    if (tableFile == NULL) {
        cerr << "invalid table file" << endl;
        exit(-1);
    }

    Record tempRecord;
    long recordCount = 0;
    long pageCount = file.GetLength() - 1;

    int isNotFull = 0;
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
    fclose(tableFile);
    cout << "loaded " << recordCount << " records into " << pageCount << " pages." << endl;
}

void Sorted::Load(Schema &schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    int numofRecords = 0;
    if (tableFile == NULL) {
        cerr << "invalid load_path" << loadpath << endl;
        exit(1);
    } else {
        Record tempRecord = Record();
        writingMode();
        while (tempRecord.SuckNextRecord(&schema, tableFile)) {
            inPipe.Insert(&tempRecord);
            ++numofRecords;
        }
        fclose(tableFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}

void DBFile::MoveFirst() {
    myInternalVar->MoveFirst();
}

void Heap::MoveFirst() {
    readingMode();
    readingPage.EmptyItOut();
    delete queryOrder;
    delete literalOrder;
    curPageIndex = -1;
}

void Sorted::MoveFirst() {
    readingMode();
    readingPage.EmptyItOut();
    delete queryOrder;
    delete literalOrder;
    curPageIndex = -1;
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
    inPipe.Insert(&rec);
}

int DBFile::GetNext(Record &fetchme) {
    return myInternalVar->GetNext(fetchme);
}

int DBFile::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    return myInternalVar->GetNext(fetchme, cnf, literal);
}

int Heap::GetNext(Record &fetchme) {
    if (mode == writing) {
        readingMode();
    }
    while (!readingPage.GetFirst(&fetchme)) {
        if (curPageIndex == file.GetLength() - 2) {
            return 0;
        } else {
            file.GetPage(&readingPage, ++curPageIndex);
        }
    }
    return 1;
}

int Heap::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (mode == writing)
        readingMode();

    while (GetNext(fetchme))
        if (compEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}

int Sorted::GetNext(Record &fetchme) {
//    cout<<"ok"<<endl;
//    cout<<mode<<endl;
    if (mode == writing)
        readingMode();

//    cout<<"ok"<<endl;
    while (!readingPage.GetFirst(&fetchme)) {

//        cout<<"ok"<<endl;
        if (curPageIndex == file.GetLength() - 2)
            return 0;
        else
            file.GetPage(&readingPage, ++curPageIndex);
    }
    return 1;
}

//binary search to boil down the search to a specific page
int Sorted::binarySearch(Record &fetchme, Record &literal) {
    ComparisonEngine comparisonEngine;
    if (!GetNext(fetchme))
        return 0;
    int compare = comparisonEngine.Compare(&fetchme, queryOrder, &literal, literalOrder);
    if (compare > 0) return 0;
    else if (compare == 0) return 1;

    off_t low = 0, high = file.GetLength() - 2, mid = (low + high) / 2;
//    cout << "binarySearch:   low" << low << "    high: " << high << endl;
    while (low < mid) {
        mid = (low + high) / 2;
//        cout << "binarySearch:   low" << low << "    high: " << high << endl;
        file.GetPage(&readingPage, mid);
        if (!GetNext(fetchme))
            exit(1);
        compare = comparisonEngine.Compare(&fetchme, queryOrder, &literal, literalOrder);
        if (compare < 0) low = mid;
        else if (compare > 0) high = mid - 1;
        else high = mid;
    }
}

//construct ordermaker instances from the cnf
void Sorted::constructQueryOrder(CNF &cnf, Record &literal) {
    queryOrder = new OrderMaker();
    literalOrder = new OrderMaker();
    for (int i = 0; i < myOrder->numAtts; ++i) {
        bool flag = false;
        int index = 0;
        for (int j = 0; j < cnf.numAnds && !flag; ++j) {
            for (int k = 0; k < cnf.orLens[j] && !flag; ++k) {
                if (cnf.orList[j][k].operand1 == Literal) {
                    if (cnf.orList[j][k].operand2 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt2 &&
                        cnf.orList[j][k].op == Equals) {
                        flag = true;

                        queryOrder->whichAtts[queryOrder->numAtts] = myOrder->whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = myOrder->whichTypes[i];
                        ++queryOrder->numAtts;

                        literalOrder->whichAtts[literalOrder->numAtts] = index;
                        literalOrder->whichTypes[literalOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalOrder->numAtts;
                    }
                    ++index;
                } else if (cnf.orList[j][k].operand2 == Literal) {
                    if (cnf.orList[j][k].operand1 == Left && myOrder->whichAtts[i] == cnf.orList[j][k].whichAtt1 &&
                        cnf.orList[j][k].op == Equals) {
                        flag = true;

                        queryOrder->whichAtts[queryOrder->numAtts] = myOrder->whichAtts[i];
                        queryOrder->whichTypes[queryOrder->numAtts] = myOrder->whichTypes[i];
                        ++queryOrder->numAtts;

                        literalOrder->whichAtts[literalOrder->numAtts] = index;
                        literalOrder->whichTypes[literalOrder->numAtts] = cnf.orList[j][k].attType;
                        ++literalOrder->numAtts;
                    }
                    ++index;
                }
            }
        }
        if (!flag) break;
    }
}

int Sorted::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if (queryOrder == NULL || literalOrder == NULL) {  // check whether this is a fresh call
        constructQueryOrder(cnf, literal);

        if (queryOrder->numAtts != 0)
            if (!binarySearch(fetchme, literal)) // binary search to pin down to the page
                return 0;
    }

    while (GetNext(fetchme)) { // linear semyvalue1arch to inside the page
        if (compEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    }

    return 0;
}


//BTREE

/*
class BinFile {
private:
    int myFilDes;
    off_t curLength; //this was private in Chris's version

public:

    BinFile (){
        curLength=0;
    }
    ~BinFile (){

    }

    string GetString (Schema *mySchema,Record* rec) {

        int n = mySchema->GetNumAtts();
        Attribute *atts = mySchema->GetAtts();

        string  out="";
        // loop through all of the attributes
        for (int i = 0; i < n; i++) {
            int pointer = ((int *) rec->bits)[i + 1];

            if (atts[i].myType == Int) {
                int *myInt = (int *) &(rec->bits[pointer]);
                out.append(to_string(*myInt));
            } else if (atts[i].myType == Double) {
                double *myDouble = (double *) &(rec->bits[pointer]);
                out.append(to_string(*myDouble));
            } else if (atts[i].myType == String) {
                char *myString = (char *) &(rec->bits[pointer]);
                out.append(string(myString));
            }
            out.append("|");
        }

//    char arr[out.size()];
//    strncpy(arr,out.c_str(),out.size());
        return out;
    }

    void GetReocrd (Schema *mySchema,Record* rec,char* fbits,off_t length) {

//    length=length+1;

        int n = mySchema->GetNumAtts();
        Attribute *atts = mySchema->GetAtts();

        char *space = new (std::nothrow) char[length+ sizeof(int)*(n+1)];
        char *recSpace = new (std::nothrow) char[length+ sizeof(int)*(n+1)];

//    char* bits=rec->bits;

        // clear out the present record
        if (rec->bits != NULL)
            delete [] rec->bits;
        rec->bits = NULL;



        off_t j=0;
        int currentPosInRec = sizeof (int) * (n + 1);

        for (int i = 0; i < n; i++) {

            int len = 0;
            while (1) {
                int nextChar = fbits[j++];
                if (nextChar == '|')
                    break;
                else if (nextChar == EOF) {
                    delete [] space;
                    delete [] recSpace;
                    return;
                }

                space[len] = nextChar;
                len++;
            }

            // set up the pointer to the current attribute in the record
            ((int *) recSpace)[i + 1] = currentPosInRec;

            // null terminate the string
            space[len] = 0;
            len++;

            // then we convert the data to the correct binary representation
            if (atts[i].myType == Int) {
                *((int *) &(recSpace[currentPosInRec])) = atoi (space);
                currentPosInRec += sizeof (int);

            } else if (atts[i].myType == Double) {

                // make sure that we are starting at a double-aligned position;
                // if not, then we put some extra space in there
                while (currentPosInRec % sizeof(double) != 0) {
                    currentPosInRec += sizeof (int);
                    ((int *) recSpace)[i + 1] = currentPosInRec;
                }

                *((double *) &(recSpace[currentPosInRec])) = atof (space);
                currentPosInRec += sizeof (double);

            } else if (atts[i].myType == String) {

                // align things to the size of an integer if needed
                if (len % sizeof (int) != 0) {
                    len += sizeof (int) - (len % sizeof (int));
                }

                strcpy (&(recSpace[currentPosInRec]), space);
                currentPosInRec += len;

            }

        }

        // the last thing is to set up the pointer to just past the end of the reocrd
        ((int *) recSpace)[0] = currentPosInRec;

        currentPosInRec+= sizeof(int)*n;


        rec->bits= new char[currentPosInRec];

        memcpy (rec->bits, recSpace, currentPosInRec);

        delete [] space;
        delete [] recSpace;
    }

    off_t GetLength (){
        return curLength;
    }
    void Open (int fileLen, char *fName){
        int mode;
        if (fileLen == 0)
            mode = O_TRUNC | O_RDWR | O_CREAT;
        else
            mode = O_RDWR;

        // actually do the open
        myFilDes = open (fName, mode, S_IRUSR | S_IWUSR);

#ifdef verbose
        cout << "Opening file " << fName << " with "<< curLength << " pages.\n";
#endif

        // see if there was an error
        if (myFilDes < 0) {
            cerr << "BAD!  Open did not work for " << fName << "\n";
            exit (1);
        }

        // read in the buffer if needed
        if (fileLen != 0) {

            // read in the first few bits, which is the page size
            lseek (myFilDes, 0, SEEK_SET);
            read (myFilDes, &curLength, sizeof (off_t));

        } else {
            curLength = 0;
        }
    }

    void Write (Schema* schema,Record* rec,bpt::value_t* res){

        string str = GetString(schema,rec);
        off_t len=str.length()+1;
        char bits[len];

        strcpy(bits,str.c_str());

        cout << bits<<endl;

        lseek (myFilDes, curLength, SEEK_DATA);
        write (myFilDes, bits,len );
        res->len=curLength;
        res->offset=len;
        curLength+=len;
    };

    int Read (Schema* schema,Record* rec, off_t offset,off_t len){
        char *bits = new (std::nothrow) char[len];

        lseek (myFilDes, offset, SEEK_SET);
        read (myFilDes, bits, len);

//        cout <<"**"<<bits<<"**"<<endl;

        GetReocrd(schema,rec,bits,len);
    }

    int Close (){
        lseek (myFilDes, 0, SEEK_SET);
        write (myFilDes, &curLength, sizeof (off_t));

        // close the file
        close (myFilDes);

        // and return the size
        return curLength;
    }
};
*/



BTree::BTree(const char *fpath) {
//    bdb = new bpt::bplus_tree(fpath, size, true);
    bdb = new bpt::bplus_tree(fpath, true);
    binFile=new BinFile();
    binFile->Open(0,(char *)(string(fpath)+".bin").c_str());
    //todo change
    Schema nation("/home/deepak/Desktop/dbi/onemoredb/catalog","nation");
    this->mySchema=&nation;
}



void BTree::writingMode() {
    if (mode == writing) return;
    mode = writing;
//    bigQ = new BigQ(inPipe, outPipe, *myOrder, runLength);
}

//todo change
void BTree::readingMode() {
    if (mode == reading)
        return;
    mode = reading;
}


void BTree::Load(Schema &schema, const char *loadpath) {
    FILE *tableFile = fopen(loadpath, "r");
    int numofRecords = 0;
    if (tableFile == NULL) {
        cerr << "invalid load_path" << loadpath << endl;
        exit(1);
    } else {
        Record tempRecord = Record();
        while (tempRecord.SuckNextRecord(&schema, tableFile)) {
//            int pointer = ((int *) tempRecord.bits)[1];
//            int *myInt = (int *) &(tempRecord.bits[pointer]);
////            tempRecord.Print()
////        const char* key=to_string(hash++).c_str();
//        char key[32] = { 0 };
//        sprintf(key, "%d", hash++);
////        sprintf(key, "%d", *myInt);
//            cout <<"inserting key "<<key <<endl;
//            bdb->insert(key,tempRecord.bits);
//            cout <<"success key" <<endl;
            Add(tempRecord);
            tempRecord = Record();
            //            inPipe.Insert(&tempRecord);
            ++numofRecords;
        }
        fclose(tableFile);
        cout << "Loaded " << numofRecords << " records" << endl;
    }
}


void BTree::Add(Record &rec) {
    int pointer = ((int *) rec.bits)[1];
    char key[32] = {0};
    int *myInt = (int *) &(rec.bits[pointer]);
    sprintf(key, "%d", *myInt);
//    sprintf(key, "%d", hash++);

    bpt::value_t location;
    cout << "inserting " << key << endl;
    Schema nation("/home/deepak/Desktop/dbi/onemoredb/catalog","nation");
    binFile->Write(&nation, &rec, &location);
    cout <<" updating curlen: "<<location.len<<endl;
    bdb->insert(key, location);
}


void BTree::MoveFirst() {
    curKey = 0;
//    bdb->move_first();
}

int BTree::GetKey(bpt::key_t key,Record &fetchme) {
    bpt::value_t val;


    Schema nation("/home/deepak/Desktop/dbi/onemoredb/catalog","nation");

    bdb->search(key,&val);

//    cout <<"it "<<val.len <<endl;

    binFile->Read(&nation,&fetchme,val.len,val.offset);
}


int BTree::GetNext(Record &fetchme) {
        char key2[32] = {0};
    sprintf(key2, "%d",5);
    GetKey(key2,fetchme);
//    bpt::value_t val;
//    char key2[32] = {0};
//    sprintf(key2, "%d", curKey++);
//    if (bdb->search(key2, &val) == -1) {
//        return 0;
//    }
////    fetchme.CopyBits(val, rsize * 8);
//    return 1;
}


int BTree::GetNext(Record &fetchme, CNF &cnf, Record &literal) {
    if(cnf.orList[0][0].whichAtt1==1){
        //todo
    }
    if (mode == writing)
        readingMode();

    while (GetNext(fetchme))
        if (compEngine.Compare(&fetchme, &literal, &cnf)) {
            return 1;
        }
    return 0;
}