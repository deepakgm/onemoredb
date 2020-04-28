#ifndef DBFILE_H
#define DBFILE_H

#include "TwoWayList.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "Comparison.h"
#include "ComparisonEngine.h"
#include "Pipe.h"
#include "BigQ.h"
#include "bpt.h"
#include <unistd.h>

typedef enum {
    heap, sorted, tree
} fType;
typedef enum {
    reading, writing
} mType;

struct SortInfo {
    OrderMaker *myOrder;  // Order of sorted DBFile
    int runLength;  // runlength for BigQ
};

class GenericDBFile {
protected:
    File file;
    Page readingPage;
    Page writingPage;
    off_t curPageIndex = -1;
    ComparisonEngine compEngine;

    //todo can be moved to sorteddbfile
    OrderMaker* myOrder;
    OrderMaker *queryOrder = NULL;
    OrderMaker *literalOrder = NULL;

    virtual void readingMode() = 0;
//    virtual void writingMode() = 0;

public:
    fType myType;
    int runLength;
    mType mode = reading;
    virtual void writingMode() = 0;
    GenericDBFile();
    int Create(const char *fpath, fType f_type, void *startup);
    int Open(const char *fpath);
    int Close();
    void MoveFirst();
    virtual void Load(Schema &myschema, const char *loadpath) = 0;
    virtual void Add(Record &addme) = 0;
    virtual int GetNext(Record &fetchme) = 0;
    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class Heap : public GenericDBFile {
private:
    void readingMode();
    void writingMode();

public:
    void Load(Schema &myschema, const char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class Sorted : public GenericDBFile {
private:
    Pipe inPipe = Pipe(100);
    Pipe outPipe = Pipe(100);
    BigQ *bigQ = NULL;

    void readingMode();
    void writingMode();
    void constructQueryOrder(CNF &cnf, Record &literal);
    int binarySearch(Record& fetchme, Record& literal);

public:
    void Load(Schema &myschema, const char *loadpath);
    void Add(Record &addme);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class BinFile {
private:
    int myFilDes;
    off_t curLength; //this was private in Chris's version
    off_t curReadOffset;
    const int RECORD_MAX_SIZE=200;

public:

    BinFile (){
        curLength=0;
        curReadOffset=0;
    }
    ~BinFile (){

    }

    void ResetReadOffset(){
        curReadOffset=0;
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

    int GetReocrd (Schema *mySchema,Record* rec,char* fbits,off_t length) {

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
                    return  -1;
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

        return j;
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

        delete[] bits;
    }

    int ReadNext (Schema* schema,Record* rec){
        char *bits = new (std::nothrow) char[RECORD_MAX_SIZE];

        lseek (myFilDes, curReadOffset, SEEK_SET);
        int res=read (myFilDes, bits, RECORD_MAX_SIZE);
        if(res<=0)
            return -1;

//        cout <<"**"<<bits<<"**"<<endl;
        res=GetReocrd(schema,rec,bits,RECORD_MAX_SIZE);
        if(res>0){
            curReadOffset+=res+1;
            res=1;
        }

        cout <<"cur off: "<<curReadOffset<<endl;
        delete[] bits;
        return res;
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

// class BTree : public GenericDBFile {
// private:
//     int hash=0;
//     int curlen;
//     bpt::bplus_tree* bdb;
//     void readingMode();
//     void writingMode();
// //    void constructQueryOrder(CNF &cnf, Record &literal);
// //    int binarySearch(Record& fetchme, Record& literal);

// public:
//     BinFile* binFile;
//     int curKey=0;
//     Schema* mySchema;
//     const char* fpath;

//     int rsize=20;
//     BTree(const char* fpath,Schema* schema);
//     void Create();
//     void Load(Schema &myschema, const char *loadpath);
//     void Add(Record &addme);
//     int GetNext(Record &fetchme);
//     int GetKey(bpt::key_t key,Record &fetchme);
//     int GetNext(Record &fetchme, CNF &cnf, Record &literal);
//     void MoveFirst();
// };


class DBFile {
public:
    DBFile();
    ~DBFile();
    GenericDBFile *myInternalVar;
    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();
    void Load(Schema &myschema, const char *loadpath);
    void MoveFirst();
    void Add(Record &record);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif