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

typedef enum {
    heap, sorted, tree
} fType;
typedef enum {
    reading, writing, query
} mType;

struct SortInfo {
    OrderMaker *myOrder;  // Order of sorted DBFile
    int runLength;  // runlength for BigQ
};

class GenericDBFile {

protected:
    File file;
    Page writingPage;
    Page readingPage;
    off_t page_index = -1;
    ComparisonEngine compEng;

    //todo can be moved to sorteddbfile
    OrderMaker* myOrder;
    int runLength;
    mType mode = reading;

    virtual void writingMode() = 0;
    virtual void readingMode() = 0;

public:
    GenericDBFile();

    int Create(const char *fpath, fType f_type, void *startup);

    int Open(const char *fpath);

    int Close();

    virtual void Load(Schema &myschema, const char *loadpath) = 0;

    void MoveFirst();

    virtual void Add(Record &addme) = 0;

    virtual int GetNext(Record &fetchme) = 0;

    virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
};

class Heap : public GenericDBFile {
private:
    void writingMode();  // Switch to writing mode
    void readingMode();  // Switch to reading mode

public:
    int curPageIndex;
    void Load(Schema &myschema, const char *loadpath);

    void Add(Record &addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class Sorted : public GenericDBFile {
private:
    string fpath;  // file path of DBFile. Used to remove old file and rename tmp file during merging.

    OrderMaker *myQueryOrder = NULL;
    OrderMaker *literalQueryOrder = NULL;

    Pipe in = Pipe(100);
    Pipe out = Pipe(100);

    BigQ *bigQ = NULL;

    void writingMode();  // Switch to writing mode
    void readingMode();  // Switch to reading mode

public:
    void Load(Schema &myschema, const char *loadpath);

    void Add(Record &addme);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

class DBFile {
public:
    DBFile();
    ~DBFile();
    GenericDBFile *myInternalVar;
    int Create(const char *fpath, fType file_type, void *startup);
    int Open(const char *fpath);
    int Close();
    void Load(Schema &myschema, const char *loadpath);
    bool GetIsDirty();
    void MoveFirst();
    void Add(Record &record);
    int GetNext(Record &fetchme);
    int GetNext(Record &fetchme, CNF &cnf, Record &literal);
};

#endif
