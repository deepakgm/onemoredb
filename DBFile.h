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

class DBFile {
    File *f;
    Page *curPage;
    off_t curPageIndex;

    class GenericDBFile {

    protected:
        File file;
        Page writingPage;
        Page readingPage;
        off_t page_index = -1;
        ComparisonEngine compEng;

        mType mode = reading;

        virtual void writingMode() = 0;
        virtual void readingMode() = 0;

    public:
        virtual int Create(const char *fpath, void *startup) = 0;

        virtual int Open(const char *fpath) = 0;

        virtual int Close() = 0;

        virtual void Load(Schema &myschema, const char *loadpath) = 0;

        virtual void MoveFirst() = 0;

        virtual void Add(Record &addme) = 0;

        virtual int GetNext(Record &fetchme) = 0;

        virtual int GetNext(Record &fetchme, CNF &cnf, Record &literal) = 0;
    };

    // Inherit from GenericDBFile and used for heap file
    class Heap : public GenericDBFile {
    private:
        void writingMode();  // Switch to writing mode
        void readingMode();  // Switch to reading mode

    public:
        int Create(const char *fpath, void *startup);

        int Open(const char *fpath);

        int Close();

        void Load(Schema &myschema, const char *loadpath);

        void MoveFirst();

        void Add(Record &addme);

        int GetNext(Record &fetchme);

        int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    };

    class Sorted : public GenericDBFile {
    private:
        string fpath;  // file path of DBFile. Used to remove old file and rename tmp file during merging.

        int runLength;  // runlength for BigQ
        OrderMaker myOrder;  // Sort order for sorted file
        OrderMaker *myQueryOrder = nullptr;
        OrderMaker *literalQueryOrder = nullptr;

        Pipe in = Pipe(100);  // Push new added records to BigQ through in pipe.
        Pipe out = Pipe(100);  // Get sorted records from BigQ through out pipe.
        BigQ *bigQ = nullptr;

        void writingMode();  // Switch to writing mode
        void readingMode();  // Switch to reading mode

    public:
        int Create(const char *fpath, void *startup);

        int Open(const char *fpath);

        int Close();

        void Load(Schema &myschema, const char *loadpath);

        void MoveFirst();

        void Add(Record &addme);

        int GetNext(Record &fetchme);

        int GetNext(Record &fetchme, CNF &cnf, Record &literal);
    };

    GenericDBFile *myInternalVar;

public:
    DBFile();

    ~DBFile();

    int Create(const char *fpath, fType file_type, void *startup);

    int Open(const char *fpath);

    int Close();

    ComparisonEngine comparisonEngine;

    void Load(Schema &myschema, const char *loadpath);

    bool GetIsDirty();

    void MoveFirst();

    void Add(Record &record);

    int GetNext(Record &fetchme);

    int GetNext(Record &fetchme, CNF &cnf, Record &literal);

};

#endif
