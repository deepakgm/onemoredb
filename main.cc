#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"
#include <chrono>
#include "bpt.h"

using namespace std;



/*
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



int main() {

    /*BinFile* binFile=new BinFile();
    binFile->Open(0,"/home/deepak/Desktop/dbi/onemoredb/temp/binfile1");
    binFile->Close();

    Schema schema("/home/deepak/Desktop/dbi/onemoredb/catalog","nation");

    Record rec;
    FILE *tableFile = fopen("/home/deepak/Desktop/dbi/tpch-dbgen/10MB/nation.tbl", "r");


    bpt::value_t myvalue1;

    char res[20]={0};

    auto bdb = new bpt::bplus_tree("/home/deepak/Desktop/dbi/onemoredb/temp/bpttemp1", true);


    rec.SuckNextRecord(&schema,tableFile);
    binFile->Write(&schema,&rec,&myvalue1);
//    cout <<"it "<<res <<endl;
    char key[32] = { 0 };
    sprintf(key, "%d", 0);
    bdb->insert(key,myvalue1);


    rec.SuckNextRecord(&schema,tableFile);
    binFile->Write(&schema,&rec,&myvalue1);
//    cout <<"it "<<res <<endl;
    sprintf(key, "%d", 2);
    bdb->insert(key,myvalue1);


    bpt::value_t val;
    bdb->search(key,&val);

    cout <<"it "<<val.len <<endl;


    Record temp;

    binFile->Read(&schema,&temp,65,GetString(&schema,&rec).length());

    temp.Print(&schema);
*/







    DBFile *dbFile = new DBFile();

    SortInfo *sortInfo = new SortInfo();

    Schema nation("/home/deepak/Desktop/dbi/onemoredb/catalog", "nation");
    OrderMaker *omg = new OrderMaker(&nation);
    sortInfo->myOrder = omg;


    dbFile->Create("/home/deepak/Desktop/dbi/onemoredb/temp/tmpf1", tree, (void *) sortInfo);
    dbFile->myInternalVar->Load(nation, "/home/deepak/Desktop/dbi/tpch-dbgen/10MB/nation.tbl");

    cout << "reading the data.." << endl;
//
    Record tempRecord;
    dbFile->myInternalVar->GetNext(tempRecord);
    tempRecord.Print(&nation);
//    while (dbFile->myInternalVar->GetNext(tempRecord) == 1) {
//        tempRecord.Print(&nation);
//    }
    cout << "done" << endl;
}