#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"


using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct AndList *final;

int main() {
    // try to parse the CNF
    cout << "Enter in your CNF: ";
    if (yyparse() != 0) {
        cout << "Can't parse your CNF.\n";
        exit (1);
    }

    // suck up the schema from the file
    Schema lineitem ("/home/gurpreet/Desktop/dbi/onemoredb/catalog", "lineitem");

    // grow the CNF expression from the parse tree
    CNF myComparison;
    Record literal;
    myComparison.GrowFromParseTree (final, &lineitem, literal);

    // print out the comparison to the screen
    myComparison.Print ();

    // now open up the text file and start procesing it
    FILE *tableFile = fopen ("/home/gurpreet/Desktop/temp/git/tpch-dbgen/lineitem.tbl", "r");

    Record temp;
    Schema mySchema ("catalog", "lineitem");

    //char *bits = literal.GetBits ();
    //cout << " numbytes in rec " << ((int *) bits)[0] << endl;
    //literal.Print (&supplier);

    // read in all of the records from the text file and see if they match
    // the CNF expression that was typed in
    int counter = 0;
    ComparisonEngine comp;
    while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
        counter++;
        if (counter % 10000 == 0) {
            cerr << counter << "\n";
        }

        if (comp.Compare (&temp, &literal, &myComparison))
            temp.Print (&mySchema);

    }

   /* DBFile *dbFile = new DBFile;
    Schema lineitem("/home/gurpreet/Desktop/dbi/onemoredb/catalog", "lineitem");


    dbFile->Create("/home/gurpreet/Desktop/temp/t.bin",heap, NULL);;
//      dbFile->Open(NULL);


    dbFile->Load(lineitem,"/home/gurpreet/Desktop/temp/git/tpch-dbgen/lineitem.tbl");
    dbFile->MoveFirst();

    Record temp;
    int counter = 0;
    while (dbFile->GetNext(temp) == 1) {
        counter += 1;
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";
    dbFile->Close();*/
}


