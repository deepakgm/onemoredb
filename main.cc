
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"


using namespace std;

//extern "C" {
//int yyparse(void);   // defined in y.tab.c
//}

extern struct AndList *final;

int main () {
    Schema lineitem ("/home/gurpreet/Desktop/dbi/onemoredb/catalog", "lineitem");
    Record literal;

//     try to parse the CNF
//    cout << "Enter in your CNF: ";
//    if (yyparse() != 0) {
//        cout << "Can't parse your CNF.\n";
//        exit (1);
//    }
//    CNF myComparison;
//    myComparison.GrowFromParseTree (final, &lineitem, literal);
//    myComparison.Print ();

    DBFile *dbFile=new DBFile;

    dbFile->Open("/home/gurpreet/Desktop/temp/t");

//    cout<<"here"<<endl;
//
//
//    dbFile->Load(lineitem,"/home/gurpreet/Desktop/temp/git/tpch-dbgen/lineitem.tbl");
//    dbFile->Close();
//    dbFile->Open("/home/gurpreet/Desktop/temp/t");

//
//

    Record temp;
//////
    dbFile->MoveFirst();
////
//    dbFile->GetNext(temp);
////
//    temp.Print(&lineitem);

    int counter = 0;
    while (dbFile->GetNext (temp) == 1) {
        counter += 1;
//        temp.Print (&lineitem);
        if (counter % 10000 == 0) {
            cout << counter << "\n";
        }
    }
    cout << " scanned " << counter << " recs \n";

    dbFile->Close ();
//
//    literal.Print(&lineitem);











//
//    // now open up the text file and start procesing it
//    FILE *tableFile = fopen ("/home/gurpreet/Desktop/temp/git/tpch-dbgen/lineitem.tbl", "r");
//    Record temp;
//    Schema mySchema ("catalog", "lineitem");
//
//    FILE *tableFile2 = fopen ("/home/gurpreet/Desktop/temp/git/tpch-dbgen/lineitem2.tbl", "r");
//    Record temp2;
//    //char *bits = literal.GetBits ();
//    //cout << " numbytes in rec " << ((int *) bits)[0] << endl;
//    //literal.Print (&supplier);
//
//    // read in all of the records from the text file and see if they match
//    // the CNF expression that was typed in
//
//
//
//    int counter = 0;
//    ComparisonEngine comp;
//    while (temp.SuckNextRecord (&mySchema, tableFile) == 1) {
//        counter++;
//        if (counter % 10000 == 0) {
//            cerr << counter << "\n";
//        }
//        temp2.Copy(&temp);
////		temp2.Print(&mySchema);
//
//        if (comp.Compare (&temp, &literal, &myComparison))
//            temp2.Print (&mySchema);
//    }

}


