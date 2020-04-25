#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"
#include <chrono>
#include "Statistics.h"

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

using namespace std;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int queryType;  // 1 for SELECT, 2 for CREATE, 3 for DROP,
// 4 for INSERT, 5 for SET, 6 for EXIT
// extern int outputType; // 0 for NONE, 1 for STDOUT, 2 for file output

extern char *outputVar;

extern char *tableName;
extern char *fileToInsert;

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;

using namespace std;


int main() {
    cout << "hello" << endl;

//    outputVar = "STDOUT";

    yyparse ();

    if (queryType == 1) {

         cout << "SELECT" << endl;


    } else if (queryType == 2) {

        cout << "CREATE" << endl;

        cout << tableName << endl;

        /*   if (attsToSort) {

               PrintNameList (attsToSort);

           }

           char fileName[100];
           char tpchName[100];

           sprintf (fileName, "bin/%s.bin", tableName);
           sprintf (tpchName, "%s.tbl", tableName);

           DBFile file;

           vector<Attribute> attsCreate;

           CopyAttrList (attsToCreate, attsCreate);

           ofstream ofs(catalog, ifstream :: app);

           ofs << endl;
           ofs << "BEGIN" << endl;
           ofs << tableName << endl;
           ofs << tpchName <<endl;

           Statistics s;
           s.Read (stats);
   //			s.Write (stats);
           s.AddRel (tableName, 0);

           for (auto iter = attsCreate.begin (); iter != attsCreate.end (); iter++) {

               s.AddAtt (tableName, iter->name, 0);

               ofs << iter->name << " ";

               cout << iter->myType << endl;
               switch (iter->myType) {

                   case Int : {
                       ofs << "Int" << endl;
                   } break;

                   case Double : {

                       ofs << "Double" << endl;

                   } break;

                   case String : {

                       ofs << "String" << endl;

                   }
                       // should never come here!
                   default : {}

               }

           }

           ofs << "END" << endl;
           s.Write (stats);

           if (!attsToSort) {

               file.Create (fileName, heap, NULL);

           } else {

               Schema sch (catalog, tableName);

               OrderMaker order;

               order.growFromParseTree (attsToSort, &sch);

               SortInfo info;

               info.myOrder = &order;
               info.runLength = BUFFSIZE;

               file.Create (fileName, sorted, &info);

           }*/

    } else if (queryType == 3) {

        cout << "DROP" << endl;

        cout << tableName << endl;


    } else if (queryType == 4) {

        cout << "INSERT" << endl;

        cout << fileToInsert << endl;
        cout << tableName << endl;


    } else if (queryType == 5) {

        cout << "SET" << endl;

        cout << outputVar << endl;

    } else if (queryType == 6) {

        cout << "EXIT" << endl;
    }

    return 0;

}