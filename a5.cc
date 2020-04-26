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
#include <string>
#include <algorithm>

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
extern char* newtable;

extern char *tableName;
extern char *fileToInsert;
char *catalog = "catalog";

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;
extern fType type;

const std::string WHITESPACE = " \n\r\t\f\v";

std::string ltrim(const std::string& s)
{
	size_t start = s.find_first_not_of(WHITESPACE);
	return (start == std::string::npos) ? "" : s.substr(start);
}

std::string rtrim(const std::string& s)
{
	size_t end = s.find_last_not_of(WHITESPACE);
	return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

using namespace std;

std::string trim(const std::string& s)
{
	return rtrim(ltrim(s));
}

bool exists(const char* relName) {
  ifstream fin (catalog);
  string line;
  while (getline(fin, line))
    if (trim(line) == relName) {
      fin.close(); return true;
    }
  fin.close();  return false;
}

int main() {
    cout << "hello" << endl;

//    outputVar = "STDOUT";

    yyparse ();

    if (queryType == 1) {

         cout << "SELECT" << endl;
         //query plan
         //print
        
    } else if (queryType == 2) {

        cout << "CREATE" << endl;

        cout << tableName << endl;
        if (exists(newtable)) return false;
        std::ofstream ofmeta ((std::string(newtable)+".meta").c_str());
        fType t = (attsToSort ? sorted : heap);
        ofmeta << t << endl;   // 1 Filetype

        // 2 Schema
        int numAtts = 0;
        std::ofstream ofcat ("catalog", std::ios_base::app);
        ofcat << "BEGIN\n" << newtable << '\n' << newtable << ".tbl" << endl;
        const char* myTypes[3] = {"Int", "Double", "String"};
        for (AttrList* att = attsToCreate; att; att = att->next, ++numAtts)
            ofcat << att->name << ' ' << myTypes[att->type] << endl;
        ofcat << "END" << endl;

        Attribute* atts = new Attribute[numAtts];
        Type types[3] = {Int, Double, String};
        numAtts = 0;
        for (AttrList* att = attsToCreate; att; att = att->next, numAtts++) {
            atts[numAtts].name = strdup(att->name);
            atts[numAtts].myType = types[att->type];
        }
        Schema newSchema ("", numAtts, atts);

        // 3 OrderMaker
        OrderMaker sortOrder;
        if (attsToSort) {
            sortOrder.growFromParseTree(attsToSort, &newSchema);
            // ofmeta  << sortOrder;
            ofmeta << 512 << endl;  // TODO: 4 runLength
        }

        struct SortInfo { OrderMaker* myOrder; int runLength; } info = {&sortOrder, 256};
        DBFile newTable;
        newTable.Create((char*)(std::string(newtable)+".bin").c_str(), t, (void*)&info); // create ".bin" files
        newTable.Close();

        delete[] atts;
        ofmeta.close(); ofcat.close();
        return true;
    } else if (queryType == 3) {
        cout << "DROP" << endl;
        cout << tableName << endl;
        string schString = "", line = "", relName = tableName;
        ifstream fin (catalog);
        ofstream fout (".cata.tmp");
        bool found = false, exists = false;
        while (getline(fin, line)) {
            if (trim(line).empty()) continue;
            if (line == tableName) exists = found = true;
            schString += trim(line) + '\n';
            if (line == "END") {
            if (!found) fout << schString << endl;
            found = false;
            schString.clear();
            }
        }
        rename (".cata.tmp", catalog);
        fin.close(); fout.close();
        // delete bin, meta
        if (exists) {
            remove ((relName+".bin").c_str());
            remove ((relName+".meta").c_str());
            return true;
        }
        return false;
    } else if (queryType == 4) {
        cout << "INSERT" << endl;
        cout << fileToInsert << endl;
        cout << tableName << endl;
        DBFile table;
        char fpath[100];
        sprintf (fpath, "bin/%s.bin", tableName);
        Schema sch (catalog, tableName);
        if (table.Open(fpath)) {
            table.Load(sch, fileToInsert);
            table.Close();
            delete[] fpath; 
            return true;
        } 
        delete[] fpath; 
        return false;
    } else if (queryType == 5) {

        cout << "SET" << endl;

        cout << outputVar << endl;

    } else if (queryType == 6) {

        cout << "EXIT" << endl;
    }

    return 0;

}