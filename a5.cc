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
#include "Operator.h"
#include "extraFunction.h"
#include <unordered_map>

extern "C"
{
    int yyparse(void); // defined in y.tab.c
}

using namespace std;
Statistics statistics;

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables;           // the list of tables and aliases in the query
extern struct AndList *boolean;            // the predicate in the WHERE clause
extern struct NameList *groupingAtts;      // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect;      // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts;                   // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;                   // 1 if there is a DISTINCT in an aggregate query

extern int queryType; // 1 for SELECT, 2 for CREATE, 3 for DROP,
// 4 for INSERT, 5 for SET, 6 for EXIT
// extern int outputType; // 0 for NONE, 1 for STDOUT, 2 for file output

extern char *outputVar;
extern char *newtable;
RelationalOp *relOp;
string inp;

extern char *tableName;
extern char *fileToInsert;
char *catalog = "catalog";
char meta_statistics_path[PATH_MAX];

extern struct AttrList *attsToCreate;
extern struct NameList *attsToSort;
extern fType type;

unordered_map<int, Pipe *> pipeMap;

const std::string WHITESPACE = " \n\r\t\f\v";

std::string ltrim(const std::string &s)
{
    size_t start = s.find_first_not_of(WHITESPACE);
    return (start == std::string::npos) ? "" : s.substr(start);
}

std::string rtrim(const std::string &s)
{
    size_t end = s.find_last_not_of(WHITESPACE);
    return (end == std::string::npos) ? "" : s.substr(0, end + 1);
}

using namespace std;

std::string trim(const std::string &s)
{
    return rtrim(ltrim(s));
}

bool exists(const char *relName)
{
    ifstream fin(catalog);
    string line;
    while (getline(fin, line))
        if (trim(line) == relName)
        {
            fin.close();
            return true;
        }
    fin.close();
    return false;
}
unordered_map<string, Schema *> schemas;
MyFucntion myfunc;
int main()
{
    cout << "hello" << endl;
    string option;
    cout << endl;
        cout << "1. SELECT QUERY" << endl;
        cout << "2. Create a new database (will delete current database)" << endl;
        cout << "3. Drop a table" << endl;
        cout << "4. Insert file to a table" << endl;
        cout << "5. Exit" << endl<< endl;
        cout << "Your choice: ";

    while (std::cin >> queryType)
    {
        yyparse();
        if(queryType !=0){
        if (queryType == 1)
        {
         
        }
        else if (queryType == 2)
        {
            cout << "CREATE" << endl;
            cout << tableName << endl;
            if (attsToSort)
            {
                myfunc.PrintNameList(attsToSort);
            }
            // mPrintNameList(attsToSort);
            char fileName[100];
            char tpchName[100];

            sprintf(fileName, "bin/%s.bin", tableName);
            sprintf(tpchName, "test/%s.tbl", tableName);
            // cout<<tpchName;
            DBFile file;
            vector<Attribute> attsCreate;

            myfunc.CopyAttrList(attsToCreate, attsCreate);

            ofstream ofs(catalog, ifstream ::app);

            ofs << endl;
            ofs << "BEGIN" << endl;
            ofs << tableName << endl;
            ofs << tpchName << endl;

            Statistics s;
            char* input = "meta/Statistics.txt";
            char* output = "Statistics.txt";
            s.Read(input);
            //			s.Write (stats);
            s.AddRel(tableName, 0);

            for (auto iter = attsCreate.begin(); iter != attsCreate.end(); iter++)
            {
                s.AddAtt(tableName, iter->name, 0);
                ofs << iter->name << " ";
                cout << iter->myType << endl;
                switch (iter->myType)
                {
                case Int:
                {
                    ofs << "Int" << endl;
                }
                break;
                case Double:
                {

                    ofs << "Double" << endl;
                }
                break;
                case String:
                {

                    ofs << "String" << endl;
                }
                // should never come here!
                default:
                {
                }
                }
            }

            ofs << "END" << endl;
            s.Write(output);
            if (!attsToSort)
            {
                // cout<<"hi";
                if(file.Create(fileName, heap, NULL)){
                    cout<<"Created bin file";
                }
            }
            else
            {
                Schema sch(catalog, tableName);
                OrderMaker order;
                order.growFromParseTree(attsToSort, &sch);
                SortInfo info;
                info.myOrder = &order;
                info.runLength = 100;
                file.Create(fileName, sorted, &info);
            }
        }
        else if (queryType == 3)
        {
            char fileName[100];
			// char metaName[100];
			char *tempFile = "tempfile.txt";

			sprintf (fileName, "bin/%s.bin", tableName);
			// sprintf (metaName, "%s.md", fileName);

			remove (fileName);
			// remove (metaName);

			ifstream ifs (catalog);
			ofstream ofs (tempFile);

			while (!ifs.eof ()) {

				char line[100];

				ifs.getline (line, 100);

				if (strcmp (line, "BEGIN") == 0) {

					ifs.getline (line, 100);

					if (strcmp (line, tableName)) {

						ofs << endl;
						ofs << "BEGIN" << endl;
						ofs << line << endl;

						ifs.getline (line, 100);

						while (strcmp (line, "END")) {

							ofs << line << endl;
							ifs.getline (line, 100);

						}

						ofs << "END" << endl;
                        if (trim(line).empty()) continue;
					}

				}

			}

			ifs.close ();
			ofs.close ();

			remove (catalog);
			rename (tempFile, catalog);
			remove (tempFile);
            cout<<"done drop";
        }
        else if (queryType == 4)
        {
            char fileName[100];
			char tpchName[100];
			sprintf (fileName, "bin/%s.bin", tableName);
			sprintf (tpchName, "tcph/%s.txt", fileToInsert);
            // cout<<tpchName;
			DBFile file;
			Schema sch (catalog, tableName);
			sch.Print ();
			if (file.Open (fileName)) {
                // cout<<"Inside here";
				file.Load (sch, tpchName);
				file.Close ();
			}
            cout<<"done insert";
        }
        else if (queryType == 5)
        {

            cout << "SET" << endl;

            cout << outputVar << endl;
        }
        else if (queryType == 6)
        {

            cout << "EXIT" << endl;
        }
        queryType=0;
        std::cin.clear();
        std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
    }
    }
    return 0;
}

//CREATE TABLE nation (att1 INTEGER, att2 DOUBLE, att3 STRING) AS HEAP
//INSERT 'trial' INTO nation