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
#include <float.h>

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
typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;

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
map<string, Schema *> loadSchema;
MyFucntion myfunc;
char *input = "meta/Statistics.txt";
char *output = "Statistics.txt";
Statistics s;

int main()
{
    cout << "hello" << endl;
//    string option;
//    cout << endl;
//    cout << "1. SELECT QUERY" << endl;
//    cout << "2. Create a new database (will delete current database)" << endl;
//    cout << "3. Drop a table" << endl;
//    cout << "4. Insert file to a table" << endl;
//    cout << "5. Exit" << endl
//         << endl;
//    cout << "Your choice: ";

    while (true)
    {
        //todo clear the variables
        cout <<endl<< "***************************************************************************************"<<endl;
        cout << "Enter the query: "<<endl;
        yyparse();
        // cout<<tables;
        if (queryType != 0)
        {
            if (queryType == 1)
            {
                // cout<<tables;
                loadSchema = myfunc.FireUpExistingDatabase();
                s.Read(input);

                vector<string> seenTable;
                map<string, string> aliasName;
                map<string, Schema *> aliasSchemas;
                //load DB //FireUpExistingDatabase
                
                TableList *cur = tables;
                while (cur)
                {
                    if (loadSchema.count(cur->tableName) == 0)
                    {
                        cerr << "Error: Table hasn't been created!" << endl;
                        return -1;
                    }
                    s.CopyRel(cur->tableName, cur->aliasAs);
                    myfunc.copySchema(aliasSchemas, cur->tableName, cur->aliasAs);
                    seenTable.push_back(cur->aliasAs);
                    aliasName[cur->aliasAs] = cur->tableName;
                    cur = cur->next;
                }

                s.Write(output);
                // cout<<"1"<<endl;
                vector<vector<string>> joinOrder = myfunc.shuffleOrder(seenTable);
                int indexofBestChoice = 0;
                double minRes = DBL_MAX;
                size_t numofRels = joinOrder[0].size();
                if (numofRels == 1)
                {
                    char **relNames = new char *[1];
                    relNames[0] = new char[joinOrder[0][0].size() + 1];
                    strcpy(relNames[0], joinOrder[0][0].c_str());
                    minRes = s.Estimate(boolean, relNames, 1);
                }
                else
                {
                    for (int i = 0; i < joinOrder.size(); ++i)
                    {
                        s.Read(output);

                        double result = 0;
                        char **relNames = new char *[numofRels];
                        for (int j = 0; j < numofRels; ++j)
                        {
                            relNames[j] = new char[joinOrder[i][j].size() + 1];
                            strcpy(relNames[j], joinOrder[i][j].c_str());
                        }

                        for (int j = 2; j <= numofRels; ++j)
                        {
                            result += s.Estimate(boolean, relNames, j);
                            s.Apply(boolean, relNames, j);
                        }

                        if (result < minRes)
                        {
                            minRes = result;
                            indexofBestChoice = i;
                        }
                    }
                }
                cout<<"MinResult: "<<minRes<<endl;
                vector<string> chosenJoinOrder = joinOrder[indexofBestChoice];

                Operator *left = new SelectFileOperator(boolean, aliasSchemas[chosenJoinOrder[0]], aliasName[chosenJoinOrder[0]]);
                Operator *root = left;
                for (int i = 1; i < numofRels; ++i)
                {
                    Operator *right = new SelectFileOperator(boolean, aliasSchemas[chosenJoinOrder[i]], aliasName[chosenJoinOrder[i]]);
                    root = new JoinOperator(left, right, boolean);
                    left = root;
                }
                if (distinctAtts == 1 || distinctFunc == 1)
                {
                    root = new DuplicateRemovalOperator(left);
                    left = root;
                }
                if (groupingAtts)
                {
                    root = new GroupByOperator(left, groupingAtts, finalFunction);
                    left = root;
                    NameList *sum = new NameList();
                    sum->name = "SUM";
                    sum->next = attsToSelect;
                    root = new ProjectOperator(left, sum);
                }
                else if (finalFunction)
                {
                    root = new SumOperator(left, finalFunction);
                    left = root;
                }
                else if (attsToSelect)
                {
                    root = new ProjectOperator(left, attsToSelect);
                }
                outputVar = "STDOUT";
                cout<<"OutputVar: "<<outputVar<<endl;
                myfunc.WriteOutFunc(root,0,outputVar);
            }
            else if (queryType == 2){
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
                    if (file.Create(fileName, heap, NULL))
                    {
                        cout << "Created bin file";
                    }
                }
                else
                {
                    //todo change this part
//                    Schema sch(catalog, tableName);
//                    OrderMaker order;
//                    order.growFromParseTree(attsToSort, &sch);
//                    SortInfo info;
//                    info.myOrder = &order;
//                    info.runLength = 100;
//                    file.Create(fileName, sorted, &info);

                    Schema* newSchema= new Schema(catalog, tableName);

                    loadSchema[tableName] =new Schema(catalog, tableName);

                    file.Create(fileName, tree, (void*)newSchema);
                }
                cout <<"create completed.." <<endl;
            }else if (queryType == 3)
            {
                char fileName[100];
                // char metaName[100];
                char *tempFile = "tempfile.txt";

                sprintf(fileName, "bin/%s.bin", tableName);
                // sprintf (metaName, "%s.md", fileName);

                remove(fileName);
                // remove (metaName);

                ifstream ifs(catalog);
                ofstream ofs(tempFile);

                while (!ifs.eof())
                {

                    char line[100];

                    ifs.getline(line, 100);

                    if (strcmp(line, "BEGIN") == 0)
                    {

                        ifs.getline(line, 100);

                        if (strcmp(line, tableName))
                        {

                            ofs << endl;
                            ofs << "BEGIN" << endl;
                            ofs << line << endl;

                            ifs.getline(line, 100);

                            while (strcmp(line, "END"))
                            {

                                ofs << line << endl;
                                ifs.getline(line, 100);
                            }

                            ofs << "END" << endl;
                            if (trim(line).empty())
                                continue;
                        }
                    }
                }

                ifs.close();
                ofs.close();

                remove(catalog);
                rename(tempFile, catalog);
                remove(tempFile);
                cout << "done drop";
            }
            else if (queryType == 4)
            {
                char fileName[100];
                char tpchName[100];
                sprintf(fileName, "bin/%s.bin", tableName);
                sprintf(tpchName, "%s", fileToInsert);
                // cout<<tpchName;
                DBFile file;
                Schema sch(catalog, tableName);
                sch.Print();
                if (file.Open(fileName))
                {
                    // cout<<"Inside here";
                    file.Load(sch, tpchName);
                    file.Close();
                }
                cout << "done insert";
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
            queryType = 0;
            //todo commented folllowing lines
//            std::cin.clear();
//            std::cin.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
        }

    }
    return 0;
}

//CREATE TABLE nation (att1 INTEGER, att2 DOUBLE, att3 STRING) AS HEAP
//INSERT 'trial' INTO nation


//CREATE TABLE table2 (n_nationkey INTEGER, n_name STRING, n_regionkey INTEGER, n_comment STRING) AS SORTED ON n_nationkey
//INSERT '/home/deepak/Desktop/dbi/onemoredb/tcph/table2.txt' INTO table2
// SELECT n.n_nationkey FROM table2 AS n WHERE (n.n_nationkey = 1)