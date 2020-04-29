#ifndef EXTRAFUNC_H
#define EXTRAFUNC_H

#include <iostream>
#include <string>
#include <vector>
#include <stdio.h>
#include "ParseTree.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Schema.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"
#include "Statistics.h"
#include "Operator.h"

class MyFucntion{
public:

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

// using namespace std;

std::string trim(const std::string &s)
{
    return rtrim(ltrim(s));
}

char *catalog = "catalog";
typedef map<string, Schema> SchemaMap;
typedef map<string, string> AliaseMap;
map<string, Schema *> loadSchema;
int outputSet=0; //0->stdout,1->file,2->none;
WriteOut wo;
const string DBInfo = "tableInfo.txt";
const string db_dir = "temp/";

void PrintNameList (NameList *nameList);
void CopyAttrList(AttrList *attrList, vector<Attribute> &atts);
// void initSchemaMap (SchemaMap &map);
// void CopyTablesNamesAndAliases (TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map);
map<string, Schema *> FireUpExistingDatabase();
void copySchema(map<string, Schema *> &aliasSchemas, char *oldName, char *newName);
void shuffleOrderHelper(vector<string> &seenTable, int index, vector<vector<string>> &res, vector<string> &tmpres);
vector<vector<string>> shuffleOrder(vector<string> &seenTable);
void WriteOutFunc(Operator *op, int outputSet, char* outputFile);
void printRecursively(Operator *root ,int outputSet);
int UpdateTable(char* c);
void UpdateTableInfo(char* table);

};
#endif