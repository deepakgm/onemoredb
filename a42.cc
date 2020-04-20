#include <map>
#include <vector>
#include <string>
#include <cstring>
#include <climits>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "DBFile.h"
#include "Function.h"
#include "ParseTree.h"
#include "Statistics.h"
#include "Comparison.h"
#include <iostream>
#include <float.h>

#include "ParseTree.h"
#include "Statistics.h"
#include "Schema.h"
#include "OpTreeNode.h"
#include "string.h"

extern "C" {
int yyparse(void);   // defined in y.tab.c
}

extern struct FuncOperator *finalFunction; // the aggregate function (NULL if no agg)
extern struct TableList *tables; // the list of tables and aliases in the query
extern struct AndList *boolean; // the predicate in the WHERE clause
extern struct NameList *groupingAtts; // grouping atts (NULL if no grouping)
extern struct NameList *attsToSelect; // the set of attributes in the SELECT (NULL if no such atts)
extern int distinctAtts; // 1 if there is a DISTINCT in a non-aggregate query
extern int distinctFunc;  // 1 if there is a DISTINCT in an aggregate query

extern int sqlType; // 0 for create, 1 for insert, 2 for drop, 3 for set, 4 for select, 5 for update
extern int dbfileType; // 0 for heap file, 1 for sorted file, 2 for B plus tree file
extern string tablename;
extern string loadFileName; // save the insert file name
extern string outputFile; // save filename of output, it could be none, string or stdout
extern struct AttrList *attrList;
extern struct NameList *sortList;

using namespace std;

const string statistics_path = "/home/deepak/Desktop/dbi/onemoredb/Statistics.txt";
const string catalog_path = "/home/deepak/Desktop/dbi/onemoredb/catalog";

Statistics s;

map<string, int> tablesInDB;
map<string, Schema *> schemas;


void  shuffleOrderHelper(vector<string> &seenTable, int index, vector<vector<string>> &res, vector<string> &tmpres) {
    if(index == seenTable.size())
    {
        res.push_back(tmpres);
        return;
    }
    for(int i = index; i < seenTable.size(); i++)
    {
        swap(seenTable[index], seenTable[i]);
        tmpres.push_back(seenTable[index]);
        shuffleOrderHelper(seenTable, index + 1, res, tmpres);
        tmpres.pop_back();
        swap(seenTable[index], seenTable[i]);
    }
}

vector<vector<string>> shuffleOrder(vector<string> &seenTable) {
    vector<vector<string>> res;
    vector<string> tmpres;
    shuffleOrderHelper(seenTable, 0, res, tmpres);
    return res;
}

void copySchema(map<string, Schema *> &aliasSchemas, char *oldName, char *newName) {

    Attribute *oldAtts = schemas[oldName]->GetAtts();
    int numAtts = schemas[oldName]->GetNumAtts();
    Attribute *newAtts = new Attribute[numAtts];
    size_t relLength = strlen(newName);

    for (int i = 0; i < numAtts; ++i) {
        size_t attLength = strlen(oldAtts[i].name);
        newAtts[i].name = new char[attLength + relLength + 2];
        strcpy(newAtts[i].name, newName);
        strcat(newAtts[i].name, ".");
        strcat(newAtts[i].name, oldAtts[i].name);
        newAtts[i].myType = oldAtts[i].myType;
    }
    aliasSchemas[newName] = new Schema(newName, numAtts, newAtts);
}



void traverse(OpTreeNode *currNode, int mode) {
    if (!currNode)
        return;
    switch (currNode->getType()) {
        case SELECTFILE:
             ((SelectFileNode*)currNode)->print();
            break;
        case SELECTPIPE:
            traverse(((SelectPipeNode*)currNode)->left, mode);
             ((SelectPipeNode*)currNode)->print();
            break;
        case PROJECT:
            traverse(((ProjectNode*)currNode)->left, mode);
             ((ProjectNode*)currNode)->print();
            break;
        case GROUPBY:
            traverse(((GroupByNode*)currNode)->left, mode);
             ((GroupByNode*)currNode)->print();
            break;
        case SUM:
            traverse(((SumNode*)currNode)->left, mode);
             ((SumNode*)currNode)->print();
            break;
        case DUPLICATEREMOVAL:
            traverse(((DuplicateRemovalNode*)currNode)->left, mode);
             ((DuplicateRemovalNode*)currNode)->print();
            break;
        case JOIN:
            traverse(((JoinNode*)currNode)->left, mode);
            traverse(((JoinNode*)currNode)->right, mode);
             ((JoinNode*)currNode)->print();
            break;
        default:
            cerr << "ERROR: Unspecified node!" << endl;
            exit(-1);
    }
     cout << endl << "*******************************************************" << endl;
}


void PrintTablesAliases (TableList * tableList)	{
    while (tableList) {
        cout << "Table " << tableList->tableName;
        cout <<	" is aliased to " << tableList->aliasAs << endl;
        tableList = tableList->next;
    }
}

char *supplier = "supplier";
char *partsupp = "partsupp";
char *part = "part";
char *nation = "nation";
char *customer = "customer";
char *orders = "orders";
char *region = "region";
char *lineitem = "lineitem";


void initSchemaMap () {
    schemas[string(region)] = new Schema ("catalog", region);
    schemas[string(part)] = new Schema ("catalog", part);
    schemas[string(partsupp)] = new Schema ("catalog", partsupp);
    schemas[string(nation)] = new Schema ("catalog", nation);
    schemas[string(customer)] = new Schema ("catalog", customer);
    schemas[string(supplier)] = new Schema ("catalog", supplier);
    schemas[string(lineitem)] = new Schema ("catalog", lineitem);
    schemas[string(orders)] = new Schema ("catalog", orders);

}
int main() {

    initSchemaMap();

    cout << "Input :" << endl;;
    yyparse();


    s.initStatistics();

    vector<string> seenTable;
    map<string, string> aliasName;
    map<string, Schema *> aliasSchemas;

    TableList *cur = tables;
//    cout << endl << "Print TableList :" << endl;
//    PrintTablesAliases (tables);

    while (cur) {
        if (schemas.count(cur->tableName) == 0) {
            cerr << "Error: Table hasn't been created!" << endl;
            return 0;
        }
        s.CopyRel(cur->tableName, cur->aliasAs);
        copySchema(aliasSchemas, cur->tableName, cur->aliasAs);
        seenTable.push_back(cur->aliasAs);
        aliasName[cur->aliasAs] = cur->tableName;
        cur = cur->next;
    }

    s.Write((char *)statistics_path.c_str());


    vector<vector<string>> joinOrder = shuffleOrder(seenTable);

    int indexofBestChoice = 0;
    double minRes = DBL_MAX;
    size_t numofRels = joinOrder[0].size();
    if (numofRels == 1) {
        char **relNames = new char *[1];
        relNames[0] = new char[joinOrder[0][0].size() + 1];
        strcpy(relNames[0], joinOrder[0][0].c_str());
        minRes = s.Estimate(boolean, relNames, 1);
    } else {
        for (int i = 0; i < joinOrder.size(); ++i) {
            s.Read((char *)statistics_path.c_str());

            double result = 0;
            char **relNames = new char *[numofRels];
            for (int j = 0; j < numofRels; ++j) {
                relNames[j] = new char[joinOrder[i][j].size() + 1];
                strcpy(relNames[j], joinOrder[i][j].c_str());
            }

            for (int j = 2; j <= numofRels; ++j) {
                result += s.Estimate(boolean, relNames, j);
                s.Apply(boolean, relNames, j);
            }

            if (result < minRes) {
                minRes = result;
                indexofBestChoice = i;
            }
        }
    }

    vector<string> chosenJoinOrder = joinOrder[indexofBestChoice];


    /*
     * Generate opTree
     */
    OpTreeNode *left = new SelectFileNode(boolean, aliasSchemas[chosenJoinOrder[0]], aliasName[chosenJoinOrder[0]]);

    OpTreeNode *root = left;
    for (int i = 1; i < numofRels; ++i) {
        OpTreeNode *right = new SelectFileNode(boolean, aliasSchemas[chosenJoinOrder[i]],
                                               aliasName[chosenJoinOrder[i]]);
        root = new JoinNode(left, right, boolean);
        left = root;
    }
    if (distinctAtts == 1 || distinctFunc == 1) {
        root = new DuplicateRemovalNode(left);
        left = root;
    }
    if (groupingAtts) {
        root = new GroupByNode(left, groupingAtts, finalFunction);
        left = root;
        NameList *sum = new NameList();
        sum->name = "SUM";
        sum->next = attsToSelect;
        root = new ProjectNode(left, sum);
    } else if (finalFunction) {
        root = new SumNode(left, finalFunction);
        left = root;
    } else if (attsToSelect) {
        root = new ProjectNode(left, attsToSelect);
    }

    traverse(root, 0);
   
    return 0;

}