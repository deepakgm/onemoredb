#include <map>
#include <vector>
#include <string>
#include <iostream>
#include <algorithm>
#include "Schema.h"
#include "ParseTree.h"
#include "Statistics.h"
#include <float.h>
#include "Operator.h"
#include "string.h"
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

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


using namespace std;

char meta_statistics_path[PATH_MAX];
char statistics_path[PATH_MAX];
char catalog_path[PATH_MAX];


Statistics s;
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



void traverse(Operator *currNode, int mode) {
    if (!currNode)
        return;
    switch (currNode->getType()) {
        case SELECT_FILE:
             ((SelectFileOperator*)currNode)->print();
            break;
        case SELECT_PIPE:
            traverse(((SelectPipeOperator*)currNode)->left, mode);
             ((SelectPipeOperator*)currNode)->print();
            break;
        case PROJECT:
            traverse(((ProjectOperator*)currNode)->left, mode);
             ((ProjectOperator*)currNode)->print();
            break;
        case GROUPBY:
            traverse(((GroupByOperator*)currNode)->left, mode);
             ((GroupByOperator*)currNode)->print();
            break;
        case SUM:
            traverse(((SumOperator*)currNode)->left, mode);
             ((SumOperator*)currNode)->print();
            break;
        case DUPLICATE_REMOVAL:
            traverse(((DuplicateRemovalOperator*)currNode)->left, mode);
             ((DuplicateRemovalOperator*)currNode)->print();
            break;
        case JOIN:
            traverse(((JoinOperator*)currNode)->left, mode);
            traverse(((JoinOperator*)currNode)->right, mode);
             ((JoinOperator*)currNode)->print();
            break;
        default:
            cerr << "ERROR: Unspecified node!" << endl;
            exit(-1);
    }
     cout << endl << "*******************************************************" << endl;
}

void createSchemaMap () {
    schemas["region"] = new Schema ("catalog", "region");
    schemas["part"] = new Schema ("catalog", "part");
    schemas["partsupp"] = new Schema ("catalog", "partsupp");
    schemas["nation"] = new Schema ("catalog", "nation");
    schemas["customer"] = new Schema ("catalog", "customer");
    schemas["supplier"] = new Schema ("catalog", "supplier");
    schemas["lineitem"] = new Schema ("catalog", "lineitem");
    schemas["orders"] = new Schema ("catalog", "orders");

}
int main() {

    if (getcwd(statistics_path, sizeof(statistics_path)) != NULL) {
        strcpy(catalog_path,statistics_path);
        strcpy(meta_statistics_path,statistics_path);
        strcat(catalog_path,"/catalog");
        strcat(statistics_path,"/Statistics.txt");
        strcat(meta_statistics_path,"/meta/Statistics.txt");
    } else {
        cerr << "error while getting current dir" << endl;
        return 1;
    }

    createSchemaMap();

    cout << "Input :" << endl;;
    yyparse();


//  Load initial statistics from meta folder
    s.Read(meta_statistics_path);
    s.Write(statistics_path);

    vector<string> tableList;
    map<string, string> aliasName;
    map<string, Schema *> aliasSchemas;

    TableList *cur = tables;

    while (cur) {
        if (schemas.count(cur->tableName) == 0) {
            cerr << "Error: Table hasn't been created!" << endl;
            return 0;
        }
        s.CopyRel(cur->tableName, cur->aliasAs);
        copySchema(aliasSchemas, cur->tableName, cur->aliasAs);
        tableList.push_back(cur->aliasAs);
        aliasName[cur->aliasAs] = cur->tableName;
        cur = cur->next;
    }

    s.Write(statistics_path);


    vector<vector<string>> joinOrder = shuffleOrder(tableList);

    int indexofBestChoice = 0;
    double minRes = DBL_MAX;
    size_t numofRels = joinOrder[0].size();
    if (numofRels == 1) {
        char **relNames = new char *[1];
        relNames[0] = new char[joinOrder[0][0].size() + 1];
        strcpy(relNames[0], joinOrder[0][0].c_str());
        minRes = s.Estimate(boolean, relNames, 1);
    } else {
//        cout << "i'm here!";
        for (int i = 0; i < joinOrder.size(); ++i) {
            s.Read(statistics_path);

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


//    cout << "join order: " << chosenJoinOrder[0] <<"   "<<aliasName[chosenJoinOrder[0]]<<endl;

    Operator *left = new SelectFileOperator(boolean, aliasSchemas[chosenJoinOrder[0]], aliasName[chosenJoinOrder[0]]);

    Operator *root = left;
    for (int i = 1; i < numofRels; ++i) {
        Operator *right = new SelectFileOperator(boolean, aliasSchemas[chosenJoinOrder[i]],
                                                 aliasName[chosenJoinOrder[i]]);
        root = new JoinOperator(left, right, boolean);
        left = root;
    }
    if (distinctAtts == 1 || distinctFunc == 1) {
        root = new DuplicateRemovalOperator(left);
        left = root;
    }
    if (groupingAtts) {
        root = new GroupByOperator(left, groupingAtts, finalFunction);
        left = root;
        NameList *sum = new NameList();
        sum->name = "SUM";
        sum->next = attsToSelect;
        root = new ProjectOperator(left, sum);
    } else if (finalFunction) {
        root = new SumOperator(left, finalFunction);
        left = root;
    } else if (attsToSelect) {
        root = new ProjectOperator(left, attsToSelect);
    }

    traverse(root, 0);
   
    return 0;

}