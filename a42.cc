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
int yyparse(void);
}

extern struct FuncOperator *finalFunction;
extern struct TableList *tables;
extern struct AndList *boolean;
extern struct NameList *groupingAtts;
extern struct NameList *attsToSelect;
extern int distinctAtts;
extern int distinctFunc;


using namespace std;

char meta_statistics_path[PATH_MAX];
char statistics_path[PATH_MAX];
char catalog_path[PATH_MAX];


Statistics statistics;
map<string, Schema *> schemas;


void helper(vector<string> &tableList, int index, vector<vector<string>> &orderList, vector<string> &tempOrder) {
    if (index == tableList.size()) {
        orderList.push_back(tempOrder);
        return;
    }
    for (int i = index; i < tableList.size(); i++) {
        swap(tableList[index], tableList[i]);
        tempOrder.push_back(tableList[index]);
        helper(tableList, index + 1, orderList, tempOrder);
        tempOrder.pop_back();
        swap(tableList[index], tableList[i]);
    }
}

vector<vector<string>> createOrderList(vector<string> &tableList) {
    vector<vector<string>> orderList;
    vector<string> tempOrder;
    helper(tableList, 0, orderList, tempOrder);
    return orderList;
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


void printRecursively(Operator *currNode) {
    if (!currNode)
        return;
    switch (currNode->getType()) {
        case SELECT_FILE:
            ((SelectFileOperator *) currNode)->print();
            break;
        case SELECT_PIPE:
            printRecursively(((SelectPipeOperator *) currNode)->left);
            ((SelectPipeOperator *) currNode)->print();
            break;
        case PROJECT:
            printRecursively(((ProjectOperator *) currNode)->left);
            ((ProjectOperator *) currNode)->print();
            break;
        case GROUPBY:
            printRecursively(((GroupByOperator *) currNode)->left);
            ((GroupByOperator *) currNode)->print();
            break;
        case SUM:
            printRecursively(((SumOperator *) currNode)->left);
            ((SumOperator *) currNode)->print();
            break;
        case DUPLICATE_REMOVAL:
            printRecursively(((DuplicateRemovalOperator *) currNode)->left);
            ((DuplicateRemovalOperator *) currNode)->print();
            break;
        case JOIN:
            printRecursively(((JoinOperator *) currNode)->left);
            printRecursively(((JoinOperator *) currNode)->right);
            ((JoinOperator *) currNode)->print();
            break;
        default:
            cerr << "ERROR: Unspecified node!" << endl;
            exit(-1);
    }
    cout << endl << "*******************************************************" << endl;
}

void createSchemaMap() {
    schemas["region"] = new Schema("catalog", "region");
    schemas["part"] = new Schema("catalog", "part");
    schemas["partsupp"] = new Schema("catalog", "partsupp");
    schemas["nation"] = new Schema("catalog", "nation");
    schemas["customer"] = new Schema("catalog", "customer");
    schemas["supplier"] = new Schema("catalog", "supplier");
    schemas["lineitem"] = new Schema("catalog", "lineitem");
    schemas["orders"] = new Schema("catalog", "orders");

}

void PrintParseTree(struct AndList *andPointer) {

    cout << "(";

    while (andPointer) {

        struct OrList *orPointer = andPointer->left;

        while (orPointer) {

            struct ComparisonOp *comPointer = orPointer->left;

            if (comPointer != NULL) {

                struct Operand *pOperand = comPointer->left;

                if (pOperand != NULL) {

                    cout << pOperand->value << "";

                }

                switch (comPointer->code) {

                    case LESS_THAN:
                        cout << " < ";
                        break;
                    case GREATER_THAN:
                        cout << " > ";
                        break;
                    case EQUALS:
                        cout << " = ";
                        break;
                    default:
                        cout << " unknown code " << comPointer->code;

                }

                pOperand = comPointer->right;

                if (pOperand != NULL) {

                    cout << pOperand->value << "";
                }

            }

            if (orPointer->rightOr) {

                cout << " OR ";

            }

            orPointer = orPointer->rightOr;

        }

        if (andPointer->rightAnd) {

            cout << ") AND (";
        }

        andPointer = andPointer->rightAnd;

    }

    cout << ")" << endl;

}

int main() {
    if (getcwd(statistics_path, sizeof(statistics_path)) != NULL) {
        strcpy(catalog_path, statistics_path);
        strcpy(meta_statistics_path, statistics_path);
        strcat(catalog_path, "/catalog");
        strcat(statistics_path, "/Statistics.txt");
        strcat(meta_statistics_path, "/meta/Statistics.txt");
    } else {
        cerr << "error while getting current dir" << endl;
        return 1;
    }

    createSchemaMap();

    cout << "Input :" << endl;;
    yyparse();

//  Load initial statistics from meta folder
    statistics.Read(meta_statistics_path);
    statistics.Write(statistics_path);

    vector<string> tableList;
    map<string, string> aliasMap;
    map<string, Schema *> schemaMap;

    TableList *curTable = tables;

    while (curTable) {
        cout << curTable->tableName << endl;

        if (schemas.count(curTable->tableName) == 0) {
            cerr << "Error: Table hasn't been created!" << endl;
            return 0;
        }
        statistics.CopyRel(curTable->tableName, curTable->aliasAs);
        copySchema(schemaMap, curTable->tableName, curTable->aliasAs);
//        tableList.push_back(curTable->aliasAs);
        tableList.insert(tableList.begin(), curTable->aliasAs);
        aliasMap[curTable->aliasAs] = curTable->tableName;
        curTable = curTable->next;
    }

    statistics.Write(statistics_path);


    vector<vector<string>> orderList = createOrderList(tableList);

    int bestOrderIndex = 0;

    int orderSize = orderList[0].size();
    if (orderSize == 1) {
        char **relNames = new char *[1];
        relNames[0] = new char[orderList[0][0].size() + 1];
        strcpy(relNames[0], orderList[0][0].c_str());
//        minEstimation = s.Estimate(boolean, relNames, 1);
    } else {
        double minEstimation = DBL_MAX;

        for (int i = 0; i < orderList.size(); ++i) {
            statistics.Read(statistics_path);

            double result = 0;
            char **relNames = new char *[orderSize];
            for (int j = 0; j < orderSize; ++j) {
                relNames[j] = new char[orderList[i][j].size() + 1];
                strcpy(relNames[j], orderList[i][j].c_str());
            }

            for (int j = 2; j <= orderSize; ++j) {
                result += statistics.Estimate(boolean, relNames, j);
                statistics.Apply(boolean, relNames, j);
            }

            if (result < minEstimation) {
                minEstimation = result;
                bestOrderIndex = i;
            }
        }
    }

//    cout << "order size: " << orderList.size() << endl;
//    cout << "best order: " << bestOrderIndex << endl;

    vector<string> bestOrder = orderList[bestOrderIndex];

    Operator *left = new SelectFileOperator(boolean, schemaMap[bestOrder[0]], aliasMap[bestOrder[0]]);

    Operator *root = left;
    for (int i = 1; i < orderSize; ++i) {
        Operator *right = new SelectFileOperator(boolean, schemaMap[bestOrder[i]],
                                                 aliasMap[bestOrder[i]]);
//        cout << "chao chao join " << endl;
//        cout << "parsetree" << endl;
//        PrintParseTree(boolean);
//
//        cout << "left " << endl;
//        left->getSchema()->Print();
//        cout << "right " << endl;
//        right->getSchema()->Print();
//        cout << "chao chao " << endl;
        root = new JoinOperator(left, right, boolean);
        boolean = boolean->rightAnd;
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

    printRecursively(root);

    return 0;

}