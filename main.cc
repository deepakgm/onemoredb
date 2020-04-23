#include <map>
#include <climits>
#include "Statistics.h"
#include "Comparison.h"
#include <vector>
#include "Function.h"
#include "ParseTree.h"
#include <string>
#include <cstring>
#include <iostream>
#include "Schema.h"
#include <algorithm>
#include "DBFile.h"
//#include "PrintParseTree.h"

extern "C"
{
int yyparse(void);
}

using namespace std;

extern struct FuncOperator *finalFunction;
extern int distinctAtts;
const int nlineitem = 6001215;
const int norders = 1500000;
extern int distinctFunc;
extern struct TableList *tables;
char *part = "part";
char *lineitem = "lineitem";
const int nregion = 5;
const int nsupplier = 10000;
extern struct AndList *boolean;
char *nation = "nation";
const int ncustomer = 150000;
const int npartsupp = 800000;
char *customer = "customer";
char *orders = "orders";
extern struct NameList *groupingAtts;
const int npart = 200000;
const int nnation = 25;
extern struct NameList *attsToSelect;
char *supplier = "supplier";
static int newBufValue = 0;
int bufOutputVal = 0;
char *partsupp = "partsupp";
char *region = "region";



enum NodeType
{
    G,
    SF,
    SP,
    P,
    D,
    S,
    GB,
    J,
    W
};



int getCode(struct ComparisonOp *comPointer)
{
    if (comPointer != NULL)
    {
        switch (comPointer->code)
        {
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
    }
    return 1;
}

int PrintParse(struct AndList *andPointer)
{
    cout << "(";
    if (andPointer != NULL)
    {
        struct OrList *orPointer = andPointer->left;
        while (orPointer)
        {
            struct ComparisonOp *comPointer = orPointer->left;

            if (comPointer != NULL)
            {
                struct Operand *pOperand = comPointer->left;
                if (pOperand != NULL)
                {
                    cout << pOperand->value << "";
                }
                getCode(comPointer);

                pOperand = comPointer->right;

                if (pOperand != NULL)
                {
                    cout << pOperand->value << "";
                }
            }

            if (orPointer->rightOr)
            {
                cout << " OR ";
            }
            orPointer = orPointer->rightOr;
        }

        cout << ")" << endl;
    }
    return 1;
}

int getPid()
{
    return ++newBufValue;
}

class QueryNode
{

public:
    int numUniqID;

    NodeType nodeValueT;
    Schema schemaVal;

    QueryNode();
    QueryNode(NodeType type) : nodeValueT(type) {}

    ~QueryNode() {}
    virtual void Print(){};
};

// void PrintParseTree(struct AndList *inputPtr)
// {
// 	cout << "(";

// 	struct OrList *finalPtr = inputPtr->left;
// 	while (finalPtr)
// 	{
// 		struct ComparisonOp *compEngPtr = finalPtr->left;

// 		if (compEngPtr != NULL)
// 		{
// 			struct Operand *optPtr = compEngPtr->left;
// 			if (optPtr != NULL)
// 			{
// 				cout << optPtr->value << "";
// 			}

// 			switch (compEngPtr->code)
// 			{
// 			case LESS_THAN:
// 				cout << " < ";
// 				break;
// 			case GREATER_THAN:
// 				cout << " > ";
// 				break;
// 			case EQUALS:
// 				cout << " = ";
// 				break;
// 			default:
// 				cout << " unknown code " << compEngPtr->code;
// 			}

// 			optPtr = compEngPtr->right;

// 			if (optPtr != NULL)
// 			{
// 				cout << optPtr->value << "";
// 			}
// 		}

// 		if (finalPtr->rightOr)
// 		{
// 			cout << " OR ";
// 		}
// 		finalPtr = finalPtr->rightOr;
// 	}

// 	cout << ")" << endl;
// }

class JoinNode : public QueryNode
{

public:
    QueryNode *left;
    int uniqIdVal;
    QueryNode *right;
    CNF myCnf;
    Record rec;

    JoinNode() : QueryNode(J) {}
    ~JoinNode()
    {

        if (left)
            delete left;
        if (right)
            delete right;
    }

    void Print()
    {
        left->Print();
        right->Print();
        cout << endl;
        cout << " ***********" << endl;
        cout << "JOIN" << endl;
        cout << "Left Input Pipe " << uniqIdVal << endl;
        cout << "Right Input Pipe " << right->numUniqID << endl;
        cout << "Output Pipe " << numUniqID << endl;
        cout << "Output Schema: " << endl;
        schemaVal.Print();
        cout << "CNF: " << endl;

        AndList *andList = boolean;
        AndList *previousNode = NULL;
        PrintParse(boolean);
        boolean = boolean->rightAnd;
    }
};

class ProjectNode : public QueryNode
{

public:
    int inputVal1;
    int outputVal2;
    int *attsToKeep;

    QueryNode *qNode;

    ProjectNode() : QueryNode(P) {}
    void Print()
    {
        qNode->Print();
        cout << endl;
        cout << " ***********" << endl;
        cout << "PROJECT" << endl;
        cout << "Input Pipe " << qNode->numUniqID << endl;
        cout << "Output Pipe " << numUniqID << endl;
        cout << "Output Schema:" << endl;
        cout << "NumAtts = " << outputVal2 << endl;
        cout << "Att " << attsToSelect->name << endl;
    }

    ~ProjectNode()
    {

        if (attsToKeep)
            delete[] attsToKeep;
    }
};

class NewProjNode : public QueryNode
{

public:
    int inputVal1;
    int outputVal2;

    //QueryNode *qNode;

    NewProjNode() : QueryNode(P) {}
    void newOutputVal()
    {
        cout << endl;
        cout << "Output Pipe " << numUniqID << endl;
    }

    ~NewProjNode()
    {
    }
};

class DistinctNode : public QueryNode
{

public:
    QueryNode *qNode;

    DistinctNode() : QueryNode(D) {}
    ~DistinctNode()
    {

        if (qNode)
            delete qNode;
    }

    void Print()
    {
        qNode->Print();
        cout << endl;
        cout << " ***********" << endl;
        cout << "DUPLICATION ELIMINATION Operation" << endl;
        cout << "Input Pipe ID : " << qNode->numUniqID << endl;
        cout << "Output Pipe ID : " << numUniqID << endl;
    }
};

class SelectFileNode : public QueryNode
{

public:
    bool checkIfOpen;
    CNF myCnf;
    Record rec;
    QueryNode *qNode;
    DBFile mydbFile;

    SelectFileNode() : QueryNode(SF) {}
    ~SelectFileNode()
    {
        if (qNode)
            delete qNode;
        if (checkIfOpen)
        {
            mydbFile.Close();
        }
    }

    void Print()
    {
        cout << endl;
        cout << " ***********" << endl;
        cout << "SELECT FILE" << endl;
        cout << "Input Pipe 0" << endl;
        cout << "Output Pipe " << numUniqID << endl;
        cout << "Output Schema:" << endl;
        schemaVal.Print();
        if (qNode)
            qNode->Print();
    }
};

class SumNode : public QueryNode
{

public:
    Function compute;
    QueryNode *qNode;
    FuncOperator *funcOperator;
    int distinctFunc;

    SumNode() : QueryNode(S) {}
    ~SumNode()
    {

        if (qNode)
            delete qNode;
    }

    void Print()
    {
        qNode->Print();
        cout << endl;
        cout << " *********** " << endl;
        cout << "SUM Operation" << endl;
        cout << "Input Pipe ID : " << qNode->numUniqID << endl;
        cout << "Output Pipe ID : " << numUniqID << endl;
        cout << "Function :" << endl;
        PrintFunction(funcOperator);
        cout << endl;
        cout << "distinctFunc: " << distinctFunc << endl;
    }

    void PrintFunction(FuncOperator *func)
    {
        if (func)
        {
            cout << "(";
            PrintFunction(func->leftOperator);
            if (func->leftOperand) {
                cout << func->leftOperand->value;
            }
            if (func->code)
            {
                switch (func->code) {
                    case '+':
                        cout << " + ";
                        break;
                    case '-':
                        cout << " - ";
                        break;
                    case '*':
                        cout << " * ";
                        break;
                    case '/':
                        cout << " / ";
                        break;

                }
            }
            PrintFunction(func->right);
            cout << ")";
        }
    }
};

class SelectPipeNode : public QueryNode
{

public:
    CNF myCnf;
    Record rec;

    SelectPipeNode() : QueryNode(SP) {}
    ~SelectPipeNode() {}

    void Print()
    {
        cout << endl;
        cout << " ***********" << endl;
        cout << "SELECT PIPE Operation" << endl;
        cout << "Input Pipe " << bufOutputVal << endl;
        cout << "Output Pipe " << numUniqID << endl;
        cout << "Output Schema:" << endl;
        schemaVal.Print();
        cout << "SELECTION CNF :" << endl;
        AndList *andList = boolean;

        bool andListPresent = false;
        while(andList) {
            bool orListPresent = false;
            OrList *orList = andList->left;
            while(orList) {
                if (orList->left->left->code == NAME ^ orList->left->right->code == NAME) {
                    Operand *nameOperand = orList->left->left->code == NAME ? orList->left->left : orList->left->right;
                    if (schemaVal.Find(nameOperand->value) != -1) {
                        if (orListPresent) {
                            cout << " OR ";
                        } else if (andListPresent) {
                            cout << " AND (";
                            andListPresent = true;
                        } else {
                            cout << " (";
                        }
                        orListPresent = true;
                        cout << "(" << nameOperand->value;
                        ComparisonOp *comparisonOp = orList->left;
                        switch (comparisonOp->code) {
                            case EQUALS:
                                cout << " = ";
                                break;
                            case GREATER_THAN:
                                cout << " > ";
                                break;
                            case LESS_THAN:
                                cout << " < ";
                                break;
                        }
                        cout << comparisonOp->right->value << ")";
                    }
                }
                orList = orList->rightOr;
            }
            if (orListPresent) {
                cout << ")";
            }
            andList = andList->rightAnd;
        }
    }
};

class GroupByNode : public QueryNode
{

public:
    QueryNode *qNode;

    Function compute;
    FuncOperator *funcOperator;
    OrderMaker group;
    int distinctFunc;

    GroupByNode() : QueryNode(GB) {}
    ~GroupByNode()
    {

        if (qNode)
            delete qNode;
    }

    void Print()
    {
        qNode->Print();
        cout << endl;
        cout << " ***********" << endl;
        cout << "GROUP BY" << endl;
        cout << "Left Input Pipe " << qNode->numUniqID << endl;
        cout << "Output Pipe " << numUniqID << endl;
        cout << "Output Schema: " << endl;
        schemaVal.Print();
        cout << "OrderMaker : " << endl;
        group.Print();
        cout << "Function :" << endl;
        PrintFunction(funcOperator);
        cout << endl;
        cout << "distinctFunc: " << distinctFunc << endl;
    }

    void PrintFunction(FuncOperator *func)
    {
        if (func)
        {
            cout << "(";
            PrintFunction(func->leftOperator);
            if (func->leftOperand) {
                cout << func->leftOperand->value;
            }
            if (func->code)
            {
                switch (func->code) {
                    case '+':
                        cout << " + ";
                        break;
                    case '-':
                        cout << " - ";
                        break;
                    case '*':
                        cout << " * ";
                        break;
                    case '/':
                        cout << " / ";
                        break;

                }
            }
            PrintFunction(func->right);
            cout << ")";
        }
    }
};

class WriteOutNode : public QueryNode
{

public:
    QueryNode *qNode;

    FILE *output;

    WriteOutNode() : QueryNode(W) {}
    ~WriteOutNode()
    {

        if (qNode)
            delete qNode;
    }

    void Print()
    {

        cout << endl;
        cout << " ***********" << endl;
        cout << "WRITE OUT Operation" << endl;
        cout << "Input Pipe ID : " << qNode->numUniqID << endl;
        qNode->Print();
    }
};

typedef map<string, Schema> map1;
typedef map<string, string> map2;

void CopyTablesNamesAndAliases(TableList *tbL, Statistics &sts, vector<char *> &tblNam, map2 &map)
{
    while (tbL)
    {
        sts.CopyRel(tbL->tableName, tbL->aliasAs);
        map[tbL->aliasAs] = tbL->tableName;
        tblNam.push_back(tbL->aliasAs);
        tbL = tbL->next;
    }
}

void initStatistics(Statistics &sts)
{

    sts.AddRel(region, nregion);
    sts.AddRel(part, npart);
    sts.AddRel(customer, ncustomer);
    sts.AddRel(supplier, nsupplier);
    sts.AddRel(nation, nnation);
    sts.AddRel(lineitem, nlineitem);
    sts.AddRel(partsupp, npartsupp);
    sts.AddRel(orders, norders);

    // nation
    sts.AddAtt(nation, "n_nationkey", nnation);
    sts.AddAtt(nation, "n_name", nnation);
    sts.AddAtt(nation, "n_regionkey", nregion);
    sts.AddAtt(nation, "n_comment", nnation);

    // partsupp
    sts.AddAtt(partsupp, "ps_partkey", npart);
    sts.AddAtt(partsupp, "ps_suppkey", nsupplier);
    sts.AddAtt(partsupp, "ps_availqty", npartsupp);
    sts.AddAtt(partsupp, "ps_supplycost", npartsupp);
    sts.AddAtt(partsupp, "ps_comment", npartsupp);

    // lineitem
    sts.AddAtt(lineitem, "l_orderkey", norders);
    sts.AddAtt(lineitem, "l_partkey", npart);
    sts.AddAtt(lineitem, "l_suppkey", nsupplier);
    sts.AddAtt(lineitem, "l_linenumber", nlineitem);
    sts.AddAtt(lineitem, "l_quantity", nlineitem);
    sts.AddAtt(lineitem, "l_extendedprice", nlineitem);
    sts.AddAtt(lineitem, "l_discount", nlineitem);
    sts.AddAtt(lineitem, "l_tax", nlineitem);
    sts.AddAtt(lineitem, "l_returnflag", 3);
    sts.AddAtt(lineitem, "l_linestatus", 2);
    sts.AddAtt(lineitem, "l_shipdate", nlineitem);
    sts.AddAtt(lineitem, "l_commitdate", nlineitem);
    sts.AddAtt(lineitem, "l_receiptdate", nlineitem);
    sts.AddAtt(lineitem, "l_shipinstruct", nlineitem);
    sts.AddAtt(lineitem, "l_shipmode", 7);
    sts.AddAtt(lineitem, "l_comment", nlineitem);

    // supplier
    sts.AddAtt(supplier, "s_suppkey", nsupplier);
    sts.AddAtt(supplier, "s_name", nsupplier);
    sts.AddAtt(supplier, "s_address", nsupplier);
    sts.AddAtt(supplier, "s_nationkey", nnation);
    sts.AddAtt(supplier, "s_phone", nsupplier);
    sts.AddAtt(supplier, "s_acctbal", nsupplier);
    sts.AddAtt(supplier, "s_comment", nsupplier);

    // customer
    sts.AddAtt(customer, "c_custkey", ncustomer);
    sts.AddAtt(customer, "c_name", ncustomer);
    sts.AddAtt(customer, "c_address", ncustomer);
    sts.AddAtt(customer, "c_nationkey", nnation);
    sts.AddAtt(customer, "c_phone", ncustomer);
    sts.AddAtt(customer, "c_acctbal", ncustomer);
    sts.AddAtt(customer, "c_mktsegment", 5);
    sts.AddAtt(customer, "c_comment", ncustomer);

    // region
    sts.AddAtt(region, "r_regionkey", nregion);
    sts.AddAtt(region, "r_name", nregion);
    sts.AddAtt(region, "r_comment", nregion);

    // orders
    sts.AddAtt(orders, "o_orderkey", norders);
    sts.AddAtt(orders, "o_custkey", ncustomer);
    sts.AddAtt(orders, "o_orderstatus", 3);
    sts.AddAtt(orders, "o_totalprice", norders);
    sts.AddAtt(orders, "o_orderdate", norders);
    sts.AddAtt(orders, "o_orderpriority", 5);
    sts.AddAtt(orders, "o_clerk", norders);
    sts.AddAtt(orders, "o_shippriority", 1);
    sts.AddAtt(orders, "o_comment", norders);

    // part
    sts.AddAtt(part, "p_partkey", npart);
    sts.AddAtt(part, "p_name", npart);
    sts.AddAtt(part, "p_mfgr", npart);
    sts.AddAtt(part, "p_brand", npart);
    sts.AddAtt(part, "p_type", npart);
    sts.AddAtt(part, "p_size", npart);
    sts.AddAtt(part, "p_container", npart);
    sts.AddAtt(part, "p_retailprice", npart);
    sts.AddAtt(part, "p_comment", npart);

}


void PrintNameList(NameList *nameList)
{
    while (nameList)
    {
        cout << nameList->name << endl;
        nameList = nameList->next;
    }
}

void PrintTablesAliases(TableList *tbL)
{
    while (tbL)
    {
        cout << "Table " << tbL->tableName;
        cout << " is aliased to " << tbL->aliasAs << endl;
        tbL = tbL->next;
    }
}

void CopyNameList(NameList *nameList, vector<string> &names)
{

    while (nameList)
    {
        names.push_back(string(nameList->name));
        nameList = nameList->next;
    }
}


void initSchemaMap(map1 &map)
{

    map[string(region)] = Schema("catalog", region);
    map[string(nation)] = Schema("catalog", nation);
    map[string(customer)] = Schema("catalog", customer);
    map[string(part)] = Schema("catalog", part);
    map[string(supplier)] = Schema("catalog", supplier);
    map[string(partsupp)] = Schema("catalog", partsupp);
    map[string(orders)] = Schema("catalog", orders);
    map[string(lineitem)] = Schema("catalog", lineitem);
}

int main()
{

    cout << "enter: " << endl;

    yyparse();

    vector<char *> tblNam;
    map2 inputMap1;
    map1 inputMap2;
    vector<char *> joinOrder;
    vector<char *> buffer(2);
    Statistics sts;
    initSchemaMap(inputMap2);
    initStatistics(sts);

    CopyTablesNamesAndAliases(tables, sts, tblNam, inputMap1);

    sort(tblNam.begin(), tblNam.end());

    int minCost = INT_MAX, cost = 0;
    int counter = 1;
    int grp = 0;
    do
    {
        Statistics temp(sts);
        auto indx = tblNam.begin();
        buffer[0] = *indx;
        indx++;

        while (indx != tblNam.end())
        {
            buffer[1] = *indx;
            cost += temp.Estimate(boolean, &buffer[0], 2);
            temp.Apply(boolean, &buffer[0], 2);

            if (cost <= 0 || cost > minCost)
            {
                break;
            }
            indx++;
        }

        if (cost >= 0 && cost < minCost)
        {
            minCost = cost;
            joinOrder = tblNam;
        }
        cost = 0;

    } while (next_permutation(tblNam.begin(), tblNam.end()));

    QueryNode *newNode;
    int NewValue11 = 0;
    auto indx = joinOrder.begin();
    char fp[50];
    SelectFileNode *sf = new SelectFileNode();
    SelectPipeNode *pp = new SelectPipeNode();
    pp->schemaVal = Schema(inputMap2[inputMap1[*indx]]);
    pp->schemaVal.Reseat(*indx);
    pp->myCnf.GrowFromParseTree(boolean, &(pp->schemaVal), pp->rec);
    sf->checkIfOpen = true;
    sf->numUniqID = getPid();
    sf->schemaVal = Schema(inputMap2[inputMap1[*indx]]);
    sf->schemaVal.Reseat(*indx);
    sf->myCnf.GrowFromParseTree(boolean, &(sf->schemaVal), sf->rec);
    sf->qNode = pp;
    bufOutputVal = sf->numUniqID;
    pp->numUniqID = getPid();

    indx++;
    if (indx == joinOrder.end())
    {
        newNode = sf;
    }
    else
    {
        JoinNode *joinNode = new JoinNode();
        joinNode->left = sf;
        joinNode->uniqIdVal = pp->numUniqID;

        sf = new SelectFileNode();
        sf->checkIfOpen = true;
        sf->numUniqID = getPid();
        sf->schemaVal = Schema(inputMap2[inputMap1[*indx]]);
        sf->schemaVal.Reseat(*indx);
        sf->myCnf.GrowFromParseTree(boolean, &(sf->schemaVal), sf->rec);

        joinNode->right = sf;
        joinNode->schemaVal.JoinSchema(joinNode->left->schemaVal, joinNode->right->schemaVal);
        joinNode->myCnf.GrowFromParseTree(boolean, &(joinNode->left->schemaVal), &(joinNode->right->schemaVal), joinNode->rec);
        indx++;
        NewValue11++;
        joinNode->numUniqID = getPid();

        while (indx != joinOrder.end())
        {
            JoinNode *p = joinNode;

            sf = new SelectFileNode();
            sf->checkIfOpen = true;
            sf->numUniqID = getPid();
            sf->schemaVal = Schema(inputMap2[inputMap1[*indx]]);
            sf->schemaVal.Reseat(*indx);
            sf->myCnf.GrowFromParseTree(boolean, &(sf->schemaVal), sf->rec);

            joinNode = new JoinNode();
            joinNode->numUniqID = getPid();
            joinNode->left = p;
            joinNode->uniqIdVal = joinNode->left->numUniqID;
            joinNode->right = sf;

            joinNode->schemaVal.JoinSchema(joinNode->left->schemaVal, joinNode->right->schemaVal);
            joinNode->myCnf.GrowFromParseTree(boolean, &(joinNode->left->schemaVal), &(joinNode->right->schemaVal), joinNode->rec);
            indx++;
            NewValue11++;
        }
        newNode = joinNode;
    }
    QueryNode *temp = newNode;
    if (groupingAtts)
    {
        newNode = new GroupByNode();
        vector<string> groupAtts;
        CopyNameList(groupingAtts, groupAtts);
        newNode->numUniqID = getPid();
        ((GroupByNode *)newNode)->compute.GrowFromParseTree(finalFunction, temp->schemaVal);
        ((GroupByNode *)newNode)->funcOperator = finalFunction;
        ((GroupByNode *)newNode)->distinctFunc = distinctFunc;
        int* keepMe;

        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        Schema sumSchema(NULL, 1, ((GroupByNode *)newNode)->compute.ReturnInt() ? atts[0] : atts[1]);
        Schema groupBySchema(&temp->schemaVal, groupingAtts, keepMe);
        newNode->schemaVal.JoinSchema(sumSchema , groupBySchema);

        NameList *nameList = new NameList();
        nameList->name = "sum";
        nameList->next = attsToSelect;
        attsToSelect = nameList;

        // newNode->schemaVal.GroupBySchema(temp->schemaVal, ((GroupByNode *)newNode)->compute.ReturnInt());

        ((GroupByNode *)newNode)->group.growFromParseTree(groupingAtts, &(newNode->schemaVal));
        ((GroupByNode *)newNode)->qNode = temp;
        grp = ((GroupByNode *)newNode)->group.Printgrp();
        temp = ((GroupByNode *)newNode);
    }

    if (!groupingAtts && finalFunction)
    {
        newNode = new SumNode();
        newNode->numUniqID = getPid();
        ((SumNode *)newNode)->compute.GrowFromParseTree(finalFunction, temp->schemaVal);
        ((SumNode *)newNode)->funcOperator = finalFunction;
        ((SumNode *)newNode)->distinctFunc = distinctFunc;
        Attribute atts[2][1] = {{{"sum", Int}}, {{"sum", Double}}};
        newNode->schemaVal = Schema(NULL, 1, ((SumNode *)newNode)->compute.ReturnInt() ? atts[0] : atts[1]);
        ((SumNode *)newNode)->qNode = temp;
        temp = ((SumNode *)newNode);
    }

    if (attsToSelect)
    {
        newNode = new ProjectNode();
        vector<string> atts;
        CopyNameList(attsToSelect, atts);
        vector<int> attsToKeep;
        newNode->numUniqID = getPid();
        newNode->schemaVal.ProjectSchema(temp->schemaVal, atts, attsToKeep);
        ((ProjectNode *)newNode)->attsToKeep = &attsToKeep[0];
        ((ProjectNode *)newNode)->inputVal1 = temp->schemaVal.GetNumAtts();
        ((ProjectNode *)newNode)->outputVal2 = atts.size();
        ((ProjectNode *)newNode)->qNode = temp;
        temp = ((ProjectNode *)newNode);
    }

    if (distinctAtts)
    {
        newNode = new DistinctNode();
        newNode->numUniqID = getPid();
        newNode->schemaVal = temp->schemaVal;
        ((DistinctNode *)newNode)->qNode = temp;
        temp = ((DistinctNode *)newNode);
    }

    cout << "Number of selects: 1" << endl;
    cout << "Number of joins: " << NewValue11 << endl;
    if (groupingAtts)
    {
        cout << "GROUPING ON ";
        cout << groupingAtts->name << endl;
        cout << "	Att " << groupingAtts->name << ":";
        if (grp == 1)
            cout << " Int";
        else if (grp == 2)
            cout << " Double";
        else
            cout << " String";
    }
    cout << endl;
    cout << "IN ORDER TRAVERSAL " << endl;
    newNode->Print();

    return 0;
}

void PrintFunction(FuncOperator *func)
{
    if (func)
    {
        cout << "(";
        PrintFunction(func->leftOperator);
        cout << func->leftOperand->value << " ";
        if (func->code)
        {
            cout << " " << func->code << " ";
        }
        PrintFunction(func->right);
        cout << ")";
    }
}
