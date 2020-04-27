#ifndef NODE_H
#define NODE_H

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

enum OpType
{
    SELECT_FILE,
    SELECT_PIPE,
    PROJECT,
    JOIN,
    GROUPBY,
    DUPLICATE_REMOVAL,
    SUM
};

static int PIPE_ID = 1;

static int generatePipeId()
{
    return PIPE_ID++;
}

class Operator
{
protected:
    int pipeId = generatePipeId();
    OpType opType;
    Schema *outputSchema = nullptr;

public:
    Operator *left = nullptr, *right = nullptr;
    Pipe outPipe = Pipe(50);

    virtual void print() = 0;
    virtual void run() = 0;
    int getPipeID() { return pipeId; };

    Schema *getSchema() { return outputSchema; };

    OpType getType() { return opType; }
};

class SelectFileOperator : public Operator
{
private:
    CNF cnf;
    Record literal;
    DBFile dbfile;
    SelectFile sf;
    string dbfilePath;

public:
    SelectFileOperator(AndList *selectList, Schema *schema, string relName);
    void run();
    void print() override;
};

class SelectPipeOperator : public Operator
{
private:
    CNF cnf;
    Record literal;
    SelectPipe sp;

public:
    SelectPipeOperator(Operator *child, AndList *selectList);
    void print();
    void run();
};

class ProjectOperator : public Operator
{
private:
    int *keepMe;
    NameList *attsLeft;
    Project p;

public:
    ProjectOperator(Operator *child, NameList *attrsLeft);
    void run();

    void print();
};

class JoinOperator : public Operator
{
private:
    CNF cnf;
    Record literal;
    Join j;
    //    Schema* leftSchema;
    //    Schema* rightSchema;

public:
    JoinOperator(Operator *leftChild, Operator *rightChild, AndList *joinList);
    void run();
    void print();
};

class DuplicateRemovalOperator : public Operator
{
private:
    DuplicateRemoval dr;
public:
    DuplicateRemovalOperator(Operator *child);
    void run();
    void print();
};

class SumOperator : public Operator
{
private:
    FuncOperator *funcOperator;
    Function function;
    Sum s;
public:
    SumOperator(Operator *child, FuncOperator *func);
    void run();
    void print();
};

class GroupByOperator : public Operator
{
private:
    FuncOperator *funcOperator;
    OrderMaker groupOrder;
    Function function;
    GroupBy gb;
    void getOrder(NameList *groupingAtts);

public:
    GroupByOperator(Operator *child, NameList *groupingAtts, FuncOperator *func);
    GroupByOperator(Operator *child, OrderMaker orderMaker);
    void createOutputSchema();
    void print();
    void run();
};

static string funcToString(FuncOperator *funcOperator)
{
    string result;

    if (funcOperator)
    {
        if (funcOperator->leftOperator)
        {
            result.append(funcToString(funcOperator->leftOperator));
        }
        if (funcOperator->leftOperand)
        {
            result.append(funcOperator->leftOperand->value);
        }
        switch (funcOperator->code)
        {
        case 42:
            result.append("*");
            break;
        case 43:
            result.append("+");
            break;
        case 44:
            result.append("/");
            break;
        case 45:
            result.append("-");
            break;
        }
        if (funcOperator->right)
        {
            result.append(funcToString(funcOperator->right));
        }
    }
    return result;
}
#endif