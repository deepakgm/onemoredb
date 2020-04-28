#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include "Statistics.h"
#include "Operator.h"
#include "Schema.h"

using namespace std;

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


char stat_file_path[PATH_MAX];
char cur_dir1[PATH_MAX];
char catalog_path[PATH_MAX];

int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(cur_dir1, sizeof(cur_dir1)) != NULL) {
        clog << "current working dir:" << cur_dir1 << endl;
        strcpy(stat_file_path,cur_dir1);
        strcpy(catalog_path,cur_dir1);
        strcat(stat_file_path, "/Statistics.txt");
        strcat(catalog_path, "/catalog");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


class QueryOptimizationTest : public ::testing::Test {
/*public:
    Statistics s;
protected:
    virtual void SetUp() {
        clog << "initializing test" << endl;
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
    }*/
};


//Test funcToString method
TEST_F(QueryOptimizationTest, funcToStringTest) {
    FuncOperator* root=new(FuncOperator);
    FuncOperator* left=new(FuncOperator);
    FuncOperator* right=new(FuncOperator);

    left->code=42;
    root->code=43;
    right->code=44;
    root->leftOperand=NULL;

    root->leftOperator=left;
    root->right=right;
    string result=funcToString(root);

    ASSERT_EQ(result,"*+/");

    delete root;
    delete left;
    delete right;
}

//Pipe ID generation test
TEST_F(QueryOptimizationTest, pipeIdGenerationTest) {
    SelectFileOperator* sel=new SelectFileOperator(boolean,new Schema("catalog","nation"),"rel");

    int pipeId=sel->getPipeID();
    ASSERT_EQ(pipeId,1);

    ProjectOperator* proj=new ProjectOperator(sel, attsToSelect);
    pipeId=proj->getPipeID();
    ASSERT_EQ(pipeId,2);

    delete sel;
    delete proj;
}

//Operator initialization test
TEST_F(QueryOptimizationTest, operatorInitializationTest) {
    SelectFileOperator* sel=new SelectFileOperator(boolean,new Schema("catalog","nation"),"rel");
    ASSERT_EQ(sel->getType(),SELECT_FILE);
    delete(sel);
}

// groupby  output schema test
TEST_F(QueryOptimizationTest, groupByOutSchemaTest) {
    Schema nation(catalog_path,"nation");
    OrderMaker orderMaker=OrderMaker(&nation);


    SelectFileOperator* sel=new SelectFileOperator(boolean,&nation,"rel");

    int numOfAttr=sel->getSchema()->GetNumAtts();

    ASSERT_EQ(numOfAttr,4);

    GroupByOperator* groupByOperator=new  GroupByOperator(sel,orderMaker);
    groupByOperator->createOutputSchema();
    numOfAttr=groupByOperator->getSchema()->GetNumAtts();
    ASSERT_EQ(numOfAttr,5);

    string attrName=groupByOperator->getSchema()->GetAtts()[0].name;
    ASSERT_EQ(attrName,"SUM");

    delete groupByOperator;
    delete sel;
}