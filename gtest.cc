#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include "Statistics.h"
#include "testkit.h"

using namespace std;

char stat_file_path[PATH_MAX];
char cur_dir1[PATH_MAX];
char *relName[] = {"relation1","relation2"};

int main(int argc, char **argv) {
    // get the current dir
    if (getcwd(cur_dir1, sizeof(cur_dir1)) != NULL) {
        clog << "current working dir:" << cur_dir1 << endl;
        strcat(stat_file_path, "Statistics.txt");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}


class StatisticTest : public ::testing::Test {
public:
    Statistics s;
protected:
    virtual void SetUp() {
        clog << "initializing test" << endl;
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        s.relationMap->clear();
        s.attrMap->clear();
    }
};


//Add rel test
TEST_F(StatisticTest, addRelTest1) {

    ASSERT_EQ(s.relationMap->size(),0);
    ASSERT_EQ(s.attrMap->size(),0);

    s.AddRel(relName[0],800000);
    s.AddAtt(relName[0], "key", 10000);

    ASSERT_EQ(s.relationMap->size(),1);
    ASSERT_EQ(s.attrMap->size(),1);
}

//Copy rel test
TEST_F(StatisticTest, copyRelTest1) {

    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key",10000);

    ASSERT_EQ(s.relationMap->size(),1);

    s.CopyRel("relation1","relation1Copy");

    ASSERT_EQ(s.relationMap->size(),2);
}


//Write and Read Test
TEST_F(StatisticTest, readWriteTest1) {
    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key",10000);

    ASSERT_EQ(s.relationMap->size(),1);

    s.Write(stat_file_path);

    s.relationMap->clear();
    s.attrMap->clear();

    s.Read(stat_file_path);
    ASSERT_EQ(s.relationMap->size(),1);
}

//Estimate Test 1
TEST_F(StatisticTest, estimateTest1) {
    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key1",25);


    char *cnf = "(key1 > 1)";
    yy_scan_string(cnf);
    yyparse();

    double result = s.Estimate(final, relName, 2);

    ASSERT_LE(fabs(result-3333.34),0.01);
}



//Estimate Test 2
TEST_F(StatisticTest, estimateTest2) {
    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key1",25);

    s.AddRel(relName[1],25);
    s.AddAtt(relName[1], "key2",25);

    
    char *cnf = "(key1 = key2)";
    yy_scan_string(cnf);
    yyparse();

    double result = s.Estimate(final, relName, 2);

    //verify estimate method does not changes state of the object
    ASSERT_EQ(s.relationMap->size() ,2);
}

//Apply Test
TEST_F(StatisticTest, applyTest1) {
    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key1",25);

    s.AddRel(relName[1],25);
    s.AddAtt(relName[1], "key2",25);


    char *cnf = "(key1 = key2)";
    yy_scan_string(cnf);
    yyparse();

    s.Apply(final, relName, 2);

    ASSERT_EQ(s.relationMap->size() ,1);
}
