#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include <stdlib.h>
//#include "gmock/gmock.h"
#include <cstring>
#include "Statistics.h"
#include "testkit.h"

using namespace std;

char stat_file_path[PATH_MAX];
char cur_dir1[PATH_MAX];
char *relName[] = {"relation","relation2"};

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
        s.relMap.clear();
        s.attrMap.clear();
    }
};

//Add rel test
TEST_F(StatisticTest, addRelTest1) {

    ASSERT_EQ(s.relMap.size(),0);
    ASSERT_EQ(s.attrMap.size(),0);

    s.AddRel(relName[0],800000);
    s.AddAtt(relName[0], "key", 10000);

    ASSERT_EQ(s.relMap.size(),1);
    ASSERT_EQ(s.attrMap.size(),1);
}

//Copy rel test
TEST_F(StatisticTest, copyRelTest1) {

    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key",10000);

    ASSERT_EQ(s.relMap.size(),1);

    s.CopyRel("relation","relationCopy");

    ASSERT_EQ(s.relMap.size(),2);
}


//Write and Read Test
TEST_F(StatisticTest, readWriteTest1) {
    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "key",10000);

    ASSERT_EQ(s.relMap.size(),1);

    s.Write(stat_file_path);

    s.relMap.clear();
    s.attrMap.clear();

    s.Read(stat_file_path);
    ASSERT_EQ(s.relMap.size(),1);
}


//Estimate Test
TEST_F(StatisticTest, estimateTest1) {
    s.AddRel(relName[0],1000);
    s.AddAtt(relName[0], "key",100);

    s.AddRel(relName[1],1);
    s.AddAtt(relName[1], "key2",1);

    char *cnf = "(key = key2)";

    yy_scan_string(cnf);
    yyparse();
    double result = s.Estimate(final, relName, 2);
//
    cout <<result <<endl;

//    s.Write(stat_file_path);

//    char *cnf = "(key = key2)";
//
//    yy_scan_string(cnf);
//    yyparse();
//    double result = s.Estimate(final, relName, 2);

}
