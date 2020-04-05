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
//        s.relMap.clear();
//        s.attrMap.clear();
    }
};
/*

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

*/

//Estimate Test
TEST_F(StatisticTest, estimateTest1) {
    char *relName[] = {"supplier","customer","nation"};

    //s.Read(fileName);

    s.AddRel(relName[0],10000);
    s.AddAtt(relName[0], "s_nationkey",25);

    s.AddRel(relName[1],150000);
    s.AddAtt(relName[1], "c_custkey",150000);
    s.AddAtt(relName[1], "c_nationkey",25);

    s.AddRel(relName[2],25);
    s.AddAtt(relName[2], "n_nationkey",25);

    s.CopyRel("nation","n1");
    s.CopyRel("nation","n2");
    s.CopyRel("supplier","s");
    s.CopyRel("customer","c");

    char *set1[] ={"s","n1"};
    char *cnf = "(s.s_nationkey = n1.n_nationkey)";
    yy_scan_string(cnf);
    yyparse();
//    s.Apply(final, set1, 2);

//    char *set2[] ={"c","n2"};
//    cnf = "(c.c_nationkey = n2.n_nationkey)";
//    yy_scan_string(cnf);
//    yyparse();
//    s.Apply(final, set2, 2);
//
//    char *set3[] = {"c","s","n1","n2"};
//    cnf = " (n1.n_nationkey = n2.n_nationkey )";
//    yy_scan_string(cnf);
//    yyparse();
//
//
//    double result = s.Estimate(final, set3, 4);

//    if(fabs(result-60000000.0)>0.1)
//        cout<<"error in estimating Q3\n";

//    s.Apply(final, set3, 4);

    double result = s.Estimate(final, set1, 2);
    cout <<result<<endl;

    s.Write(stat_file_path);

}
