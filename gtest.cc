#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include <cstring>
#include "Statistics.h"
#include "Operator.h"
#include "Schema.h"
#include "extraFunction.h"
#include <fstream>

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


class DatabaseImp : public ::testing::Test {
public:
MyFucntion myFucntion;
//     Statistics s;
// protected:
//     virtual void SetUp() {
//         clog << "initializing test" << endl;
//     }

//     virtual void TearDown() {
//         clog << "clearing memory.." << endl;
//     }*/
};


//Test UpdateTable method
TEST_F(DatabaseImp, UpdateTable) {
    int x = myFucntion.UpdateTable("abc");
    ifstream fin(myFucntion.DBInfo.c_str());
    string line;
    int count=0;
    while (getline(fin, line))
    {
        // cout<<tableName<<endl;
        if (myFucntion.trim(line).empty())
            continue;
        line = myFucntion.trim(line);
        // cout<<line<<endl;
        if (strcmp(line.c_str(), "abc") == 0)
        {
            ASSERT_NE(x,0);
            count++;
        }
    }
    if(count==0)
        ASSERT_EQ(x,0);
}

//LoadSchema test - yyparse()
TEST_F(DatabaseImp, FireUpExistingDatabase) {
    ofstream ofs(myFucntion.catalog, ifstream ::app);
    ofs <<endl<< "BEGIN" << endl <<"abc"<<endl<<"abc.tbl"<<endl<<"att1 Int"<<endl<<"END"<<endl;
    ofs.close();
    map<string, Schema *> loadSch= myFucntion.FireUpExistingDatabase();
    string tableName = "abc";
    // cout<<loadSch.count(tableName);
    ASSERT_NE(loadSch.count(tableName),0);
}
