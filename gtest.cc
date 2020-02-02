#include <limits.h>
#include "gtest/gtest.h"
#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}



class DBFileTest : public ::testing::Test {
public:
    DBFile *dbFile;
protected:
    virtual void SetUp() {
        clog << "creating DBFile instance.." << endl;
        DBFile *dbFile=new DBFile();
    }

    virtual void TearDown() {
        clog << "clearing memory.." << endl;
        delete dbFile;
    }
};

TEST_F(DBFileTest,test1){
    //given
    Record record;
    //when
    int r=dbFile->GetNext(record);
    //assert
    ASSERT_EQ(r,0);
}