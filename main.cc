#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include <unistd.h>

using namespace std;

int main() {
    cout << "hello" <<endl;
    char cur_dir[PATH_MAX];
    char dbfile_dir[PATH_MAX];
    char table_path[PATH_MAX];
    char catalog_path[PATH_MAX];
    char tempfile_path[PATH_MAX];
    if (getcwd(cur_dir, sizeof(cur_dir)) != NULL) {
        clog <<"current working dir:" << cur_dir << endl;
        strcpy(dbfile_dir,cur_dir);
        strcpy(table_path,cur_dir);
        strcpy(catalog_path,cur_dir);
        strcpy(tempfile_path,cur_dir);
        strcat(dbfile_dir,"/test/test.bin");
        strcat(table_path,"/test/nation.tbl");
        strcat(catalog_path,"/test/catalog");
        strcat(tempfile_path,"/test/tempfile");
    } else {
        cerr << "error while getting curent dir" << endl;
        return 1;
    }

    DBFile* dbFile=new DBFile();
    dbFile->Create(dbfile_dir,heap,NULL);
    Schema nation (catalog_path, (char*)"nation");
    dbFile->Load(nation,table_path);

    dbFile->MoveFirst();


    Record record;
    dbFile->GetNext(record);
    record.Print(&nation);

    dbFile->Close();
}