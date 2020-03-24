#include <iostream>
#include "Record.h"
#include <stdlib.h>
#include "DBFile.h"
#include "Schema.h"
#include <limits.h>
#include <cstring>
#include "Meta.h"
#include <chrono>

using namespace std;

const char *settings = "test.cat";

char *catalog_path, *dbfile_dir, *tpch_dir = NULL;

int main() {

    FILE *fp = fopen(settings, "r");
    if (fp) {
        char *mem = (char *) malloc(80 * 3);
        catalog_path = &mem[0];
        dbfile_dir = &mem[80];
        tpch_dir = &mem[160];
        char line[80];
        fgets(line, 80, fp);
        sscanf(line, "%s\n", catalog_path);
        fgets(line, 80, fp);
        sscanf(line, "%s\n", dbfile_dir);
        fgets(line, 80, fp);
        sscanf(line, "%s\n", tpch_dir);
        fclose(fp);
        if (!(catalog_path && dbfile_dir && tpch_dir)) {
            cerr << " Test settings file 'test.cat' not in correct format.\n";
            free(mem);
            exit(1);
        }
    } else {
        cerr << " Test settings files 'test.cat' missing \n";
        exit(1);
    }

    char *tnames[] = {"supplier", "partsupp", "part", "nation", "customer", "orders", "region", "lineitem"};

    for (int i = 0; i < 8; i++) {

        DBFile *dbFile = new DBFile();

        char tbl_path[PATH_MAX];
        char bin_path[PATH_MAX];

        sprintf(bin_path, "%s%s.bin", dbfile_dir, tnames[i]);
        sprintf(tbl_path, "%s%s.tbl", tpch_dir, tnames[i]);

        Schema schema(catalog_path, tnames[i]);

        dbFile->Create(bin_path, heap, NULL);

        dbFile->Load(schema, tbl_path);

        /*dbFile->Open(bin_path);
        Record record;
        int counter=0;
        dbFile->MoveFirst();
        while (dbFile->GetNext (record) == 1) {
            if(counter==0){
                cout<< "first record:" <<endl;
            record.Print(&schema);
            }
            counter ++;
        }
        cout <<"records: "<<counter<<endl;*/


        dbFile->Close();

        cout << "loaded " << tnames[i] << " table";
    }
}