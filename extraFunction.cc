using namespace std;
#include "extraFunction.h"
#include <fstream>
#include <iostream>
#include <string>
#include <cstring>
#include "Schema.h"
#include <vector>

void MyFucntion ::PrintNameList(NameList *nameList)
{
    while (nameList)
    {
        cout << nameList->name << endl;
        nameList = nameList->next;
    }
}

void MyFucntion ::CopyAttrList(AttrList *attrList, vector<Attribute> &atts)
{
    while (attrList)
    {
        Attribute att;
        att.name = attrList->name;
        switch (attrList->type)
        {
        case 0:
        {
            att.myType = Int;
        }
        break;
        case 1:
        {
            att.myType = Double;
        }
        break;
        case 2:
        {
            att.myType = String;
        }
        break;
        default:
        {
        }
        }
        atts.push_back(att);
        attrList = attrList->next;
    }
}

void MyFucntion ::initSchemaMap(SchemaMap &map)
{
    ifstream ifs(catalog);
    char str[100];
    while (!ifs.eof())
    {
        ifs.getline(str, 100);
        if (strcmp(str, "BEGIN") == 0)
        {
            ifs.getline(str, 100);
            map[string(str)] = Schema(catalog, str);
        }
    }
    ifs.close();
}
// typedef map<string, string> AliaseMap;

void MyFucntion ::CopyTablesNamesAndAliases(TableList *tableList, Statistics &s, vector<char *> &tableNames, AliaseMap &map)
{
    while (tableList)
    {
        s.CopyRel(tableList->tableName, tableList->aliasAs);
        map[tableList->aliasAs] = tableList->tableName;
        tableNames.push_back(tableList->aliasAs);
        tableList = tableList->next;
    }
}

map<string, Schema *> MyFucntion ::FireUpExistingDatabase()
{
    // ifstream file(DBInfo);
    // file >> numofTables;
    // for (int i = 0; i < numofTables; ++i)
    // {
    //     string name = "";
    //     file >> name;
    //     int type = 0;
    //     file >> type;
    //     tablesInDB[name] = type;
    //     loadSchema[name] = new Schema(catalog, name);
    // }
    // file.close();
    // if (tablesInDB.count(tablename) == 0)
    // {
    //     cerr << "Error: Table hasn't been created yet!" << endl;
    //     return;
    // }
    // DBFile dbfile;
    // string filepath = "db/" + tablename + ".bin";
    // dbfile.Open(filepath.c_str());
    // string loadPath = tpch_dir + loadFileName;
    // dbfile.Load(*loadSchema[tablename], loadPath.c_str());
    // dbfile.Close();
    loadSchema["region"] = new Schema("catalog", "region");
    loadSchema["part"] = new Schema("catalog", "part");
    loadSchema["partsupp"] = new Schema("catalog", "partsupp");
    loadSchema["nation"] = new Schema("catalog", "nation");
    loadSchema["customer"] = new Schema("catalog", "customer");
    loadSchema["supplier"] = new Schema("catalog", "supplier");
    loadSchema["lineitem"] = new Schema("catalog", "lineitem");
    loadSchema["orders"] = new Schema("catalog", "orders");
    return loadSchema;
}

void MyFucntion ::copySchema(map<string, Schema *> &aliasSchemas, char *oldName, char *newName)
{

    Attribute *oldAtts = loadSchema[oldName]->GetAtts();
    int numAtts = loadSchema[oldName]->GetNumAtts();
    Attribute *newAtts = new Attribute[numAtts];
    size_t relLength = strlen(newName);

    for (int i = 0; i < numAtts; ++i)
    {
        size_t attLength = strlen(oldAtts[i].name);
        newAtts[i].name = new char[attLength + relLength + 2];
        strcpy(newAtts[i].name, newName);
        strcat(newAtts[i].name, ".");
        strcat(newAtts[i].name, oldAtts[i].name);
        newAtts[i].myType = oldAtts[i].myType;
    }
    aliasSchemas[newName] = new Schema(newName, numAtts, newAtts);
}

void MyFucntion ::shuffleOrderHelper(vector<string> &seenTable, int index, vector<vector<string>> &res, vector<string> &tmpres)
{
    if (index == seenTable.size())
    {
        res.push_back(tmpres);
        return;
    }
    for (int i = index; i < seenTable.size(); i++)
    {
        swap(seenTable[index], seenTable[i]);
        tmpres.push_back(seenTable[index]);
        shuffleOrderHelper(seenTable, index + 1, res, tmpres);
        tmpres.pop_back();
        swap(seenTable[index], seenTable[i]);
    }
}

vector<vector<string>> MyFucntion ::shuffleOrder(vector<string> &seenTable)
{
    vector<vector<string>> res;
    vector<string> tmpres;
    shuffleOrderHelper(seenTable, 0, res, tmpres);
    return res;
}

void MyFucntion ::WriteOutFunc(Operator *root, int outputSet, char *outputFile)
{
    if (outputSet == 2)
    {
        cout << endl
             << "Selected Query Plan:" << endl;
        cout << endl
             << "*******************************************************" << endl;
        traverse(root, 0);
    }
    else if (outputSet == 0)
    {
        traverse(root, 1);
        Record rec;
        while (root->outPipe.Remove(&rec))
        {
            rec.Print(root->getSchema());
        }
    }
    else
    {
        traverse(root, 1);
        string outputPath = db_dir + outputFile;
        FILE *outFile = fopen(outputPath.c_str(), "w");
        wo.Run(root->outPipe, outFile, *root->getSchema());
        wo.WaitUntilDone();
        fclose(outFile);
    }
}

void MyFucntion ::traverse(Operator *root, int outputSet)
{
    if (!root)
        return;
    switch (root->getType())
    {
    case SELECT_FILE:
        if (outputSet == 2)
            ((SelectFileOperator *)root)->print();
        else
            ((SelectFileOperator *)root)->run();
        break;
    case SELECT_PIPE:
        traverse(((SelectPipeOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((SelectPipeOperator *)root)->print();
        else
            ((SelectPipeOperator *)root)->run();
        break;
    case PROJECT:
        traverse(((ProjectOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((ProjectOperator *)root)->print();
        else
            ((ProjectOperator *)root)->run();
        break;
    case GROUPBY:
        traverse(((GroupByOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((GroupByOperator *)root)->print();
        else
            ((GroupByOperator *)root)->run();
        break;
    case SUM:
        traverse(((SumOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((SumOperator *)root)->print();
        else
            ((SumOperator *)root)->run();
        break;
    case DUPLICATE_REMOVAL:
        traverse(((DuplicateRemovalOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((DuplicateRemovalOperator *)root)->print();
        else
            ((DuplicateRemovalOperator *)root)->run();
        break;
    case JOIN:
        traverse(((JoinOperator *)root)->left, outputSet);
        traverse(((JoinOperator *)root)->right, outputSet);
        if (outputSet == 2)
            ((JoinOperator *)root)->print();
        else
            ((JoinOperator *)root)->run();
        break;
    default:
        cerr << "ERROR: Unspecified node!" << endl;
        exit(-1);
    }
    if (outputSet == 2)
        cout << endl
             << "*******************************************************" << endl;
}