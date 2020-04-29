using namespace std;
#include "extraFunction.h"
#include <fstream>
#include <iostream>
#include <string>
#include <cstring>
#include "Schema.h"
#include <vector>
#include <iterator>

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

// typedef map<string, string> AliaseMap;

void MyFucntion ::UpdateTableInfo(char *tableName)
{
    string schString = "", line = "";
    ifstream fin(DBInfo.c_str());
    char *tempfile = ".info.tmp";
    // ofstream fout(tempfile);
    std::ofstream fout(tempfile);
    bool found = false;
    while (getline(fin, line))
    {
        // cout<<tableName<<endl;
        if (trim(line).empty())
            continue;
        line = trim(line);
        // cout<<line<<endl;
        if (strcmp(line.c_str(), tableName) == 0)
        {
            found = true;
        }
        if (!found){
            schString = trim(line) + '\n';
            cout<<schString;
            fout << schString;
        }
        found = false;
    }
    // cout << schString << endl;
    // fout << schString << endl;
    fin.close();
    fout.close();
    remove(DBInfo.c_str());
    rename(tempfile, DBInfo.c_str());
    cout<<"done updateTableInfo"<<endl;
}

int MyFucntion ::UpdateTable(char *tableName)
{
    string line;
    ifstream inFile;
    inFile.open(DBInfo.c_str());
    int count = 0;
    while (inFile >> line)
    {
        if (strcmp((char *)line.c_str(), tableName) == 0)
        {
            count++;
        }
    }
    if (count == 0)
    {
        ofstream myfile(DBInfo.c_str(), ios::out | ios::app);
        myfile << endl
               << tableName;
    }
    // cout << "Count: " << count << endl;
    return count;
}

map<string, Schema *> MyFucntion ::FireUpExistingDatabase()
{
    string line;

    ifstream inFile;

    inFile.open(DBInfo.c_str());
    if (!inFile)
    {
        cout << "Unable to open file";
        exit(1); // terminate with error
    }

    while (inFile >> line)
    {
        // cout<<"line length: "<<line.length()<<endl;
        // char file[line.length()];
        // std::copy(line.begin(), line.end(), file);
        // sprintf(file, "%s", line);
        // cout << "File input: " << line.c_str() << endl;
        loadSchema[(char *)line.c_str()] = new Schema("catalog", (char *)line.c_str());
        // cout<<line<<endl;
        // memset(file, 0, sizeof(file));
    }

    inFile.close();

    // cout << loadSchema["region"];
    // loadSchema["region"] = new Schema("catalog", "region");
    // loadSchema["part"] = new Schema("catalog", "part");
    // loadSchema["partsupp"] = new Schema("catalog", "partsupp");
    // loadSchema["nation"] = new Schema("catalog", "nation");
    // loadSchema["customer"] = new Schema("catalog", "customer");
    // loadSchema["supplier"] = new Schema("catalog", "supplier");
    // loadSchema["lineitem"] = new Schema("catalog", "lineitem");
    // loadSchema["orders"] = new Schema("catalog", "orders");
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
        printRecursively(root, 2);
    }
    else if (outputSet == 0)
    {
        printRecursively(root, 0);
        cout << endl;
        Record rec;
        while (root->outPipe.Remove(&rec))
        {
            rec.Print(root->getSchema());
        }
    }
    else
    {
        printRecursively(root, 1);
        string outputPath = db_dir + outputFile;
        cout<<"OutputPath: "<<outputPath<<endl;
        FILE *outFile = fopen(outputPath.c_str(), "w");
        wo.Run(root->outPipe, outFile, *root->getSchema());
        wo.WaitUntilDone();
        // cout<<"okay"<<endl;
        // fclose(outFile);
    }
}

void MyFucntion ::printRecursively(Operator *root, int outputSet)
{
    // cout << "root type: " << root->getType() << endl;
    if (!root)
        return;
    switch (root->getType())
    {
    case SELECT_FILE:
        // cout << "Select file" << endl;
        // cout<<"OutputSet: "<<outputSet<<endl;
        if (outputSet == 2){
            // cout<<"Q1"<<endl;
            ((SelectFileOperator *)root)->print();
            // cout<<"Q2"<<endl;
        }
        else
        {
            ((SelectFileOperator *)root)->run();
            // cout << "done" << endl;
        }
        break;
    case SELECT_PIPE:
        printRecursively(((SelectPipeOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((SelectPipeOperator *)root)->print();
        else
            ((SelectPipeOperator *)root)->run();
        break;
    case PROJECT:
        // cout << "Project" << endl;
        printRecursively(((ProjectOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((ProjectOperator *)root)->print();
        else
            ((ProjectOperator *)root)->run();
        break;
    case GROUPBY:
        printRecursively(((GroupByOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((GroupByOperator *)root)->print();
        else
            ((GroupByOperator *)root)->run();
        break;
    case SUM:
        printRecursively(((SumOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((SumOperator *)root)->print();
        else
            ((SumOperator *)root)->run();
        break;
    case DUPLICATE_REMOVAL:
        printRecursively(((DuplicateRemovalOperator *)root)->left, outputSet);
        if (outputSet == 2)
            ((DuplicateRemovalOperator *)root)->print();
        else
            ((DuplicateRemovalOperator *)root)->run();
        break;
    case JOIN:
        printRecursively(((JoinOperator *)root)->left, outputSet);
        printRecursively(((JoinOperator *)root)->right, outputSet);
        if (outputSet == 2)
            ((JoinOperator *)root)->print();
        else
            ((JoinOperator *)root)->run();
        break;
    default:
        cerr << "ERROR: Unspecified node!" << endl;
        exit(-1);
    }
    // cout << "out of loop" << endl;
    if (outputSet == 2)
        cout << endl
             << "*******************************************************" << endl;
    // Record rec;
    // while (root->outPipe.Remove(&rec))
    // {
    //     rec.Print(root->getSchema());
    // }
    return;
}