using namespace std;
#include "extraFunction.h"

void MyFucntion :: PrintNameList(NameList *nameList)
{
    while (nameList)
    {
        cout << nameList->name << endl;
        nameList = nameList->next;
    }
}

void MyFucntion :: CopyAttrList(AttrList *attrList, vector<Attribute> &atts)
{
    while (attrList)
    {
        Attribute att;
        att.name = attrList->name;
        switch (attrList->type)
        {
        case 0:
        {   att.myType = Int;}
        break;
        case 1:
        { att.myType = Double;}
        break;
        case 2:
        {att.myType = String;}
        break;
        default:
        {}
        }
        atts.push_back(att);
        attrList = attrList->next;
    }
}