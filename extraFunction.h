#ifndef EXTRAFUNC_H
#define EXTRAFUNC_H

#include <iostream>
#include <string>
#include <vector>
#include <stdio.h>
#include "ParseTree.h"
#include "DBFile.h"
#include "Comparison.h"
#include "Schema.h"
#include "Function.h"
#include "Pipe.h"
#include "RelOp.h"

class MyFucntion{
public:
void PrintNameList (NameList *nameList);
void CopyAttrList(AttrList *attrList, vector<Attribute> &atts);
// void cleanup ();
};
#endif