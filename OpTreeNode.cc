#include "OpTreeNode.h"

using namespace std;

SelectFileNode :: SelectFileNode(AndList *selectList, Schema *schema, string relName) {
    this->myType = SELECTFILE;
    this->outputSchema = schema;

    schema->Print();
    cout << "before cnf" <<endl;
    cnf.GrowFromParseTree(selectList,schema,literal);
    cout << "after cnf" <<endl;

}

void SelectFileNode :: run() {
    dbfile.Open(dbfilePath.c_str());
    sf.Run(dbfile, outpipe, cnf, literal);
}

void SelectFileNode :: print() {
    cout << endl << "Operation: Select File"<< endl;
    cout<<"Output pipe: " << this->getPipeID()<<endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print();
}

SelectPipeNode :: SelectPipeNode(OpTreeNode *child, AndList *selectList) {
    this->myType = SELECTPIPE;
    this->left = child;
    this->outputSchema = child->getSchema();
    cnf.GrowFromParseTree(selectList, outputSchema, literal);
};

void SelectPipeNode :: run() {
    sp.Run(left->outpipe, outpipe, cnf, literal);
}

void SelectPipeNode :: print() {
    cout << endl << "Operation: Select Pipe" << endl;
    cout << "Input Pipe " << this->left->getPipeID() <<endl;
    cout<< "Output Pipe " << this->getPipeID()<<endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print();
};

ProjectNode :: ProjectNode(OpTreeNode *child, NameList* attrsLeft) {
    this->myType = PROJECT;
    this->left = child;
    this->attsLeft = attrsLeft;
    this->outputSchema = child->getSchema()->Project(attsLeft, keepMe);
};

void ProjectNode :: run() {
    p.Run(left->outpipe, outpipe, keepMe, this->left->getSchema()->GetNumAtts(), this->outputSchema->GetNumAtts());
}

void ProjectNode :: print() {
    cout << endl << "Operation: Project"  << endl;
    cout <<"Input Pipe " << this->left->getPipeID() <<endl;
    cout<<"Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Attributes to Keep:" << endl;
    NameList* tmpList = attsLeft;
    while (tmpList)
    {
        cout << tmpList->name << " ";
        tmpList = tmpList->next;
    }
    cout << endl;
};

JoinNode :: JoinNode(OpTreeNode *leftChild, OpTreeNode *rightChild, AndList *joinList) {
    this->myType = JOIN;
    this->left = leftChild;
    this->right = rightChild;
    joinSchema();
    cnf.GrowFromParseTree(joinList,leftChild->getSchema(),rightChild->getSchema(),literal);
};

void JoinNode :: run() {
    j.Run(left->outpipe, right->outpipe, outpipe, cnf, literal);
}

void JoinNode :: joinSchema()
{
    int resNumAttrs = left->getSchema()->GetNumAtts() + right->getSchema()->GetNumAtts();
    Attribute *resAtts = new Attribute[resNumAttrs];

    for (int i = 0; i < left->getSchema()->GetNumAtts(); i++) {
        resAtts[i].name = left->getSchema()->GetAtts()[i].name;
        resAtts[i].myType = left->getSchema()->GetAtts()[i].myType;
    }

    for (int j = 0; j < right->getSchema()->GetNumAtts(); j++) {
        resAtts[j + left->getSchema()->GetNumAtts()].name = right->getSchema()->GetAtts()[j].name;
        resAtts[j + left->getSchema()->GetNumAtts()].myType = right->getSchema()->GetAtts()[j].myType;
    }


    outputSchema = new Schema("joined", resNumAttrs, resAtts);
};

void JoinNode :: print() {
    cout << endl << "Operation: Join"<<endl;
    cout << "Input Pipe " << this->left->getPipeID() <<endl;
    cout<< "Input Pipe " << this->right->getPipeID() <<endl;
    cout<< "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Outpt Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Join CNF:" << endl;
    this->cnf.Print();
};

DuplicateRemovalNode :: DuplicateRemovalNode(OpTreeNode *child) {
    this->myType = DUPLICATEREMOVAL;
    this->left = child;
    this->outputSchema = child->getSchema();
};

void DuplicateRemovalNode :: run() {
    dr.Run(left->outpipe, outpipe, *outputSchema);
}

void DuplicateRemovalNode :: print() {
    cout << endl << "Operation: DuplicateRemoval"<<endl;
    cout<<"Input Pipe " << this->left->getPipeID() << " Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
};

SumNode :: SumNode(OpTreeNode *child, FuncOperator *func) {
    this->myType = SUM;
    this->left = child;
    this->func = func;
    this->computeMe.GrowFromParseTree(func, *child->getSchema());
    sumSchema();
};

void SumNode :: run() {
    s.Run(left->outpipe, outpipe, computeMe);
}

void SumNode :: print() {
    cout << endl << "Operation: Sum" <<endl;
    cout <<"Input Pipe " << this->left->getPipeID() <<endl;
    cout<<"Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Function:" << endl;
    cout << joinFunc(this->func) << endl;
};

void SumNode :: sumSchema() {
    Attribute *atts = new Attribute[1];
    atts[0].name = "SUM";
    if (computeMe.returnsInt == 0) {
        atts[0].myType = Double;
    }
    else {
        atts[0].myType = Int;
    }
    outputSchema = new Schema("SUM", 1, atts);
}

string SumNode :: joinFunc(FuncOperator *func) {
    string resStr;
    if (func) {
        if (func->leftOperator) {
            resStr.append(joinFunc(func->leftOperator));
        }
        if (func->leftOperand) {
            resStr.append(func->leftOperand->value);
        }
        switch (func->code) {
            case 42:
                resStr.append("*");
                break;
            case 43:
                resStr.append("+");
                break;
            case 44:
                resStr.append("/");
                break;
            case 45:
                resStr.append("-");
                break;
        }
        if (func->right) {
            resStr.append(joinFunc(func->right));
        }
    }
    return resStr;
};

GroupByNode :: GroupByNode(OpTreeNode *child, NameList *groupingAtts, FuncOperator *func) {
    this->myType = GROUPBY;
    this->left = child;
    this->func = func;
    this->computeMe.GrowFromParseTree(func, *child->getSchema());
    getOrder(groupingAtts);
    groupBySchema();
};

void GroupByNode :: run() {
    gb.Run(left->outpipe, outpipe, groupOrder, computeMe);
}

void GroupByNode :: groupBySchema() {

    Attribute* atts = new Attribute[groupOrder.numAtts + 1];
    atts[0].name = "SUM";
    stringstream output;
    if (computeMe.returnsInt == 0) {
        atts[0].myType = Double;
    }
    else {
        atts[0].myType = Int;
    }
    Attribute* childAtts = left->getSchema()->GetAtts();
    for (int i = 0; i < groupOrder.numAtts; ++i) {
        atts[i + 1].name = childAtts[groupOrder.whichAtts[i]].name;
        atts[i + 1].myType = childAtts[groupOrder.whichAtts[i]].myType;
    }

    outputSchema = new Schema("out_shema", groupOrder.numAtts + 1, atts);
}

void GroupByNode :: getOrder(NameList* groupingAtts) {
    Schema* inputSchema = left->getSchema();
    while (groupingAtts) {
        groupOrder.whichAtts[groupOrder.numAtts] = inputSchema->Find(groupingAtts->name);
        groupOrder.whichTypes[groupOrder.numAtts] = inputSchema->FindType(groupingAtts->name);
        ++groupOrder.numAtts;
        groupingAtts = groupingAtts->next;
    }

}

void GroupByNode :: print() {
    cout << endl << "Operation: GroupBy"<<endl;
    cout <<"Input Pipe " << this->left->getPipeID() <<endl;
    cout <<"Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "OrderMaker:" << endl;
    groupOrder.Print();
    cout << endl << "Function:" << endl;
    cout << joinFunc(this->func) << endl;
};

string GroupByNode :: joinFunc(FuncOperator *func) {
    string resStr;
    if (func) {
        if (func->leftOperator) {
            resStr.append(joinFunc(func->leftOperator));
        }
        if (func->leftOperand) {
            resStr.append(func->leftOperand->value);
        }
        switch (func->code) {
            case 42:
                resStr.append(" * ");
                break;
            case 43:
                resStr.append(" + ");
                break;
            case 44:
                resStr.append(" / ");
                break;
            case 45:
                resStr.append(" - ");
                break;
        }
        if (func->right) {
            resStr.append(joinFunc(func->right));
        }
    }
    return resStr;
};