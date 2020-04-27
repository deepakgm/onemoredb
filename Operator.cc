#include "Operator.h"

using namespace std;

SelectFileOperator::SelectFileOperator(AndList *selectList, Schema *schema, string relName) {
    this->opType = SELECT_FILE;
    this->outputSchema = schema;
    this->dbfilePath = "test/" + relName + ".bin";
    cnf.GrowFromParseTree(selectList, schema, literal);
}

void SelectFileOperator::print() {
    cout << endl << "Operation: Select File" << endl;
    cout << "Output pipe: " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();

    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print(this->getSchema(), &literal);
//    this->cnf.Print();
}

void SelectFileOperator :: run() {
    cout<<"DBfile location: "<<dbfilePath;
    dbfile.Open(dbfilePath.c_str());
    sf.Run(dbfile, outPipe, cnf, literal);
    sf.WaitUntilDone();
    cout<<"After SelectFileRun"<<endl;
}

SelectPipeOperator::SelectPipeOperator(Operator *child, AndList *selectList) {
    this->opType = SELECT_PIPE;
    this->left = child;
    this->outputSchema = child->getSchema();
    cnf.GrowFromParseTree(selectList, outputSchema, literal);
};


void SelectPipeOperator::print() {
    cout << endl << "Operation: Select Pipe" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "SELECTION CNF:" << endl;
    this->cnf.Print(this->getSchema(), &literal);
};

ProjectOperator::ProjectOperator(Operator *child, NameList *attrsLeft) {
    this->opType = PROJECT;
    this->left = child;
    this->attsLeft = attrsLeft;
    this->outputSchema = child->getSchema()->Project(attsLeft, keepMe);
};

void SelectPipeOperator :: run() {
    sp.Run(left->outPipe, outPipe, cnf, literal);
}

void ProjectOperator::print() {
    cout << endl << "Operation: Project" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Attributes to Keep:" << endl;
    NameList *tmpList = attsLeft;
    while (tmpList) {
        cout << tmpList->name << " ";
        tmpList = tmpList->next;
    }
    cout << endl;
};

void ProjectOperator :: run() {
    p.Run(left->outPipe, outPipe, keepMe, this->left->getSchema()->GetNumAtts(), this->outputSchema->GetNumAtts());
    p.WaitUntilDone();
    cout<<endl<<"Outpipe: "<<endl;
}

JoinOperator::JoinOperator(Operator *leftChild, Operator *rightChild, AndList *joinList) {
    this->opType = JOIN;
    this->left = leftChild;
    this->right = rightChild;

    //create output schema
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

    outputSchema = new Schema("join", resNumAttrs, resAtts);

    cnf.GrowFromParseTree(joinList, leftChild->getSchema(), rightChild->getSchema(), literal);
//    *joinList = *joinList->rightAnd;
};

void JoinOperator::print() {
    cout << endl << "Operation: Join" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Input Pipe " << this->right->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();

    cout << endl << "Join CNF:" << endl;
//    this->cnf.PrintJoin(right->getSchema(),left->getSchema(),&literal);
//    this->cnf.PrintJoin(right->getSchema(),left->getSchema(),&literal);
    this->cnf.PrintJoin(left->getSchema(),right->getSchema(),&literal);
    //    this->cnf.Print(this->getSchema(), &literal);
};

void JoinOperator :: run() {
    j.Run(left->outPipe, right->outPipe, outPipe, cnf, literal);
}

DuplicateRemovalOperator::DuplicateRemovalOperator(Operator *child) {
    this->opType = DUPLICATE_REMOVAL;
    this->left = child;
    this->outputSchema = child->getSchema();
};

void DuplicateRemovalOperator::print() {
    cout << endl << "Operation: DuplicateRemoval" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
};

void DuplicateRemovalOperator :: run() {
    dr.Run(left->outPipe, outPipe, *outputSchema);
}

SumOperator::SumOperator(Operator *child, FuncOperator *func) {
    this->opType = SUM;
    this->left = child;
    this->funcOperator = func;
    this->function.GrowFromParseTree(func, *child->getSchema());

    //create output schema
    Attribute *atts = new Attribute[1];
    atts[0].name = "SUM";
    if (function.returnsInt == 0) {
        atts[0].myType = Double;
    } else {
        atts[0].myType = Int;
    }
    outputSchema = new Schema("SUM", 1, atts);

};

void SumOperator::print() {
    cout << endl << "Operation: Sum" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "Function:" << endl;
    cout << funcToString(this->funcOperator) << endl;
}

void SumOperator :: run() {
    s.Run(left->outPipe, outPipe, function);
}

GroupByOperator::GroupByOperator(Operator *child, OrderMaker orderMaker) {
    left = child;
    groupOrder = orderMaker;
}

GroupByOperator::GroupByOperator(Operator *child, NameList *groupingAtts, FuncOperator *func) {
    this->opType = GROUPBY;
    this->left = child;
    this->funcOperator = func;
    this->function.GrowFromParseTree(func, *child->getSchema());
    getOrder(groupingAtts);
    createOutputSchema();
}

void GroupByOperator::createOutputSchema() {
    Attribute *atts = new Attribute[groupOrder.numAtts + 1];
    atts[0].name = "SUM";
    stringstream output;
    if (function.returnsInt == 0) {
        atts[0].myType = Double;
    } else {
        atts[0].myType = Int;
    }
    Attribute *childAtts = left->getSchema()->GetAtts();
    for (int i = 0; i < groupOrder.numAtts; ++i) {
        atts[i + 1].name = childAtts[groupOrder.whichAtts[i]].name;
        atts[i + 1].myType = childAtts[groupOrder.whichAtts[i]].myType;
    }
    outputSchema = new Schema("group", groupOrder.numAtts + 1, atts);
}

void GroupByOperator::getOrder(NameList *groupingAtts) {
    Schema *inputSchema = left->getSchema();
    while (groupingAtts) {
        groupOrder.whichAtts[groupOrder.numAtts] = inputSchema->Find(groupingAtts->name);
        groupOrder.whichTypes[groupOrder.numAtts] = inputSchema->FindType(groupingAtts->name);
        ++groupOrder.numAtts;
        groupingAtts = groupingAtts->next;
    }
}


void GroupByOperator::print() {
    cout << endl << "Operation: GroupBy" << endl;
    cout << "Input Pipe " << this->left->getPipeID() << endl;
    cout << "Output Pipe " << this->getPipeID() << endl;
    cout << endl << "Output Schema:" << endl;
    this->outputSchema->Print();
    cout << endl << "OrderMaker:" << endl;
    groupOrder.Print();
    cout << endl << "Function:" << endl;
    cout << funcToString(this->funcOperator) << endl;
};

void GroupByOperator :: run() {
    gb.Run(left->outPipe, outPipe, groupOrder, function);
}