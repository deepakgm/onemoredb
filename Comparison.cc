#include <stdio.h>
#include <iostream>
#include <stdlib.h>
#include <string.h>
#include "Comparison.h"


Comparison::Comparison() {
}


Comparison::Comparison(const Comparison &copy_me) {
    operand1 = copy_me.operand1;
    whichAtt1 = copy_me.whichAtt1;
    operand2 = copy_me.operand2;
    whichAtt2 = copy_me.whichAtt2;

    attType = copy_me.attType;

    op = copy_me.op;
}

void Comparison::Print(Schema* leftSchema,Schema* rightSchema, Record *literal) {

//    cout << "(Att-" << whichAtt1 << ") ";
//    cout << "" << leftSchema->GetAtts()[whichAtt1].name << " from ";
//
//    if (operand1 == Left)
//        cout << "left record ";
//    else if (operand1 == Right)
//        cout << "right record ";
//    else {
//        literal->PrintLiteral(leftSchema, whichAtt1);
//        cout << "literal record ";
//    }

    if (operand1 == Left || operand1 == Right) {
        cout << "(Att-" << whichAtt1 << ") ";
        if (attType == Int)
            cout << "(Int)";
        else if (attType == Double)
            cout << "(Double)";
        else
            cout << "(String)";
        if (operand1 == Left) {
            cout << "" << leftSchema->GetAtts()[whichAtt1].name << " from ";
            cout << "left record ";
        } else {
            cout << "" << rightSchema->GetAtts()[whichAtt1].name << " from ";
            cout << "right record ";
        }
    } else {
        cout << "(literal) ";
        literal->PrintLiteral(rightSchema, whichAtt1);
        cout << " ";
    }

    if (op == LessThan)
        cout << "< ";
    else if (op == GreaterThan)
        cout << "> ";
    else
        cout << "= ";

    if (operand2 == Left || operand2 == Right) {
        cout << "(Att-" << whichAtt2 << ") ";
        if (attType == Int)
            cout << "(Int)";
        else if (attType == Double)
            cout << "(Double)";
        else
            cout << "(String)";
        if (operand2 == Left) {
            cout << "" << leftSchema->GetAtts()[whichAtt2].name << " from ";
            cout << "left record ";
        } else {
            cout << "" << rightSchema->GetAtts()[whichAtt2].name << " from ";
            cout << "right record ";
        }
    } else {
        cout << "(literal) ";
        literal->PrintLiteral(rightSchema, whichAtt2);
        cout << " ";
    }
}

void Comparison::Print(Schema *schema, Record *literal) {

    cout << "(Att-" << whichAtt1 << ") ";
    cout << "" << schema->GetAtts()[whichAtt1].name;
    cout << " from ";

    if (operand1 == Left)
        cout << "left record ";
    else if (operand1 == Right)
        cout << "right record ";
    else {
        literal->PrintLiteral(schema, whichAtt1);
        cout << "literal record ";
    }

    if (op == LessThan)
        cout << "< ";
    else if (op == GreaterThan)
        cout << "> ";
    else
        cout << "= ";

    if (operand2 == Left || operand2 == Right) {
        cout << "(Att-" << whichAtt2 << ") ";
        if (attType == Int)
            cout << "(Int)";
        else if (attType == Double)
            cout << "(Double)";
        else
            cout << "(String)";
        cout << "" << schema->GetAtts()[whichAtt2].name;
        cout << " from ";
        if (operand2 == Left) {
            cout << "left record ";

        } else {
            cout << "right record ";
        }
    } else {
        cout << "(literal) ";
        literal->PrintLiteral(schema, whichAtt1);
        cout << " ";
    }
}

void Comparison::Print() {

    cout << "Att " << whichAtt1 << " from ";

    if (operand1 == Left)
        cout << "left record ";
    else if (operand1 == Right)
        cout << "right record ";
    else
        cout << "literal record ";


    if (op == LessThan)
        cout << "< ";
    else if (op == GreaterThan)
        cout << "> ";
    else
        cout << "= ";


    cout << "Att " << whichAtt2 << " from ";

    if (operand2 == Left)
        cout << "left record ";
    else if (operand2 == Right)
        cout << "right record ";
    else
        cout << "literal record ";


    if (attType == Int)
        cout << "(Int)";
    else if (attType == Double)
        cout << "(Double)";
    else
        cout << "(String)";
}


OrderMaker::OrderMaker() {
    numAtts = 0;
}

OrderMaker::OrderMaker(Schema *schema) {
    numAtts = 0;

    int n = schema->GetNumAtts();
    Attribute *atts = schema->GetAtts();

    for (int i = 0; i < n; i++) {
        if (atts[i].myType == Int) {
            whichAtts[numAtts] = i;
            whichTypes[numAtts] = Int;
            numAtts++;
        }
    }

    // now add in the doubles
    for (int i = 0; i < n; i++) {
        if (atts[i].myType == Double) {
            whichAtts[numAtts] = i;
            whichTypes[numAtts] = Double;
            numAtts++;
        }
    }

    // and finally the strings
    for (int i = 0; i < n; i++) {
        if (atts[i].myType == String) {
            whichAtts[numAtts] = i;
            whichTypes[numAtts] = String;
            numAtts++;
        }
    }
}


void OrderMaker::Print() {
    printf("NumAtts = %5d\n", numAtts);
    for (int i = 0; i < numAtts; i++) {
        printf("%3d: %5d ", i, whichAtts[i]);
        if (whichTypes[i] == Int)
            printf("Int\n");
        else if (whichTypes[i] == Double)
            printf("Double\n");
        else
            printf("String\n");
    }
}

int OrderMaker :: Printgrp () {
    if (whichTypes[numAtts] == Int)
        return 1;
    else if (whichTypes[numAtts] == Double)
        return 2;
    else
        return 3;
}

void OrderMaker::growFromParseTree (NameList* gAtts, Schema* inputSchema) {

    for(; gAtts; gAtts = gAtts->next, numAtts++) {

        whichAtts[numAtts] = inputSchema->Find(gAtts->name);
        whichTypes[numAtts] = inputSchema->FindType(gAtts->name);

    }

}

int CNF::GetSortOrders(OrderMaker &left, OrderMaker &right) {

    // initialize the size of the OrderMakers
    left.numAtts = 0;
    right.numAtts = 0;

    // loop through all of the disjunctions in the CNF and find those
    // that are acceptable for use in a sort ordering
    for (int i = 0; i < numAnds; i++) {

        // if we don't have a disjunction of length one, then it
        // can't be acceptable for use with a sort ordering
        if (orLens[i] != 1) {
            continue;
        }

        // made it this far, so first verify that it is an equality check
        if (orList[i][0].op != Equals) {
            continue;
        }

        // now verify that it operates over atts from both tables
        if (!((orList[i][0].operand1 == Left && orList[i][0].operand2 == Right) ||
              (orList[i][0].operand2 == Left && orList[i][0].operand1 == Right))) {
            //continue;
        }

        // since we are here, we have found a join attribute!!!
        // so all we need to do is add the new comparison info into the
        // relevant structures
        if (orList[i][0].operand1 == Left) {
            left.whichAtts[left.numAtts] = orList[i][0].whichAtt1;
            left.whichTypes[left.numAtts] = orList[i][0].attType;
        }

        if (orList[i][0].operand1 == Right) {
            right.whichAtts[right.numAtts] = orList[i][0].whichAtt1;
            right.whichTypes[right.numAtts] = orList[i][0].attType;
        }

        if (orList[i][0].operand2 == Left) {
            left.whichAtts[left.numAtts] = orList[i][0].whichAtt2;
            left.whichTypes[left.numAtts] = orList[i][0].attType;
        }

        if (orList[i][0].operand2 == Right) {
            right.whichAtts[right.numAtts] = orList[i][0].whichAtt2;
            right.whichTypes[right.numAtts] = orList[i][0].attType;
        }

        // note that we have found two new attributes
        left.numAtts++;
        right.numAtts++;
    }

    return left.numAtts;
}


void CNF::PrintJoin(Schema* leftSchema,Schema* rightSchema,Record* literal) {
//    cout << "join details" <<endl;
//    cout << "left" <<endl;
//    leftSchema->Print();
//    cout << "right" <<endl;
//    rightSchema->Print();

//        if (numAnds>2){
//            cout <<"specia effect" <<endl;
//            orList[2]->Print(leftSchema,rightSchema,literal);
//        } else{
            orList[0]->Print(leftSchema,rightSchema,literal);
//        }
//    cout << numAnds <<endl;
//    cout << orLens[0] <<endl;
//    numAnds

//    string returnString = "";
//    if (comparisonOp == NULL)
//        return returnString;
//    if (comparisonOp->left->code != NAME)
//        return returnString;
//    string temp = comparisonOp->left->value;
//    string temp2 = temp.substr(0, temp.find('.'));
//    string left = temp;//.substr(temp.find('.')+1);
//
//    string right = "";
//    if (comparisonOp->right->code != NAME) {
//        right = comparisonOp->right->value;
//        if (comparisonOp->right->code == STRING) {
//            string temp = "'";
//            right += temp;
//            temp += right;
//            right = temp;
//        }
//    } else {
//        temp = "";
//        temp = comparisonOp->right->value;
//        temp2 = temp.substr(0, temp.find('.'));
//        right = temp;//.substr(temp.find('.')+1);
//    }
//    string operand = "";
//    if (comparisonOp->code == LESS_THAN)
//        operand = " < ";
//    else if (comparisonOp->code == GREATER_THAN)
//        operand = " > ";
//    else if (comparisonOp->code == EQUALS)
//        operand = " = ";
//    returnString = left + operand + right;
}

void CNF::Print(Schema *schema, Record *literal) {

    for (int i = 0; i < numAnds; i++) {

        cout << "( ";
        for (int j = 0; j < orLens[i]; j++) {
            orList[i][j].Print(schema, literal);
            if (j < orLens[i] - 1)
                cout << " OR ";
        }
        cout << ") ";
        if (i < numAnds - 1)
            cout << " AND\n";
        else
            cout << "\n";
    }
}

void CNF::Print() {

    for (int i = 0; i < numAnds; i++) {

        cout << "( ";
        for (int j = 0; j < orLens[i]; j++) {
            orList[i][j].Print();
            if (j < orLens[i] - 1)
                cout << " OR ";
        }
        cout << ") ";
        if (i < numAnds - 1)
            cout << " AND\n";
        else
            cout << "\n";
    }
}

// this is a helper routine that writes out another field for the literal record and its schema
void AddLitToFile(int &numFieldsInLiteral, FILE *outRecFile, FILE *outSchemaFile, char *value, Type myType) {

    // first write out the new record field
    fprintf(outRecFile, "%s|", value);

    // now write out the new schema field
    if (myType == Int) {
        fprintf(outSchemaFile, "att%d Int\n", numFieldsInLiteral);
    } else if (myType == Double) {
        fprintf(outSchemaFile, "att%d Double\n", numFieldsInLiteral);
    } else if (myType == String) {
        fprintf(outSchemaFile, "att%d String\n", numFieldsInLiteral);
    } else {
        cerr << "I don't know that type!!\n";
        exit(1);
    }

    // and note that we have another field
    numFieldsInLiteral++;
}



/*
void CNF :: GrowFromParseTree (struct AndList *parseTree, Schema *leftSchema,
	Schema *rightSchema, Record &literal) {

	CNF &cnf = *this;

	// as kind of a hack, the literal record is built up insiide of a text file,
	// where it will be read in from subsequently
	FILE *outRecFile = fopen ("sdafdsfFFDSDA", "w");

	// also as kind of a hack, the schema for the literal record is built up
	// inside of a text file, where it will also be read from subsequently
	FILE *outSchemaFile = fopen ("hkljdfgkSDFSDF", "w");
	fprintf (outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

	// this tells us the size of the literal record
	int numFieldsInLiteral = 0;

	// now we go through and build the comparison structure
	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
		
		// see if we have run off of the end of all of the ANDs
		if (parseTree == NULL) {
			cnf.numAnds = whichAnd;
			break;
		}	

		// we have not, so copy over all of the ORs hanging off of this AND
		struct OrList *myOr = parseTree->left;
		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

			// see if we have run off of the end of the ORs
			if (myOr == NULL) {
				cnf.orLens[whichAnd] = whichOr;
				break;
			}

			// we have not run off the list, so add the current OR in!
			
			// these store the types of the two values that are found
			Type typeLeft;
			Type typeRight;


			// first thing is to deal with the left operand
			// so we check to see if it is an attribute name, and if so,
			// we look it up in the schema
			if (myOr->left->left->code == NAME) {
				
				// see if we can find this attribute in the left schema
				if (leftSchema->Find (myOr->left->left->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand1 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt1 =
						leftSchema->Find (myOr->left->left->value);	
					typeLeft = leftSchema->FindType (myOr->left->left->value);

				// see if we can find it in the right schema
				} else if (rightSchema->Find (myOr->left->left->value) != -1) {
                                        cnf.orList[whichAnd][whichOr].operand1 = Right;
                                        cnf.orList[whichAnd][whichOr].whichAtt1 =
                                                rightSchema->Find (myOr->left->left->value);
                                        typeLeft = rightSchema->FindType (myOr->left->left->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " <<
						myOr->left->left->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->left->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
				typeLeft = String;

			// see if it is an integer
			} else if (myOr->left->left->code == INT) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
				typeLeft = Int;

			// see if it is a double
			} else if (myOr->left->left->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
				typeLeft = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}

			// now that we have dealt with the left operand, we need to deal with the
			// right operand
			if (myOr->left->right->code == NAME) {
				
				// see if we can find this attribute in the left schema
				if (leftSchema->Find (myOr->left->right->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand2 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt2 =
						leftSchema->Find (myOr->left->right->value);	
					typeRight = leftSchema->FindType (myOr->left->right->value);

				// see if we can find it in the right schema
				} else if (rightSchema->Find (myOr->left->right->value) != -1) {
                                        cnf.orList[whichAnd][whichOr].operand2 = Right;
                                        cnf.orList[whichAnd][whichOr].whichAtt2 =
                                                rightSchema->Find (myOr->left->right->value);
					typeRight = rightSchema->FindType (myOr->left->right->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->right->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
				typeRight = String;

			// see if it is an integer
			} else if (myOr->left->right->code == INT) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
				typeRight = Int;

			// see if it is a double
			} else if (myOr->left->right->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
				typeRight = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}

			

			// now we check to make sure that there was not a type mismatch
			if (typeLeft != typeRight) {
				cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
					<< myOr->left->right->value << " were found to not match.\n";
				exit (1);
			}

			// set up the type info for this comparison
			cnf.orList[whichAnd][whichOr].attType = typeLeft;

			// and finally set up the comparison operator for this comparison
			if (myOr->left->code == LESS_THAN) {
				cnf.orList[whichAnd][whichOr].op = LessThan;	
			} else if (myOr->left->code == GREATER_THAN) {
				cnf.orList[whichAnd][whichOr].op = GreaterThan;	
			} else if (myOr->left->code == EQUALS) {
				cnf.orList[whichAnd][whichOr].op = Equals;
			} else {
				cerr << "BAD: found a comparison op I don't recognize.\n";
				exit (1);
			}
		
		}
	}

	// the very last thing is to set up the literal record; first close the
	// file where its information has been stored
	fclose (outRecFile);
	fprintf (outSchemaFile, "END\n");
	fclose (outSchemaFile);

	// and open up the record file
	outRecFile = fopen ("sdafdsfFFDSDA", "r");

	// read in the record's schema
	Schema mySchema("hkljdfgkSDFSDF", "tempSchema");

	// and get the record
	literal.SuckNextRecord (&mySchema, outRecFile);

	// close the record file
	fclose (outRecFile);

	remove("sdafdsfFFDSDA");
	remove("hkljdfgkSDFSDF");
}
*/


/*
// this is the version that only deals with unary relational selection predicates
void CNF :: GrowFromParseTree (struct AndList *parseTree, Schema *mySchema, 
	Record &literal) {

	CNF &cnf = *this;

	// as kind of a hack, the literal record is built up insiide of a text file,
	// where it will be read in from subsequently
	FILE *outRecFile = fopen ("sdafdsfFFDSDA", "w");

	// also as kind of a hack, the schema for the literal record is built up
	// inside of a text file, where it will also be read from subsequently
	FILE *outSchemaFile = fopen ("hkljdfgkSDFSDF", "w");
	fprintf (outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

	// this tells us the size of the literal record
	int numFieldsInLiteral = 0;

	// now we go through and build the comparison structure
	for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {
		
		// see if we have run off of the end of all of the ANDs
		if (parseTree == NULL) {
			cnf.numAnds = whichAnd;
			break;
		}	

		// we have not, so copy over all of the ORs hanging off of this AND
		struct OrList *myOr = parseTree->left;
		for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

			// see if we have run off of the end of the ORs
			if (myOr == NULL) {
				cnf.orLens[whichAnd] = whichOr;
				break;
			}

			// we have not run off the list, so add the current OR in!
			
			// these store the types of the two values that are found
			Type typeLeft;
			Type typeRight;

			// first thing is to deal with the left operand
			// so we check to see if it is an attribute name, and if so,
			// we look it up in the schema
			if (myOr->left->left->code == NAME) {
				
				// see if we can find this attribute in the schema
				if (mySchema->Find (myOr->left->left->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand1 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt1 =
						mySchema->Find (myOr->left->left->value);	
					typeLeft = mySchema->FindType (myOr->left->left->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " <<
						myOr->left->left->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->left->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
				typeLeft = String;

			// see if it is an integer
			} else if (myOr->left->left->code == INT) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
				typeLeft = Int;

			// see if it is a double
			} else if (myOr->left->left->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand1 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
				typeLeft = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}

			// now that we have dealt with the left operand, we need to deal with the
			// right operand
			if (myOr->left->right->code == NAME) {
				
				// see if we can find this attribute in the left schema
				if (mySchema->Find (myOr->left->right->value) != -1) {
					cnf.orList[whichAnd][whichOr].operand2 = Left;
					cnf.orList[whichAnd][whichOr].whichAtt2 =
						mySchema->Find (myOr->left->right->value);	
					typeRight = mySchema->FindType (myOr->left->right->value);

				// it is not there!  So there is an error in the query
                                } else {
					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
					exit (1);	
				}

			// the next thing is to see if we have a string; if so, add it to the 
			// literal record that stores all of the comparison values
			} else if (myOr->left->right->code == STRING) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
				typeRight = String;

			// see if it is an integer
			} else if (myOr->left->right->code == INT) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
				typeRight = Int;

			// see if it is a double
			} else if (myOr->left->right->code == DOUBLE) {

				cnf.orList[whichAnd][whichOr].operand2 = Literal;
				cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
				AddLitToFile (numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
				typeRight = Double;
	
			// catch-all case
			} else {
				cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
				exit (1);
			}
			

			// now we check to make sure that there was not a type mismatch
			if (typeLeft != typeRight) {
				cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
					<< myOr->left->right->value << " were found to not match.\n";
				exit (1);
			}

			// set up the type info for this comparison
			cnf.orList[whichAnd][whichOr].attType = typeLeft;

			// and finally set up the comparison operator for this comparison
			if (myOr->left->code == LESS_THAN) {
				cnf.orList[whichAnd][whichOr].op = LessThan;	
			} else if (myOr->left->code == GREATER_THAN) {
				cnf.orList[whichAnd][whichOr].op = GreaterThan;	
			} else if (myOr->left->code == EQUALS) {
				cnf.orList[whichAnd][whichOr].op = Equals;
			} else {
				cerr << "BAD: found a comparison op I don't recognize.\n";
				exit (1);
			}
		
		}
	}

	// the very last thing is to set up the literal record; first close the
	// file where its information has been stored
	fclose (outRecFile);
	fprintf (outSchemaFile, "END\n");
	fclose (outSchemaFile);

	// and open up the record file
	outRecFile = fopen ("sdafdsfFFDSDA", "r");

	// read in the record's schema
	Schema outSchema("hkljdfgkSDFSDF", "tempSchema");

	// and get the record
	literal.SuckNextRecord (&outSchema, outRecFile);

	// close the record file
	fclose (outRecFile);

	remove("sdafdsfFFDSDA");
	remove("hkljdfgkSDFSDF");
}
*/

// this is the version that only deals with unary relational selection predicates
void CNF::GrowFromParseTree(struct AndList *parseTree, Schema *mySchema,
                            Record &literal) {

    CNF &cnf = *this;

    // as kind of a hack, the literal record is built up insiide of a text file,
    // where it will be read in from subsequently
    FILE *outRecFile = fopen("sdafdsfFFDSDA", "w");

    // also as kind of a hack, the schema for the literal record is built up
    // inside of a text file, where it will also be read from subsequently
    FILE *outSchemaFile = fopen("hkljdfgkSDFSDF", "w");
    fprintf(outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

    // this tells us the size of the literal record
    int numFieldsInLiteral = 0;

    // now we go through and build the comparison structure
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {

        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            cnf.numAnds = whichAnd;
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                cnf.orLens[whichAnd] = whichOr;
                break;
            }

            // we have not run off the list, so add the current OR in!

            // these store the types of the two values that are found
            Type typeLeft;
            Type typeRight;

            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {

                // see if we can find this attribute in the schema
                if (mySchema->Find(myOr->left->left->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand1 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt1 =
                            mySchema->Find(myOr->left->left->value);
                    typeLeft = mySchema->FindType(myOr->left->left->value);

                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " <<
//						myOr->left->left->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;
                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            } else if (myOr->left->left->code == STRING) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
                typeLeft = String;

                // see if it is an integer
            } else if (myOr->left->left->code == INT) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
                typeLeft = Int;

                // see if it is a double
            } else if (myOr->left->left->code == DOUBLE) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
                typeLeft = Double;

                // catch-all case
            } else {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                exit(1);
            }

            // now that we have dealt with the left operand, we need to deal with the
            // right operand
            if (myOr->left->right->code == NAME) {

                // see if we can find this attribute in the left schema
                if (mySchema->Find(myOr->left->right->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand2 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt2 =
                            mySchema->Find(myOr->left->right->value);
                    typeRight = mySchema->FindType(myOr->left->right->value);

                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;

                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            } else if (myOr->left->right->code == STRING) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
                typeRight = String;

                // see if it is an integer
            } else if (myOr->left->right->code == INT) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
                typeRight = Int;

                // see if it is a double
            } else if (myOr->left->right->code == DOUBLE) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
                typeRight = Double;

                // catch-all case
            } else {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                exit(1);
            }


            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
                     << myOr->left->right->value << " were found to not match.\n";
                exit(1);
            }

            // set up the type info for this comparison
            cnf.orList[whichAnd][whichOr].attType = typeLeft;

            // and finally set up the comparison operator for this comparison
            if (myOr->left->code == LESS_THAN) {
                cnf.orList[whichAnd][whichOr].op = LessThan;
            } else if (myOr->left->code == GREATER_THAN) {
                cnf.orList[whichAnd][whichOr].op = GreaterThan;
            } else if (myOr->left->code == EQUALS) {
                cnf.orList[whichAnd][whichOr].op = Equals;
            } else {
                cerr << "BAD: found a comparison op I don't recognize.\n";
                exit(1);
            }

        }
    }

    // the very last thing is to set up the literal record; first close the
    // file where its information has been stored
    fclose(outRecFile);
    fprintf(outSchemaFile, "END\n");
    fclose(outSchemaFile);

    // and open up the record file
    outRecFile = fopen("sdafdsfFFDSDA", "r");

    // read in the record's schema
    Schema outSchema("hkljdfgkSDFSDF", "tempSchema");

    // and get the record
    literal.SuckNextRecord(&outSchema, outRecFile);
//    cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++ "<<endl;
//    outSchema.PrintLiteral();
//    literal.PrintLiteral(&outSchema);
//    cout << "+++++++++++++++++++++++++++++++++++++++++++++++++++++++++++---- "<<endl;

    // close the record file
    fclose(outRecFile);

    remove("sdafdsfFFDSDA");
    remove("hkljdfgkSDFSDF");
}


void CNF::GrowFromParseTree(struct AndList *parseTree, Schema *leftSchema, Schema *rightSchema, Record &literal) {

    CNF &cnf = *this;

    // as kind of a hack, the literal record is built up insiide of a text file,
    // where it will be read in from subsequently
    FILE *outRecFile = fopen("sdafdsfFFDSDA", "w");

    // also as kind of a hack, the schema for the literal record is built up
    // inside of a text file, where it will also be read from subsequently
    FILE *outSchemaFile = fopen("hkljdfgkSDFSDF", "w");
    fprintf(outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

    // this tells us the size of the literal record
    int numFieldsInLiteral = 0;

    // now we go through and build the comparison structure
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {

        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            cnf.numAnds = whichAnd;
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                cnf.orLens[whichAnd] = whichOr;
                break;
            }

            // we have not run off the list, so add the current OR in!

            // these store the types of the two values that are found
            Type typeLeft;
            Type typeRight;


            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {

                // see if we can find this attribute in the left schema
                if (leftSchema->Find(myOr->left->left->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand1 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt1 =
                            leftSchema->Find(myOr->left->left->value);
                    typeLeft = leftSchema->FindType(myOr->left->left->value);

                    // see if we can find it in the right schema
                } else if (rightSchema->Find(myOr->left->left->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand1 = Right;
                    cnf.orList[whichAnd][whichOr].whichAtt1 =
                            rightSchema->Find(myOr->left->left->value);
                    typeLeft = rightSchema->FindType(myOr->left->left->value);

                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " <<
//						myOr->left->left->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;
                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            } else if (myOr->left->left->code == STRING) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, String);
                typeLeft = String;

                // see if it is an integer
            } else if (myOr->left->left->code == INT) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Int);
                typeLeft = Int;

                // see if it is a double
            } else if (myOr->left->left->code == DOUBLE) {

                cnf.orList[whichAnd][whichOr].operand1 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt1 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->left->value, Double);
                typeLeft = Double;

                // catch-all case
            } else {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                exit(1);
            }

            // now that we have dealt with the left operand, we need to deal with the
            // right operand
            if (myOr->left->right->code == NAME) {

                // see if we can find this attribute in the left schema
                if (leftSchema->Find(myOr->left->right->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand2 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt2 =
                            leftSchema->Find(myOr->left->right->value);
                    typeRight = leftSchema->FindType(myOr->left->right->value);

                    // see if we can find it in the right schema
                } else if (rightSchema->Find(myOr->left->right->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand2 = Right;
                    cnf.orList[whichAnd][whichOr].whichAtt2 =
                            rightSchema->Find(myOr->left->right->value);
                    typeRight = rightSchema->FindType(myOr->left->right->value);

                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;
                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            } else if (myOr->left->right->code == STRING) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, String);
                typeRight = String;

                // see if it is an integer
            } else if (myOr->left->right->code == INT) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Int);
                typeRight = Int;

                // see if it is a double
            } else if (myOr->left->right->code == DOUBLE) {

                cnf.orList[whichAnd][whichOr].operand2 = Literal;
                cnf.orList[whichAnd][whichOr].whichAtt2 = numFieldsInLiteral;
                AddLitToFile(numFieldsInLiteral, outRecFile, outSchemaFile, myOr->left->right->value, Double);
                typeRight = Double;

                // catch-all case
            } else {
                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
                exit(1);
            }



            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
                     << myOr->left->right->value << " were found to not match.\n";
                exit(1);
            }

            // set up the type info for this comparison
            cnf.orList[whichAnd][whichOr].attType = typeLeft;

            // and finally set up the comparison operator for this comparison
            if (myOr->left->code == LESS_THAN) {
                cnf.orList[whichAnd][whichOr].op = LessThan;
            } else if (myOr->left->code == GREATER_THAN) {
                cnf.orList[whichAnd][whichOr].op = GreaterThan;
            } else if (myOr->left->code == EQUALS) {
                cnf.orList[whichAnd][whichOr].op = Equals;
            } else {
                cerr << "BAD: found a comparison op I don't recognize.\n";
                exit(1);
            }

        }
    }

    // the very last thing is to set up the literal record; first close the
    // file where its information has been stored
    fclose(outRecFile);
    fprintf(outSchemaFile, "END\n");
    fclose(outSchemaFile);

    // and open up the record file
    outRecFile = fopen("sdafdsfFFDSDA", "r");

    // read in the record's schema
    Schema mySchema("hkljdfgkSDFSDF", "tempSchema");

    // and get the record
    literal.SuckNextRecord(&mySchema, outRecFile);

    // close the record file
    fclose(outRecFile);

    remove("sdafdsfFFDSDA");
    remove("hkljdfgkSDFSDF");
}



void CNF::GrowFromParseTreeJoin(struct AndList *parseTree, Schema *leftSchema, Schema *rightSchema, Record &literal) {
//    cout <<"grow for joins:" <<endl;
//    cout <<"left:" <<endl;
//    leftSchema->Print();
//    cout <<"right:" <<endl;
//    rightSchema->Print();
//    cout <<"ptree:" <<endl;

    bool leftDone= false;
    bool rightDone= false;

    CNF &cnf = *this;

    // as kind of a hack, the literal record is built up inside of a text file,
    // where it will be read in from subsequently
    FILE *outRecFile = fopen("sdafdsfFFDSDA", "w");

    // also as kind of a hack, the schema for the literal record is built up
    // inside of a text file, where it will also be read from subsequently
    FILE *outSchemaFile = fopen("hkljdfgkSDFSDF", "w");
    fprintf(outSchemaFile, "BEGIN\ntempSchema\nwherever\n");

    // this tells us the size of the literal record
    int numFieldsInLiteral = 0;

    // now we go through and build the comparison structure
    for (int whichAnd = 0; 1; whichAnd++, parseTree = parseTree->rightAnd) {

        // see if we have run off of the end of all of the ANDs
        if (parseTree == NULL) {
            cnf.numAnds = whichAnd;
            break;
        }

        // we have not, so copy over all of the ORs hanging off of this AND
        struct OrList *myOr = parseTree->left;
        for (int whichOr = 0; 1; whichOr++, myOr = myOr->rightOr) {

            // see if we have run off of the end of the ORs
            if (myOr == NULL) {
                cnf.orLens[whichAnd] = whichOr;
                break;
            }

            // we have not run off the list, so add the current OR in!

            // these store the types of the two values that are found
            Type typeLeft;
            Type typeRight;


            if(myOr->left->code!= EQUALS){
                cout <<"skipping condition\n";
                continue;
            }
            // first thing is to deal with the left operand
            // so we check to see if it is an attribute name, and if so,
            // we look it up in the schema
            if (myOr->left->left->code == NAME) {

                // see if we can find this attribute in the left schema
                if (!leftDone && leftSchema->Find(myOr->left->left->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand1 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt1 =
                            leftSchema->Find(myOr->left->left->value);
                    typeLeft = leftSchema->FindType(myOr->left->left->value);
                    leftDone=true;

                    // see if we can find it in the right schema
                } else if (!rightDone && rightSchema->Find(myOr->left->left->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand1 = Right;
                    cnf.orList[whichAnd][whichOr].whichAtt1 =
                            rightSchema->Find(myOr->left->left->value);
                    typeLeft = rightSchema->FindType(myOr->left->left->value);
                    rightDone=true;
                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " <<
//						myOr->left->left->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;
                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            }  else {
                cerr << "does not look like a join condition \n";
                continue;
            }

            // now that we have dealt with the left operand, we need to deal with the
            // right operand
            if (myOr->left->right->code == NAME) {

                // see if we can find this attribute in the left schema
                if (!leftDone &&leftSchema->Find(myOr->left->right->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand2 = Left;
                    cnf.orList[whichAnd][whichOr].whichAtt2 =
                            leftSchema->Find(myOr->left->right->value);
                    typeRight = leftSchema->FindType(myOr->left->right->value);
                    leftDone=true;
                    // see if we can find it in the right schema
                } else if (!rightDone && rightSchema->Find(myOr->left->right->value) != -1) {
                    cnf.orList[whichAnd][whichOr].operand2 = Right;
                    cnf.orList[whichAnd][whichOr].whichAtt2 =
                            rightSchema->Find(myOr->left->right->value);
                    typeRight = rightSchema->FindType(myOr->left->right->value);

                    rightDone=true;
                    // it is not there!  So there is an error in the query
                } else {
//					cout << "ERROR: Could not find attribute " << myOr->left->right->value << "\n";
//					exit (1);
                    whichAnd--;
                    continue;
                }

                // the next thing is to see if we have a string; if so, add it to the
                // literal record that stores all of the comparison values
            } else {
//                cerr << "You gave me some strange type for an operand that I do not recognize!!\n";
//                exit(1);
                cerr << "does not look like a join condition \n";
                continue;
            }



            // now we check to make sure that there was not a type mismatch
            if (typeLeft != typeRight) {
                cerr << "ERROR! Type mismatch in CNF.  " << myOr->left->left->value << " and "
                     << myOr->left->right->value << " were found to not match.\n";
//                exit(1);
                return;
            }

            // set up the type info for this comparison
            cnf.orList[whichAnd][whichOr].attType = typeLeft;

//            // and finally set up the comparison operator for this comparison
//            if (myOr->left->code == LESS_THAN) {
//                cnf.orList[whichAnd][whichOr].op = LessThan;
//            } else if (myOr->left->code == GREATER_THAN) {
//                cnf.orList[whichAnd][whichOr].op = GreaterThan;
//            } else
            if (myOr->left->code == EQUALS) {
                cnf.orList[whichAnd][whichOr].op = Equals;
            } else {
                cerr << "didn't expect this condition!\n";
//                exit(1);
                break;
            }
        }
        if(leftDone && rightDone)
            break;
        else{
            leftDone= false;
            rightDone= false;
        }
    }

    // the very last thing is to set up the literal record; first close the
    // file where its information has been stored
    fclose(outRecFile);
    fprintf(outSchemaFile, "END\n");
    fclose(outSchemaFile);

    // and open up the record file
    outRecFile = fopen("sdafdsfFFDSDA", "r");

    // read in the record's schema
    Schema mySchema("hkljdfgkSDFSDF", "tempSchema");

    // and get the record
    literal.SuckNextRecord(&mySchema, outRecFile);

    // close the record file
    fclose(outRecFile);

    remove("sdafdsfFFDSDA");
    remove("hkljdfgkSDFSDF");
}


