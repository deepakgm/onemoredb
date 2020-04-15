#include "Statistics.h"
#include <iostream>
#include <map>
#include <set>
#include <stdlib.h>
#include <fstream>
#include <math.h>
#include <string.h>


Statistics::Statistics() {
    relationMap = new map<string, int>();
    attrMap = new map<string, map<string, int> >();
}


Statistics::Statistics(Statistics &copyMe) {
    relationMap = new map<string, int>(*(copyMe.relationMap));
    attrMap = new map<string, map<string, int> >(*(copyMe.attrMap));
}


Statistics::~Statistics() {
    delete relationMap;
    delete attrMap;
}



void Statistics::initStatistics() {

    relationMap->clear();
    attrMap->clear();

    const char *supplier = "supplier";
    const char *partsupp = "partsupp";
    const char *part = "part";
    const char *nation = "nation";
    const char *customer = "customer";
    const char *orders = "orders";
    const char *region = "region";
    const char *lineitem = "lineitem";

    AddRel((char *)(char *)region,5);
    AddRel((char *)nation,25);
    AddRel((char *)part,200000);
    AddRel((char *)supplier,10000);
    AddRel((char *)partsupp,800000);
    AddRel((char *)customer,150000);
    AddRel((char *)orders,1500000);
    AddRel((char *)lineitem,6001215);

    // region
    AddAtt((char *)region, "r_regionkey",5); // r_regionkey=5
    AddAtt((char *)region, "r_name",5); // r_name=5
    AddAtt((char *)region, "r_comment",5); // r_comment=5
    // nation
    AddAtt((char *)nation, "n_nationkey",25); // n_nationkey=25
    AddAtt((char *)nation, "n_name",25);  // n_name=25
    AddAtt((char *)nation, "n_regionkey",5);  // n_regionkey=5
    AddAtt((char *)nation, "n_comment",25);  // n_comment=25
    // part
    AddAtt((char *)part, "p_partkey",200000); // p_partkey=200000
    AddAtt((char *)part, "p_name",200000); // p_name=199996
    AddAtt((char *)part, "p_mfgr",200000); // p_mfgr=5
    AddAtt((char *)part, "p_brand",200000); // p_brand=25
    AddAtt((char *)part, "p_type",200000); // p_type=150
    AddAtt((char *)part, "p_size",200000); // p_size=50
    AddAtt((char *)part, "p_container",200000); // p_container=40
    AddAtt((char *)part, "p_retailprice",200000); // p_retailprice=20899
    AddAtt((char *)part, "p_comment",200000); // p_comment=127459
    // supplier
    AddAtt((char *)supplier,"s_suppkey",10000);
    AddAtt((char *)supplier,"s_name",10000);
    AddAtt((char *)supplier,"s_address",10000);
    AddAtt((char *)supplier,"s_nationkey",25);
    AddAtt((char *)supplier,"s_phone",10000);
    AddAtt((char *)supplier,"s_acctbal",9955);
    AddAtt((char *)supplier,"s_comment",10000);
    // partsupp
    AddAtt((char *)partsupp,"ps_partkey",200000);
    AddAtt((char *)partsupp,"ps_suppkey",10000);
    AddAtt((char *)partsupp,"ps_availqty",9999);
    AddAtt((char *)partsupp,"ps_supplycost",99865);
    AddAtt((char *)partsupp,"ps_comment",799123);
    // customer
    AddAtt((char *)customer,"c_custkey",150000);
    AddAtt((char *)customer,"c_name",150000);
    AddAtt((char *)customer,"c_address",150000);
    AddAtt((char *)customer,"c_nationkey",25);
    AddAtt((char *)customer,"c_phone",150000);
    AddAtt((char *)customer,"c_acctbal",140187);
    AddAtt((char *)customer,"c_mktsegment",5);
    AddAtt((char *)customer,"c_comment",149965);
    // orders
    AddAtt((char *)orders,"o_orderkey",1500000);
    AddAtt((char *)orders,"o_custkey",99996);
    AddAtt((char *)orders,"o_orderstatus",3);
    AddAtt((char *)orders,"o_totalprice",1464556);
    AddAtt((char *)orders,"o_orderdate",2406);
    AddAtt((char *)orders,"o_orderpriority",5);
    AddAtt((char *)orders,"o_clerk",1000);
    AddAtt((char *)orders,"o_shippriority",1);
    AddAtt((char *)orders,"o_comment",1478684);
    // lineitem
    AddAtt((char *)lineitem,"l_orderkey",1500000);
    AddAtt((char *)lineitem,"l_partkey",200000);
    AddAtt((char *)lineitem,"l_suppkey",10000);
    AddAtt((char *)lineitem,"l_linenumber",7);
    AddAtt((char *)lineitem,"l_quantity",50);
    AddAtt((char *)lineitem,"l_extendedprice",933900);
    AddAtt((char *)lineitem,"l_discount",11);
    AddAtt((char *)lineitem,"l_tax",9);
    AddAtt((char *)lineitem,"l_returnflag",3);
    AddAtt((char *)lineitem,"l_linestatus",2);
    AddAtt((char *)lineitem,"l_shipdate",2526);
    AddAtt((char *)lineitem,"l_commitdate",2466);
    AddAtt((char *)lineitem,"l_receiptdate",2554);
    AddAtt((char *)lineitem,"l_shipinstruct",4);
    AddAtt((char *)lineitem,"l_shipmode",7);
    AddAtt((char *)lineitem,"l_comment",4501941);
}

void Statistics::AddRel(char *relName, int numTuples) {
    string relation(relName);

    auto result = relationMap->insert(pair<string, int>(relation, numTuples));

    //replace existing
    if (!result.second) {
        relationMap->erase(result.first);
        relationMap->insert(pair<string, int>(relation, numTuples));
    }
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts) {
    string relation(relName);
    string attr(attName);

    if (numDistincts == -1) {
        int numTuples = relationMap->at(relation);
        (*attrMap)[relation][attr] = numTuples;
    } else {
        (*attrMap)[relation][attr] = numDistincts;
    }
}

void Statistics::CopyRel(char *oldName1, char *newName1) {
    string oldName(oldName1);
    string newName(newName1);


    int oldNumTuples = (*relationMap)[oldName];
    (*relationMap)[newName] = oldNumTuples;

    map<string, int> &oldAttrMap = (*attrMap)[oldName];

    for (auto oldAttrInfo = oldAttrMap.begin();
         oldAttrInfo != oldAttrMap.end(); ++oldAttrInfo) {
        string newAtt = newName;
        newAtt += "." + oldAttrInfo->first;
        (*attrMap)[newName][newAtt] = oldAttrInfo->second;
    }
}

void Statistics::Read(char *fromWhere) {
    relationMap->clear();
    attrMap->clear();

    ifstream file_exists(fromWhere);
    if (!file_exists) {
        cerr << "The give file_path '" << fromWhere << "' doest not exist";
        return;
    }

    string fileName(fromWhere);
    ifstream statFile;
    statFile.open(fileName.c_str(), ios::in);

    string str;
    statFile >> str;
    int relationCount = atoi(str.c_str());


    for (int i = 0; i < relationCount; i++) {
        statFile >> str;

        size_t index = str.find_first_of(':');
        string relationName = str.substr(0, index);
        string numOfTupleStr = str.substr(index + 1);

        (*relationMap)[relationName] = atoi(numOfTupleStr.c_str());
    }

    statFile >> str;
    string relName, attrName, distinctCount;
    statFile >> relName >> attrName >> distinctCount;

    while (!statFile.eof()) {
        int distinctCountInt = atoi(distinctCount.c_str());
        (*attrMap)[relName][attrName] = distinctCountInt;
        statFile >> relName >> attrName >> distinctCount;
    }
    statFile.close();
}

void Statistics::Write(char *fromWhere) {
    string fileName(fromWhere);
    //delete existing file
    remove(fromWhere);

    ofstream statFile;
    statFile.open(fileName.c_str(), ios::out);

    statFile << relationMap->size() << "\n";

    for (auto entry = relationMap->begin(); entry != relationMap->end(); entry++)
        statFile << entry->first.c_str() << ":" << entry->second << "\n";

    statFile << attrMap->size() << "\n";

    for (auto relItr = attrMap->begin(); relItr != attrMap->end(); ++relItr)
        for (auto attrItr = relItr->second.begin(); attrItr != relItr->second.end(); ++attrItr)
            statFile << (*relItr).first.c_str() << " " << (*attrItr).first.c_str() << " " << (*attrItr).second << "\n";

    statFile.close();
}

void Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin) {
    struct AndList *curAnd;
    struct OrList *curOr;

    map<string, int> opratorMap;
    curAnd = parseTree;
    while (curAnd != NULL) {
        curOr = curAnd->left;
        while (curOr != NULL) {
            opratorMap[curOr->left->left->value] = curOr->left->code;
            curOr = curOr->rightOr;
        }
        curAnd = curAnd->rightAnd;
    }

    double result = Estimate(parseTree, relNames, numToJoin);

    map<string, int>::iterator opMapItr, countMapItr;
    set<string> joinAttrSet;
    if (joinFlag2) {
        for (opMapItr = opratorMap.begin(); opMapItr != opratorMap.end(); opMapItr++) {
            for (int i = 0; i < relationMap->size(); i++) {
                if (relNames[i] == NULL)
                    continue;
                int count = ((*attrMap)[relNames[i]]).count(opMapItr->first);
                if (count == 0)
                    continue;
                else if (count == 1) {
                    for (countMapItr = (*attrMap)[relNames[i]].begin();
                         countMapItr != (*attrMap)[relNames[i]].end(); countMapItr++) {
                        if ((opMapItr->second == LESS_THAN) || (opMapItr->second == GREATER_THAN)) {
                            (*attrMap)[joinLeftRel + "_" +
                                       joinRightRel][countMapItr->first] = (int) round(
                                    (double) (countMapItr->second) / 3.0);
                        } else if (opMapItr->second == EQUALS) {
                            if (opMapItr->first == countMapItr->first)
                                (*attrMap)[joinLeftRel + "_" + joinRightRel][countMapItr->first] = 1;
                            else
                                (*attrMap)[joinLeftRel + "_" + joinRightRel][countMapItr->first] = min(
                                        (int) round(result), countMapItr->second);
                        }
                    }
                    break;
                } else if (count > 1) {
                    for (countMapItr = (*attrMap)[relNames[i]].begin();
                         countMapItr != (*attrMap)[relNames[i]].end(); countMapItr++) {
                        if (opMapItr->second == EQUALS) {
                            if (opMapItr->first == countMapItr->first) {
                                (*attrMap)[joinLeftRel + "_" +
                                           joinRightRel][countMapItr->first] = count;
                            } else
                                (*attrMap)[joinLeftRel + "_" +
                                           joinRightRel][countMapItr->first] = min(
                                        (int) round(result), countMapItr->second);
                        }
                    }
                    break;
                }
                joinAttrSet.insert(relNames[i]);
            }
        }

        if (joinAttrSet.count(joinLeftRel) == 0) {
            for (auto entry = (*attrMap)[joinLeftRel].begin();
                 entry != (*attrMap)[joinLeftRel].end(); entry++) {
                (*attrMap)[joinLeftRel + "_" + joinRightRel][entry->first] = entry->second;

            }
        }
        if (joinAttrSet.count(joinRightRel) == 0) {
            for (auto entry = (*attrMap)[joinRightRel].begin();
                 entry != (*attrMap)[joinRightRel].end(); entry++) {
                (*attrMap)[joinLeftRel + "_" + joinRightRel][entry->first] = entry->second;

            }
        }
        (*relationMap)[joinLeftRel + "_" + joinRightRel] = round(result);

        relationMap->erase(joinLeftRel);
        relationMap->erase(joinRightRel);
        attrMap->erase(joinLeftRel);
        attrMap->erase(joinRightRel);

    } else {
        // find the rel for which the attr belongs to
        string relName;
        for (auto entry = opratorMap.begin(); entry != opratorMap.end(); entry++) {
            for (auto mapEntry = attrMap->begin(); mapEntry != attrMap->end(); ++mapEntry)
                if ((*attrMap)[mapEntry->first].count(entry->first) > 0) {
                    relName = mapEntry->first;
                    break;
                }
        }
        (*relationMap)[relName]=round(result);
    }
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin) {

    struct AndList *curAnd;
    struct OrList *curOr;
    string leftRel, rightRel, leftAttr, rightAttr, prevAttr;
    bool isPrevious = false;
    bool isAnotherPrevious = false;
    double result = 0.0;
    double andResult = 1.0;
    double orResult = 1.0;

    curAnd = parseTree;

    while (curAnd != NULL) {
        curOr = curAnd->left;
        orResult = 1.0;

        while (curOr != NULL) {
            ComparisonOp *compOp = curOr->left;
            joinFlag1 = false;
            leftAttr = compOp->left->value;

            if (leftAttr == prevAttr)
                isPrevious = true;
            prevAttr = leftAttr;

            for (auto iterator = attrMap->begin();
                 iterator != attrMap->end(); iterator++) {
                if ((*attrMap)[iterator->first].count(leftAttr) > 0) {
                    leftRel = iterator->first;
                    break;
                }
            }


            if (compOp->right->code == NAME) {
                joinFlag1 = true;
                joinFlag2 = true;
                rightAttr = compOp->right->value;

                for (auto iterator = attrMap->begin();
                     iterator != attrMap->end(); ++iterator) {
                    if ((*attrMap)[iterator->first].count(rightAttr) > 0) {
                        rightRel = iterator->first;
                        break;
                    }
                }
            }

            if (joinFlag1) {
                if (compOp->code == EQUALS)
                    orResult *= (1.0 - (1.0 / max((*attrMap)[leftRel][compOp->left->value],
                                                  (*attrMap)[rightRel][compOp->right->value])));
                joinLeftRel = leftRel;
                joinRightRel = rightRel;
            } else {
                if (isPrevious) {
                    if (!isAnotherPrevious) {
                        orResult = 1.0 - orResult;
                        isAnotherPrevious = true;
                    }
                    if (compOp->code == GREATER_THAN || compOp->code == LESS_THAN)
                        orResult += (1.0 / 3.0);

                    if (compOp->code == EQUALS)
                        orResult += (1.0 / ((*attrMap)[leftRel][compOp->left->value]));

                } else {
                    if (compOp->code == GREATER_THAN || compOp->code == LESS_THAN)
                        orResult *= (2.0 / 3.0);

                    if (compOp->code == EQUALS)
                        orResult *= (1.0 - (1.0 / (*attrMap)[leftRel][compOp->left->value]));
                }
            }
            curOr = curOr->rightOr;
        }

        if (!isPrevious)
            orResult = 1.0 - orResult;

        isPrevious = false;
        isAnotherPrevious = false;

        andResult *= orResult;
        curAnd = curAnd->rightAnd;
    }

    double rightCount;
    if (rightRel.empty())
        rightCount = 0;
    else
        rightCount = (*relationMap)[rightRel];


    if (joinFlag2)
        result = (*relationMap)[joinLeftRel] * rightCount * andResult;
    else
        result = (*relationMap)[leftRel] * andResult;

    return result;
}