#include "Statistics.h"

using namespace std;


Statistics::Statistics(){
}

Statistics::~Statistics(){
//todo
}
Statistics::Statistics(Statistics &copyMe){
    relMap = map<string, int>(copyMe.relMap);
    attrMap = map<string, pair<string, int>>(copyMe.attrMap);
}

void Statistics::AddRel(char *relName, int numTuples){
    relMap[string(relName)] = numTuples;
}

void Statistics::AddAtt(char *relName, char *attName, int numDistincts){
    string relName1(relName);
    string attName1(attName);

    if (numDistincts == -1) {
        attrMap[attName1].first = relName1;
        attrMap[attName1].second = relMap[relName1];
    }
    else {
        attrMap[attName1].first = relName1;
        attrMap[attName1].second = numDistincts;
    }
}

void Statistics::CopyRel(char *oldName, char *newName)
{
    string oldName1(oldName);
    string newName1(newName);

    relMap[newName1] = relMap[oldName1];
    map<string, pair<string, int>> tmp;

    for (auto iter = attrMap.begin(); iter != attrMap.end(); ++iter) {
        if (iter->second.first == oldName1) {
            string newAttrName = newName1 + "." + iter->first;
//            tmp[newAttrName].first = newName1;
//            tmp[newAttrName].second = iter->second.second;
            AddAtt(newName, (char *)newAttrName.c_str(), iter->second.second);
        }
    }
//    for (auto iter = tmp.begin(); iter != tmp.end(); ++iter) {
//        AddAtt(newName, (char *)iter->first.c_str(), iter->second.second);
//    }
}

void Statistics::Read( char *fromWhere)
{
    ifstream file(fromWhere);

    if (!file) {
        cerr << "Statistics file doesn't exist!" << endl;
        exit(-1);
    }

    relMap.clear();
    attrMap.clear();

    int attrNum;
    file >> attrNum;

    for (int i = 0; i < attrNum; i++) {
        string attr;
        file >> attr;

        string::size_type index1 = attr.find_first_of(":");
        string::size_type index2 = attr.find_last_of(":");
        string attrName = attr.substr(0, index1);
        string relName = attr.substr(index1 + 1, index2 - index1 - 1);
        int numDistincts = atoi(attr.substr(index2 + 1).c_str());
        attrMap[attrName].first = relName;
        attrMap[attrName].second = numDistincts;
    }

    int relNum;
    file >> relNum;

    for (int i = 0; i < relNum; i++) {
        string rel;
        file >> rel;

        string::size_type index = rel.find_first_of(":");
        string relName = rel.substr(0, index);
        int NumTuples = atoi(rel.substr(index + 1).c_str());
        relMap[relName] = NumTuples;
    }

    file.close();
}

void Statistics::Write(char *fromWhere)
{
    ofstream file(fromWhere);

    file << attrMap.size() << endl;

    for (auto iter1 = attrMap.begin(); iter1 != attrMap.end(); ++iter1) {
        file << iter1->first << ":" << iter1->second.first << ":" << iter1->second.second << endl;
    }

    file << relMap.size() << endl;

    for (auto iter = relMap.begin(); iter != relMap.end(); ++iter) {
        file << iter->first << ":" << iter->second << endl;
    }

    file.close();
}

void  Statistics::Apply(struct AndList *parseTree, char *relNames[], int numToJoin)
{
    // cout << "APPLY!!!" << endl;
    double andRes = 1.0;
    int64_t modifier = 1;
    vector<string> joinRels(2);
    string joinedRelName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        joinedRelName += relNames[i];
    }
    if (relMap.count(joinedRelName) > 0) {
        joinRels[0] = joinedRelName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        joinedRelName += relNames[numToJoin - 1];
        joinRels[0] = joinedRelName;
    }

    if (!parseTree) {
        return;
    }

    /*
     * Add code to check whether parseTree is legal here.
     */

    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;

        /*
         * Should figure out whether attrs in this orList appear more than onece.
         * If they are, the ratio of tuples left should be sum of each comparion's possibility.
         * If they aren't, the ratio of tuples left should be 1 - product of (1 - each possibility)
         */

        map<string, double> orPairList;
        bool isAlwaysTrue = false;
        double orRes = 1.0;

        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;

            if (curComp->code == EQUALS) {
                // Join. The ratio of tuples left should be 1 / max(leftDistinct, rightDistinct).
                if (left->code == NAME && right->code == NAME) {
                    // cout << "JOIN!!!" << endl;
                    string leftAttrName = left->value;
                    string rightAttrName = right->value;

                    string leftRel = attrMap[leftAttrName].first;
                    // cout << "leftRel: " << leftRel << endl;
                    string rightRel = attrMap[rightAttrName].first;
                    // cout << "rightRel: " << rightRel << endl;

                    if ((leftRel != joinRels[0] && leftRel != joinRels[1]) || (rightRel != joinRels[0] && rightRel != joinRels[1])) {
                        isAlwaysTrue = true;
                        break;
                    }

                    int leftDistinct = attrMap[leftAttrName].second;
                    // cout << "leftDistinct: " << leftDistinct << endl;
                    int rightDistinct = attrMap[rightAttrName].second;
                    // cout << "rightDistinct: " << rightDistinct << endl;

                    attrMap[leftAttrName].second = min(leftDistinct, rightDistinct);
                    attrMap[rightAttrName].second = min(leftDistinct, rightDistinct);

                    modifier = max(leftDistinct, rightDistinct);
                    orRes = 1.0 / modifier;
                    // cout << "orRes: " << orRes << endl;
                }
                    // Equal Selection. Use different formula according to situation mentioned above.
                else {
                    if (left->code == NAME || right->code == NAME) {
                        // cout << "Equal selection!!!" << endl;
                        string attrName = (left->code == NAME) ? left->value : right->value;
                        string relName = attrMap[attrName].first;
                        // cout << attrName << endl;
                        // cout << attrMap[attrName].first << endl;
                        if (relName != joinRels[0] && relName != joinRels[1]) {
                            isAlwaysTrue = true;
                            break;
                        }
                        int distinct = attrMap[attrName].second;

                        orPairList[attrName] += 1.0 / distinct;
                        if (orPairList[attrName] > 1.0) {
                            orPairList[attrName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        // cout << "Constant selection!!!" << endl;
                        isAlwaysTrue = true;
                        break;
                    }
                }
            }
                /*
                 * Less or greater than. Actually I don't know how to get the possibility of this operation.
                 * According to testcases we can assume possibility of equal, greater than and less than are
                 * equal, which is 1 / 3 each.
                 * Also need to figure out whether the same attrs or not then use different formula.
                 */
            else {
                if (left->code == NAME || right->code == NAME) {
                    // cout << "Greater or less selection!!!" << endl;
                    string attrName = (left->code == NAME) ? left->value : right->value;
                    string relName = attrMap[attrName].first;
                    // cout << attrName << endl;
                    // cout << attrMap[attrName].first << endl;
                    if (relName != joinRels[0] && relName != joinRels[1]) {
                        isAlwaysTrue = true;
                        break;
                    }
                    orPairList[attrName] += 1.0 / 3.0;
                    if (orPairList[attrName] > 1.0) {
                        orPairList[attrName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    // cout << "Constant selection!!!" << endl;
                    isAlwaysTrue = true;
                    break;
                }
            }

            curOr = curOr->rightOr;
        }

        if (!isAlwaysTrue && !orPairList.empty()) {
            // cout << "I am here!" << endl;
            double reveRes = 1;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orRes = 1 - reveRes;
        }
        andRes *= orRes;
        // cout << "andRes: " << andRes << endl;

        curAnd = curAnd->rightAnd;
    }

    double numofTuples = andRes;
    numofTuples *= relMap[joinRels[0]];
    relMap.erase(joinRels[0]);
    if (joinRels[1] != "") {
        numofTuples *= relMap[joinRels[1]];
        relMap.erase(joinRels[1]);
    }
    string newRelName = joinRels[0] + joinRels[1];
    relMap[newRelName] = numofTuples;
    andRes *= modifier;
    for (auto iter = attrMap.begin(); iter != attrMap.end(); ++iter) {
        if (iter->second.first == joinRels[0] || iter->second.first == joinRels[1]) {
            string attrName = iter->first;
            int numofDistinct = iter->second.second * andRes;
            attrMap[attrName].first = newRelName;
            attrMap[attrName].second = numofDistinct;
        }
    }
    // cout << "newRelName: " << newRelName << endl;
    // cout << "numofTuples: " << relMap[newRelName] << endl;
}

double Statistics::Estimate(struct AndList *parseTree, char **relNames, int numToJoin)
{
    double andRes = 1.0;
    vector<string> joinRels(2);
    string joinedRelName = "";
    for (int i = 0; i < numToJoin - 1; ++i) {
        joinedRelName += relNames[i];
    }
    if (relMap.count(joinedRelName) > 0) {
        joinRels[0] = joinedRelName;
        joinRels[1] = relNames[numToJoin - 1];
    }
    else {
        joinedRelName += relNames[numToJoin - 1];
        joinRels[0] = joinedRelName;
    }

    if (!parseTree) {
        return 0;
    }

    /*
     * Add code to check whether parseTree is legal here.
     */

    AndList *curAnd = parseTree;
    while (curAnd) {
        OrList *curOr = curAnd->left;
        double orRes = 1.0;

        /*
         * Should scan the orList first to figure out whether attrs in this orList are the same.
         * If they are, the ratio of tuples left should be sum of each comparion's possibility.
         * If they aren't, the ratio of tuples left should be 1 - product of (1 - each possibility)
         */

        map<string, double> orPairList;
        bool isAlwaysTrue = false;

        while (curOr) {
            ComparisonOp *curComp = curOr->left;
            Operand *left = curComp->left;
            Operand *right = curComp->right;

            if (curComp->code == EQUALS) {
                // Join. The ratio of tuples left should be 1 / max(leftDistinct, rightDistinct).
                if (left->code == NAME && right->code == NAME) {
                    // cout << "JOIN!!!" << endl;
                    string leftAttrName = left->value;
                    string rightAttrName = right->value;

                    string leftRel = attrMap[leftAttrName].first;
                    // cout << "leftRel: " << leftRel << endl;
                    string rightRel = attrMap[rightAttrName].first;
                    // cout << "rightRel: " << rightRel << endl;

                    if ((leftRel != joinRels[0] && leftRel != joinRels[1]) || (rightRel != joinRels[0] && rightRel != joinRels[1])) {
                        isAlwaysTrue = true;
                        break;
                    }

                    int leftDistinct = attrMap[leftAttrName].second;
                    // cout << "leftDistinct: " << leftDistinct << endl;
                    int rightDistinct = attrMap[rightAttrName].second;
                    // cout << "rightDistinct: " << rightDistinct << endl;

                    // cout << "leftNumofTuples: " << relMap[leftRel] << endl;
                    // cout << "rightNumofTuples: " << relMap[rightRel] << endl;

                    // cout << "result: " << result << endl;
                    orRes = 1.0 / max(leftDistinct, rightDistinct);
                    // cout << "result: " << result << endl;
                }
                    // Equal Selection. Use different formula according to situation mentioned above.
                else {
                    // cout << "EQUAL COMPARISON!!!" << endl;
                    if (left->code == NAME || right->code == NAME) {
                        string attrName = (left->code == NAME) ? left->value : right->value;
                        string relName = attrMap[attrName].first;
                        // cout << "relName: " << relName << endl;
                        if (relName != joinRels[0] && relName != joinRels[1]) {
                            isAlwaysTrue = true;
                            break;
                        }
                        int distinct = attrMap[attrName].second;

                        orPairList[attrName] += 1.0 / distinct;
                        if (orPairList[attrName] > 1.0) {
                            orPairList[attrName] = 1.0;
                        }
                    }
                    else if (left->code == right->code && left->value == right->value) {
                        isAlwaysTrue = true;
                        break;
                    }
                }
            }
                // Less or greater than. Actually I don't know how to get the possibility of this operation.
                // First we can simply try equal possibility of equal, greater than and less than. 1 / 3 each.
                // Also need to figure out whether the same attrs or not then use different formula.
            else {
                if (left->code == NAME || right->code == NAME) {
                    string attrName = (left->code == NAME) ? left->value : right->value;
                    string relName = attrMap[attrName].first;
                    // cout << "relName: " << relName << endl;
                    // cout << "joinRels[0]: " << joinRels[0] << endl;
                    // cout << "joinRels[1]: " << joinRels[1] << endl;
                    if (relName != joinRels[0] && relName != joinRels[1]) {
                        isAlwaysTrue = true;
                        break;
                    }
                    orPairList[attrName] += 1.0 / 3.0;
                    if (orPairList[attrName] > 1.0) {
                        orPairList[attrName] = 1.0;
                    }
                }
                else if (left->code == right->code && ((curComp->code == GREATER_THAN && left->value > right->value) || (curComp->code == LESS_THAN && left->value < right->value))) {
                    isAlwaysTrue = true;
                    break;
                }
            }

            curOr = curOr->rightOr;
        }
        if (!isAlwaysTrue && !orPairList.empty()) {
            // cout << "I am here!" << endl;
            double reveRes = 1.0;
            for (auto iter = orPairList.begin(); iter != orPairList.end(); ++iter) {
                reveRes *= (1.0 - iter->second);
            }
            orRes = 1 - reveRes;
        }
        // cout << "orRes: " << orRes << endl;
        andRes *= orRes;

        curAnd = curAnd->rightAnd;
    }

    double result = 1.0;
    result *= relMap[joinRels[0]];
    if (joinRels[1] != "") result *= relMap[joinRels[1]];
    result *= andRes;

    // cout << "result: " << result << endl;
    return result;
}