#ifndef ONEMOREDB_TESTKIT_H
#define ONEMOREDB_TESTKIT_H

#include <stdio.h>
#include <stdlib.h>
#include <iostream>
#include <math.h>

#include "Function.h"
#include "Pipe.h"
#include "DBFile.h"
#include "Record.h"

using namespace std;

extern "C" {
int yyparse(void);   // defined in y.tab.c
int yyfuncparse(void);   // defined in yyfunc.tab.c
void init_lexical_parser (char *); // defined in lex.yy.c (from Lexer.l)
void close_lexical_parser (); // defined in lex.yy.c
void init_lexical_parser_func (char *); // defined in lex.yyfunc.c (from Lexerfunc.l)
void close_lexical_parser_func (); // defined in lex.yyfunc.c
}

extern struct AndList *final;
extern struct FuncOperator *finalfunc;
extern FILE *yyin;


void get_cnf (char *input, Schema *left, CNF &cnf_pred, Record &literal) {
    init_lexical_parser (input);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << input << endl;
        exit (1);
    }
    cnf_pred.GrowFromParseTree(final, left, literal); // constructs CNF predicate
    close_lexical_parser ();
}


void get_cnf (char *input, Schema *left, Schema *right, CNF &cnf_pred, Record &literal) {
    init_lexical_parser (input);
    if (yyparse() != 0) {
        cout << " Error: can't parse your CNF " << input << endl;
        exit (1);
    }
    cnf_pred.GrowFromParseTree (final, left, right, literal); // constructs CNF predicate
    close_lexical_parser ();
}


void get_cnf (char *input, Schema *left, Function &fn_pred) {
    init_lexical_parser_func (input);
    if (yyfuncparse() != 0) {
        cout << " Error: can't parse your arithmetic expr. " << input << endl;
        exit (1);
    }
    fn_pred.GrowFromParseTree (finalfunc, *left); // constructs CNF predicate
    close_lexical_parser_func ();
}
#endif //ONEMOREDB_TESTKIT_H
