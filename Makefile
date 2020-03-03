CC = g++ -O2 -Wno-deprecated  -std=c++11

tag = -i

ifdef linux
tag = -n
endif

main.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o main.o
	$(CC) -o main.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o main.o

test.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o test.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o test.o

gtest.out: Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o gtest.o
	$(CC) -o test.out Record.o Comparison.o ComparisonEngine.o Schema.o File.o BigQ.o DBFile.o Pipe.o y.tab.o lex.yy.o Meta.o gtest.o -lgtest

main.o: main.cc
	$(CC) -g -c main.cc

test.o: test.cc
	$(CC) -g -c test.cc

gtest.o: gtest.cc
	$(CC) -g -c gtest.cc

Meta.o: Meta.cc
	$(CC) -g -c Meta.cc

Comparison.o: Comparison.cc
	$(CC) -g -c Comparison.cc

ComparisonEngine.o: ComparisonEngine.cc
	$(CC) -g -c ComparisonEngine.cc

Pipe.o: Pipe.cc
	$(CC) -g -c Pipe.cc

BigQ.o: BigQ.cc
	$(CC) -g -c BigQ.cc

DBFile.o: DBFile.cc
	$(CC) -g -c DBFile.cc

File.o: File.cc
	$(CC) -g -c File.cc

Record.o: Record.cc
	$(CC) -g -c Record.cc

Schema.o: Schema.cc
	$(CC) -g -c Schema.cc
	
y.tab.o: Parser.y
	yacc -d Parser.y
	sed $(tag) -e "s/  __attribute__ ((__unused__))$$/# ifndef __cplusplus\n  __attribute__ ((__unused__));\n# endif/" y.tab.c
	g++ -c y.tab.c

lex.yy.o: Lexer.l
	lex  Lexer.l
	gcc  -c lex.yy.c

clean:
	rm -f *.o
	rm -f *.out
	rm -f y.tab.c
	rm -f lex.yy.c
	rm -f y.tab.h