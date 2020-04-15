cmake_minimum_required(VERSION 3.15)
project(onemoredb)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_library(
                lex.yy.c
                lex.yyfunc.c
                y.tab.c
                y.tab.h
                yyfunc.tab.c
                yyfunc.tab.h
                ParseFunc.h
                ParseTree.h
)


add_executable(onemoredb
        BigQ.cc
        BigQ.h
                DBFile.cc
                DBFile.h
        #        lex.yy.c
        #        lex.yyfunc.c
        #        y.tab.c
        #        y.tab.h
        #        yyfunc.tab.c
        #        yyfunc.tab.h
        #        ParseFunc.h
        #        ParseTree.h
        #        RelOp.cc
        #        RelOp.h
                test.cc
        #        test.h
        #        test1.cc
        #        test1.h
                gtest.cc


        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        Defs.h
        File.cc
        File.h
        Function.cc
        Function.h
        json.hpp
#        main.cc
        Meta.cc
        Meta.h

        Pipe.cc
        Pipe.h
        Record.cc
        Record.h

        Schema.cc
        Schema.h
        Statistics.cc
        Statistics.h
        testkit.h
        TwoWayList.cc
        TwoWayList.h
        a42.cc
        OpTreeNode.cc
        a4-2utils.h
        a3utils.h
        )
