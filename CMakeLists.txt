cmake_minimum_required(VERSION 3.15)
project(onemoredb)

set(CMAKE_CXX_STANDARD 14)



add_executable(onemoredb
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        main.cc
        ParseTree.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        TwoWayList.cc
        TwoWayList.h)
