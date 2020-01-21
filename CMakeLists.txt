cmake_minimum_required(VERSION 3.15)
project(P2)

set(CMAKE_CXX_STANDARD 11)

add_executable(P2
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        Defs.h
        File.cc
        File.h
        main.cpp
        ParseTree.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        TwoWayList.cc
        DBFile.cc
        DBFile.h
        TwoWayList.h
        )