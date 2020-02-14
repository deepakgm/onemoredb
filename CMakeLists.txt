cmake_minimum_required(VERSION 3.15)
project(a2test)

set(CMAKE_CXX_STANDARD 14)

include_directories(.)

add_executable(a2test
        BigQ.cc
        BigQ.h
        Comparison.cc
        Comparison.h
        ComparisonEngine.cc
        ComparisonEngine.h
        DBFile.cc
        DBFile.h
        Defs.h
        File.cc
        File.h
        Parser.y
        ParseTree.h
        Pipe.cc
        Pipe.h
        Record.cc
        Record.h
        Schema.cc
        Schema.h
        test.cc
        test.h
        TwoWayList.cc
        TwoWayList.h)
