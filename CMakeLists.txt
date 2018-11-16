cmake_minimum_required(VERSION 3.12)
project(DataIngestion)

set(CMAKE_CXX_STANDARD 14)

set(
        SOURCE_FILE
        myTime.h
        RingBuffer.h
        DataInputManager.h
        Bucket.h
        DataItem.h
        MyThreadPool.h
        DataStreamUtil.h)

add_executable(DataIngestion main.cpp ${SOURCE_FILE})