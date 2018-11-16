//
// Created by zzhfeng on 2018/11/13.
//

#ifndef DATAINGESTION_RINGBUFFER_H
#define DATAINGESTION_RINGBUFFER_H

#pragma once

#include <memory>
#include "DataItem.h"

#define DATA_RINGBUFFER_SIZE 5000           //单个ringbuffer大小
//#define DATA_RINGBUFFER_PARTITION 16        //ringbuffer分片数量

template<class T>
class DataRingBuffer {
private:
    std::shared_ptr<std::vector<DataItem<T>>> data_sptr;
    size_t size;
    size_t pos;

public:
    DataRingBuffer() : size(DATA_RINGBUFFER_SIZE), pos(0) {
        data_sptr = std::make_shared<std::vector<DataItem<T>>>(DATA_RINGBUFFER_SIZE);
    }

    DataRingBuffer(std::shared_ptr<std::vector<DataItem<T>>> data_ptr, size_t s) : size(s), pos(0) {
        data_sptr = data_ptr;
    }

    DataRingBuffer(std::vector<DataItem<T>> v, size_t s) : size(s), pos(0) {
        data_sptr = std::make_shared<std::vector<DataItem<T>>>(v);
    }

    void AddItem(DataItem<T> item) {
        (*data_sptr)[pos++] = item;
    }

    bool IsFull() {
        return pos == size;
    }

    std::shared_ptr<std::vector<DataItem<T>>> GetDataPtr() {
        return data_sptr;
    }

    size_t GetSize() {
        return size;
    }
};

#endif //DATAINGESTION_RINGBUFFER_H
