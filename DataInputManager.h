//
// Created by zzhfeng on 2018/11/13.
//

#ifndef DATAINGESTION_DATAINPUTMANAGER_H
#define DATAINGESTION_DATAINPUTMANAGER_H

#pragma once

#include <algorithm>
#include <atomic>
#include <thread>

#include "RingBuffer.h"
#include "Bucket.h"
#include "MyThreadPool.h"
#include "DataStreamUtil.h"
#include "myTime.h"

template<class T>
class DataManager {
private:
    std::shared_ptr<DataRingBuffer<T>> ringbuffer_data_cache;
    std::shared_ptr<DataRingBuffer<T>> ringbuffer_data_snapshot;
    std::vector<DataItem<T>> data_sorting_cache;
    Buckets data_bucket;
    std::ThreadPool threadPool;
    std::vector<std::future<void >> fv;
    size_t buf_size;
    bool isSorting = false;
    bool sortCompleted = false;
    std::atomic_flag sort_flag = ATOMIC_FLAG_INIT;
    double time = 0;
    double add_time = 0;

public:
    DataManager() : buf_size(DATA_RINGBUFFER_SIZE) {
        ringbuffer_data_cache = std::make_shared<DataRingBuffer<T>>();
    };

    void Add(DataItem<T> item) {
        ringbuffer_data_cache->AddItem(item);
        if (ringbuffer_data_cache->IsFull()) {
            ringbuffer_data_snapshot = ringbuffer_data_cache;
            ringbuffer_data_cache = std::make_shared<DataRingBuffer<T>>();
            while (!sort_flag.test_and_set() && !isSorting) {
                isSorting = true;
//                threadPool.commit(
//                        SortCacheDataToBucket<T>,
//                        std::ref(ringbuffer_data_snapshot),
//                        std::ref(data_sorting_cache),
//                        buf_size,
//                        std::ref(data_bucket),
//                        std::ref(isSorting));
//                SortCacheDataToBucket(ringbuffer_data_snapshot, data_sorting_cache, buf_size, data_bucket, isSorting);
                std::thread t(&DataManager::SortCacheDataToBucket, this);
                t.join();
            }
            sort_flag.clear();
        }
    }

    void Add2(DataItem<T> item) {
        double t1 = double_time();
        ringbuffer_data_cache->AddItem(item);
        if (ringbuffer_data_cache->IsFull()) {
            ringbuffer_data_snapshot = ringbuffer_data_cache;
            ringbuffer_data_cache = std::make_shared<DataRingBuffer<T>>();
            while (!sort_flag.test_and_set() && !isSorting) {
                isSorting = true;
                std::thread t(&DataManager::TransferRingbufferDataToBucket2, this);
                t.join();
            }
            sort_flag.clear();
        }
        double t2 = double_time();
        add_time += t2 - t1;
    }

    double GetTime() { return time; }

    double GetAddTime() { return add_time; }

private:
    void SortCacheDataToBucket() {
        double t1 = double_time();
        data_sorting_cache.insert(data_sorting_cache.end(), ringbuffer_data_snapshot->GetDataPtr()->begin(),
                                  ringbuffer_data_snapshot->GetDataPtr()->end());
        std::sort(data_sorting_cache.begin(), data_sorting_cache.end());
        double t2 = double_time();
        time += t2 - t1;
        if (data_sorting_cache.size() >= buf_size * 2) {
            TransferRingbufferDataToBucket();
//            std::thread t(&DataManager::TransferRingbufferDataToBucket, this);
//            t.join();
            data_sorting_cache.erase(data_sorting_cache.begin(), data_sorting_cache.begin() + buf_size);
        }
        isSorting = false;
    }

    void TransferRingbufferDataToBucket() {
        for (auto it = data_sorting_cache.begin(); it != data_sorting_cache.begin() + buf_size; it++) {
            data_bucket.add(*it);
        }
        data_bucket.testBucket();
    }

    void TransferRingbufferDataToBucket2() {
        double t1 = double_time();
        for (auto it = ringbuffer_data_snapshot->GetDataPtr()->begin();
             it != ringbuffer_data_snapshot->GetDataPtr()->begin() + buf_size; it++) {
            data_bucket.add(*it);
        }
        isSorting = false;
        data_bucket.testBucket();
        double t2 = double_time();
        time += t2 - t1;
    }
};

#endif //DATAINGESTION_DATAINPUTMANAGER_H
