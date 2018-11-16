//
// Created by zzhfeng on 2018/11/14.
//

#ifndef DATAINGESTION_DATASTREAMUTIL_H
#define DATAINGESTION_DATASTREAMUTIL_H

#include <vector>
#include <algorithm>
#include <iostream>
#include "DataItem.h"
#include "Bucket.h"
#include "RingBuffer.h"

template<class T>
void TransferRingbufferDataToBucket(std::vector<DataItem<T>> &v, size_t size, Bucket &data_bucket) {
    for (auto it = v.begin(); it != v.begin() + size; it++) {
        std::cout << (*it).values_[0] << " ";
    }
}

template<class T>
void SortCacheDataToBucket(std::shared_ptr<DataRingBuffer<T>> ringbuffer_data_snapshot,
                           std::vector<DataItem<T>> &data_sorting_cache, size_t buf_size, Bucket &data_bucket,
                           bool &isSorting) {
    data_sorting_cache.insert(data_sorting_cache.end(), ringbuffer_data_snapshot->GetDataPtr()->begin(),
                              ringbuffer_data_snapshot->GetDataPtr()->end());
    std::sort(data_sorting_cache.begin(), data_sorting_cache.end());
    if (data_sorting_cache.size() >= buf_size * 2) {
        TransferRingbufferDataToBucket(data_sorting_cache, buf_size, data_bucket);
        data_sorting_cache.erase(data_sorting_cache.begin(), data_sorting_cache.begin() + buf_size);
    }
    isSorting = false;
}

#endif //DATAINGESTION_DATASTREAMUTIL_H
