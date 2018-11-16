//
// Created by zzhfeng on 2018/11/13.
//

#ifndef DATAINGESTION_DATAITEM_H
#define DATAINGESTION_DATAITEM_H

#pragma once

#include <memory>
#include <string>
#include <vector>

enum DataType {
    Int = 1,
    Int64 = 2,
    Uint = 3,
    Uint64 = 4,
    Float = 5,
    Double = 6
};

template<class T>
struct DataItem {
    uint64_t timestamp_;
    std::string measurement_;
    std::vector<std::string> tagk_;
    std::vector<std::string> tagv_;
    std::vector<std::string> fields_;
    std::vector<T> values_;

    DataItem() = default;

    DataItem(uint64_t timestamp,
             std::string measurement,
             std::vector<std::string> tagk,
             std::vector<std::string> tagv,
             std::vector<std::string> fields, std::vector<T> values) :
            timestamp_(timestamp),
            measurement_(measurement),
            tagk_(tagk),
            tagv_(tagv),
            fields_(fields),
            values_(values) {}

    bool operator<(const DataItem d) {
        return timestamp_ < d.timestamp_;
    }
};

#endif //DATAINGESTION_DATAITEM_H
