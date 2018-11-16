//
// Created by zzhfeng on 2018/11/13.
//

#ifndef DATAINGESTION_BUCKET_H
#define DATAINGESTION_BUCKET_H

#pragma once

#include <iostream>
#include <map>
#include <algorithm>
#include "DataItem.h"

union ValueUnion {
    int i;
    long l;
    uint32_t ui;
    uint64_t ul;
    float f;
    double d;
};

struct Value {
    ValueUnion v;
    uint64_t timestamp;

    bool operator<(const Value &v) {
        return timestamp < v.timestamp;
    }
};

class Bucket {
    std::atomic_flag lock = ATOMIC_FLAG_INIT;
    std::string key_;
    DataType dataType_;
    std::shared_ptr<std::vector<Value>> values_;

public:

    Bucket() {
        values_ = std::make_shared<std::vector<Value>>();
    };

    Bucket(std::string key, DataType dataType) : key_(key), dataType_(dataType) {
        values_ = std::make_shared<std::vector<Value>>();
    }

    void add(Value v) {
//        while(lock.test_and_set()){}
        values_->emplace_back(v);
//        lock.clear();
    };

    void test() {
        std::vector<Value> v = *values_;
        std::sort(v.begin(), v.end());
        values_ = std::make_shared<std::vector<Value>>();
    }

    bool operator<(const Bucket &b) { return key_ < b.key_; };

    void operator=(const Bucket &b) {
        key_ = b.key_;
        values_ = b.values_;
    };
};

class Buckets {
private:
    std::map<std::string, std::shared_ptr<Bucket>> buckets;
    std::map<std::string, DataType> key_to_type_map;

public:
    template<typename T>
    void add(DataItem<T> v) {
        if (v.fields_.size() > v.values_.size()) {
            //TODO 报错
            return;
        }
        DataType dataType = GetType<T>();
        for (int i = 0; i < v.fields_.size(); i++) {
            ValueUnion valueUnion = GetValueByType(v.values_[i]);
            Value value = {valueUnion, v.timestamp_};
            std::string key = SerializeKey(v.measurement_, v.tagk_, v.tagv_, v.fields_[i]);
            add(key, dataType, value);
        }
    };

    void add(std::string key, DataType dataType, Value value) {
        if (buckets.count(key)) {
            buckets[key]->add(value);
            return;
        }
        key_to_type_map[key] = dataType;
        buckets[key] = std::make_shared<Bucket>(key, dataType);
        buckets[key]->add(value);
    };


    std::map<std::string, std::shared_ptr<Bucket>> GetBuckets() {
        return buckets;
    };

    void testBucket() {
        for (auto it = buckets.begin(); it != buckets.end(); it++) {
            std::cout << "key : " << it->first << std::endl;
            it->second->test();
        }
    }


private:
    std::string SerializeKey(std::string measurement, std::vector<std::string> tagk, std::vector<std::string> tagv,
                             std::string field) {
        if (tagk.size() <= 0 || tagv.size() <= 0)
            return measurement + " " + field;
        int len = tagk.size() >= tagv.size() ? tagv.size() : tagk.size();
        std::string key = measurement + " ";
        for (int i = 0; i < len; i++) {
            key += tagk[i] + "=" + tagv[i];
            if (i != len - 1)
                key += ",";
        }
        key += (" " + field);
        return key;
    }

    template<typename T>
    DataType GetType() {
        if (typeid(T) == typeid(int))
            return Int;
        if (typeid(T) == typeid(long))
            return Int64;
        if (typeid(T) == typeid(uint32_t))
            return Uint;
        if (typeid(T) == typeid(uint64_t))
            return Uint64;
        if (typeid(T) == typeid(float))
            return Float;
        if (typeid(T) == typeid(double))
            return Double;
    }

    template<typename T>
    ValueUnion GetValueByType(T t) {
        DataType type = GetType<T>();
        ValueUnion v;
        if (type == Int) {
            v.i = t;
            return v;
        }
        if (type == Int64) {
            v.l = t;
            return v;
        }
        if (type == Uint) {
            v.ui = t;
            return v;
        }
        if (type == Uint64) {
            v.ul = t;
            return v;
        }
        if (type == Float) {
            v.f = t;
            return v;
        }
        if (type == Double) {
            v.d = t;
            return v;
        }
        //TODO 报错，类型错误
        return v;
    }
};

#endif //DATAINGESTION_BUCKET_H
