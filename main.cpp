#include <iostream>
#include <sstream>
#include <fstream>

#include "DataInputManager.h"
#include "myTime.h"

using namespace std;

template<class T>
vector<DataItem<int>> create_test_data(size_t size, int loop) {
    /**
     * 构造测试数据，主要构造的是模拟正常timestamp顺序数据
     * 通过加入一个小的误差的方差使得数据中产生一些乱序问题来模拟可能存在的网络延时等问题造成的数据不是顺序到来
     */
    vector<DataItem<T>> in;
    in.reserve(size);
    in.clear();
    DataItem<T> dataItem;
    dataItem.measurement_ = "test";
    dataItem.tagk_ = vector<std::string>({"platform", "city"});
    dataItem.tagv_ = vector<std::string>({"windows", "wuhan"});
    dataItem.fields_ = vector<std::string>(
            {"value1", "value2", "value3", "value4", "value5", "value6", "value7", "value8", "value9"});
    uint64_t base_time = long_time();
    for (size_t i = size * loop; i < size * (loop + 1); i++) {
        /**
         * base_time + i * 5 构造的是正常时间顺序，每个数据间隔5
         * rand() % 20 - 10 为加入的偏差（-10——10的偏差）
         */
        dataItem.timestamp_ = base_time + i * 5 + rand() % 20 - 10;
        dataItem.values_.clear();
        for (int j = 0; j < 9; j++) {
            dataItem.values_.emplace_back(i);
        }
        in.emplace_back(dataItem);
    }
    return in;
}

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

int main() {
    DataManager<int> dataManager;
    int total_size = 1000 * 100;
    int size = 10000;
    int loops = total_size / size;

//    DataItem<int> dataItem;
//    dataItem.measurement_ = "test";
//    dataItem.tagk_ = vector<std::string>({"platform"});
//    dataItem.tagv_ = vector<std::string>({"windows"});
//    dataItem.fields_ = vector<std::string>({"value"});
//    for (int i = 0; i < size; i++) {
//        dataItem.values_ = vector<int>({i});
//        dataItem.timestamp_ = long_time();
//        dataManager.Add(dataItem);
//    }

    int index = 0;
    double create_time = 0.0;
    double insert_time = 0.0;
//    ofstream out("E:/Desktop/testData.txt");
//    if (!out) return 0;
    while (index < loops) {
        double t1 = double_time();
        vector<DataItem<int>> testData = create_test_data<int>(size, index);
        double t2 = double_time();
        create_time += t2 - t1;
        for (int i = 0; i < testData.size(); i++) {
            dataManager.Add2(testData[i]);
//            for (int j = 0; j < testData[i].fields_.size(); j++) {
//                out << "key:" << SerializeKey(testData[i].measurement_, testData[i].tagk_, testData[i].tagv_,
//                                              testData[i].fields_[j]);
//                out << " " << testData[i].values_[j];
//                out << " " << testData[i].timestamp_ << ";";
//            }

        }
        double t3 = double_time();
        insert_time += t3 - t2;
        index++;
        cout << "loop " << index << " completed!" << endl;
    }
    cout << "create time : " << create_time << "s" << endl;
    cout << "insert size : " << total_size << " points" << endl;
    cout << "insert time : " << insert_time << "s" << endl;
    cout << "insert rate : " << size * loops / insert_time << " points/s" << endl;
    cout << "sort to bucket time : " << dataManager.GetTime() << "s" << endl;
    cout << "add to buffer time : " << dataManager.GetAddTime() << "s" << endl;
    return 0;
}