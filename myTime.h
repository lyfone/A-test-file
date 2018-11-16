//
// Created by zzhfeng on 2018/11/13.
//

#ifndef DATAINGESTION_MYTIME_H
#define DATAINGESTION_MYTIME_H

#pragma once

#include <sys/time.h>

double double_time() {
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) return 0.0;
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

uint64_t long_time() {
    struct timeval tv;
    if (gettimeofday(&tv, NULL) != 0) return 0;
    return tv.tv_sec + tv.tv_usec;
}

#endif //DATAINGESTION_MYTIME_H
