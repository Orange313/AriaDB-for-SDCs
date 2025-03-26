#ifndef RANDOM_GENERATOR_H
#define RANDOM_GENERATOR_H

#include <iostream>
#include <random>

bool sdc_generator(){
    constexpr double prob_sdc = 0.00005; // 假设以 2万分之一 的概率发生SDC——Yu
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_real_distribution<> dis(0.0, 1.0);
    return dis(gen) < prob_sdc;
}

int sdc_random_choice(int n) {
    static std::random_device rd;
    static std::mt19937 gen(rd());
    static std::uniform_int_distribution<int> dist(1, n);
    return dist(gen);
}

#endif // RANDOM_GENERATOR_H