#ifndef RANDOM_GENERATOR_H
#define RANDOM_GENERATOR_H

#include <iostream>
#include <random>

bool sdc_generator(){
    double prob_sdc = 0.00005; // 假设以 2万分之一 的概率发生SDC——Yu
    std::random_device rd;
    std::mt19937 gen(rd());
    std::uniform_real_distribution<> dis(0.0, 1.0);
    return dis(gen) < prob_sdc;
}


#endif // RANDOM_GENERATOR_H