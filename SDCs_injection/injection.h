#ifndef INJECTION_H
#define INJECTION_H
#include <iostream>
#include <random>
#include <type_traits>
#include <cstddef>
#include <cstring>


template <typename KeyType>
KeyType flip_bit(KeyType &key) {
   KeyType corruptedKey = key;
    std::size_t size = sizeof(KeyType);

    static std::random_device rd;
    static std::mt19937 gen(rd());
    std::uniform_int_distribution<std::size_t> byteDist(0, size - 1);
    std::uniform_int_distribution<int> bitDist(0, 7);

    // 通过随机选择要翻转的比特位（先选择字节，然后选择该字节当中的一位）
    // 但是这样的修改可能会产生异常，比如key值当中的某个值超范围什么的
    std::size_t byteIndex = byteDist(gen);
    int bitIndex = bitDist(gen);

    auto *bytePtr = reinterpret_cast<unsigned char *>(&corruptedKey);
    bytePtr[byteIndex] ^= (1 << bitIndex);

    return corruptedKey;
}



#endif