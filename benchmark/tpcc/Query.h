//
// Created by Yi Lu on 7/19/18.
//

#pragma once

#include "benchmark/tpcc/Context.h"
#include "benchmark/tpcc/Random.h"
#include "common/FixedString.h"
#include <string>

#include "SDCs_injection/random_generator.h"

namespace aria {
namespace tpcc {

/**
 * Query.h当中负责生成两种查询：
 * 两种查询均为结构体，生成完毕后包含在对应事务的成员变量Query当中。
 * 1. NewOrderQuery：负责生成一个新的订单的查询
 * 2. PaymentQuery：负责生成一个支付订单的查询
 * 所有的数值均为随机生成（指定了范围的）
 * SQL层面的静默错误注入实现如下：在初始化的时候，记录原始订单信息当中的某一个数值，然后进行bit_flip操作，标记该query结构体
 * 在Transaction层面访问+查看标志，打印变化内容。
 * ——Yu
 */



struct NewOrderQuery {
  bool isRemote() const {
    for (auto i = 0; i < O_OL_CNT; i++) {
      if (INFO[i].OL_SUPPLY_W_ID != W_ID) {
        return true;
      }
    }
    return false;
  }

  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  int8_t O_OL_CNT;

  // 静默错误注入标志位和原始值
  // 这里选择D_ID作为注入对象——Yu
  bool SDC_To_Injected = false; 
  int32_t SDC_original_D_ID;

  struct NewOrderQueryInfo {
    int32_t OL_I_ID;  // [8191-100000]之间，且不重复——Yu
    int32_t OL_SUPPLY_W_ID; // [1-partition_num（分布式仓库总的数量）]之间，且90%的概率为本地仓库，10%的概率为其他远程仓库——Yu
    int8_t OL_QUANTITY; // [1-10]之间——Yu
  };

  NewOrderQueryInfo INFO[15]; // 冗余的存储，实际上用的元素的数量是O_OL_CNT，在初始化的时候O_OL_CNT是随机的，范围在5-15之间——Yu
};

class makeNewOrderQuery {
public:
  NewOrderQuery operator()(const Context &context, int32_t W_ID,
                           Random &random) const {
    NewOrderQuery query;
    // W_ID is constant over the whole measurement interval

    // W_ID 在整个测量时间段为常量——Yu
    query.W_ID = W_ID;
    // The district number (D_ID) is randomly selected within [1 ..
    // context.n_district] from the home warehouse (D_W_ID = W_ID).
    query.D_ID = random.uniform_dist(1, context.n_district);
    if(sdc_generator()){
      query.SDC_To_Injected = true;
      query.SDC_original_D_ID = query.D_ID;
      // 这里还是先限制范围来模拟比特翻转，主要还是考虑到bit_flip的操作可能会超出范围，进而引起很多麻烦的问题。
      // 需要保证和原来的不同。——Yu
      bool filp_bit_retry;
      do{
        filp_bit_retry = false;
        query.D_ID = random.uniform_dist(1, context.n_district);
        if(query.D_ID == query.SDC_original_D_ID)
          filp_bit_retry = true;
      }while(filp_bit_retry);
    } else {
      query.SDC_To_Injected = false;
    }

    // The non-uniform random customer number (C_ID) is selected using the
    // NURand(1023,1,3000) function from the selected district number (C_D_ID =
    // D_ID) and the home warehouse number (C_W_ID = W_ID).

    query.C_ID = random.non_uniform_distribution(1023, 1, 3000);

    // The number of items in the order (ol_cnt) is randomly selected within [5
    // .. 15] (an average of 10).

    query.O_OL_CNT = random.uniform_dist(5, 15);

    int rbk = random.uniform_dist(1, 100);

    for (auto i = 0; i < query.O_OL_CNT; i++) {

      // A non-uniform random item number (OL_I_ID) is selected using the
      // NURand(8191,1,100000) function. If this is the last item on the order
      // and rbk = 1 (see Clause 2.4.1.4), then the item number is set to an
      // unused value.

      // 生成OL_I_ID，且为不重复的生成。——Yu
      // 如果是最后一个，且rbk=1，则OL_I_ID设置为0，为一个没用的值。——Yu
      bool retry;
      do {
        retry = false;
        query.INFO[i].OL_I_ID =
            random.non_uniform_distribution(8191, 1, 100000);
        for (int k = 0; k < i; k++) {
          if (query.INFO[k].OL_I_ID == query.INFO[i].OL_I_ID) {
            retry = true;
            break;
          }
        }
      } while (retry);

      if (i == query.O_OL_CNT - 1 && rbk == 1) {
        query.INFO[i].OL_I_ID = 0;
      }

      // The first supplying warehouse number (OL_SUPPLY_W_ID) is selected as
      // the home warehouse 90% of the time and as a remote warehouse 10% of the
      // time.

      // 生成OL_SUPPLY_W_ID，90%的概率为W_ID，即本地仓库，10%的概率为远程仓库。——Yu
      if (i == 0) {
        int x = random.uniform_dist(1, 100);
        if (x <= context.newOrderCrossPartitionProbability &&
            context.partition_num > 1) {
          int32_t OL_SUPPLY_W_ID = W_ID;
          while (OL_SUPPLY_W_ID == W_ID) {
            OL_SUPPLY_W_ID = random.uniform_dist(1, context.partition_num);
          }
          query.INFO[i].OL_SUPPLY_W_ID = OL_SUPPLY_W_ID;
        } else {
          query.INFO[i].OL_SUPPLY_W_ID = W_ID;
        }
      } else {
        query.INFO[i].OL_SUPPLY_W_ID = W_ID;
      }
      query.INFO[i].OL_QUANTITY = random.uniform_dist(1, 10);
    }

    return query;
  }
};

struct PaymentQuery {
  int32_t W_ID;
  int32_t D_ID;
  int32_t C_ID;
  FixedString<16> C_LAST;
  int32_t C_D_ID;
  int32_t C_W_ID;
  float H_AMOUNT;
  // 静默错误注入标志位——Yu
  bool SDC_To_Injected = false; 
  int32_t SDC_original_D_ID;
};

class makePaymentQuery {
public:
  PaymentQuery operator()(const Context &context, int32_t W_ID,
                          Random &random) const {
    PaymentQuery query;

    // W_ID is constant over the whole measurement interval

    query.W_ID = W_ID;

    // The district number (D_ID) is randomly selected within [1
    // ..context.n_district] from the home warehouse (D_W_ID) = W_ID).

    query.D_ID = random.uniform_dist(1, context.n_district);
    // 为了代码好运行起来，这里也选择对D_ID进行静默错误注入——Yu
    if(sdc_generator()){
      query.SDC_To_Injected = true;
      query.SDC_original_D_ID = query.D_ID;
      // 这里还是先限制范围来模拟比特翻转，主要还是考虑到bit_flip的操作可能会超出范围，进而引起很多麻烦的问题。
      // 需要保证和原来的不同
      bool filp_bit_retry;
      do{
        filp_bit_retry = false;
        query.D_ID = random.uniform_dist(1, context.n_district);
        if(query.D_ID == query.SDC_original_D_ID)
          filp_bit_retry = true;
      }while(filp_bit_retry);
    } else {
      query.SDC_To_Injected = false;
    }

    // the customer resident warehouse is the home warehouse 85% of the time
    // and is a randomly selected remote warehouse 15% of the time.

    // If the system is configured for a single warehouse,
    // then all customers are selected from that single home warehouse.

    int x = random.uniform_dist(1, 100);
    // 85%的概率为本地仓库，15%的概率为远程仓库。——Yu

    if (x <= context.paymentCrossPartitionProbability &&
        context.partition_num > 1) {
      // If x <= 15 a customer is selected from a random district number (C_D_ID
      // is randomly selected within [1 .. context.n_district]), and a random
      // remote warehouse number (C_W_ID is randomly selected within the range
      // of active warehouses (see Clause 4.2.2), and C_W_ID ≠ W_ID).

      int32_t C_W_ID = W_ID;

      while (C_W_ID == W_ID) {
        C_W_ID = random.uniform_dist(1, context.partition_num);
      }

      query.C_W_ID = C_W_ID;
      query.C_D_ID = random.uniform_dist(1, context.n_district);
    } else {
      // If x > 15 a customer is selected from the selected district number
      // (C_D_ID = D_ID) and the home warehouse number (C_W_ID = W_ID).

      query.C_D_ID = query.D_ID;
      query.C_W_ID = W_ID;
    }

    // a CID is always used.
    int y = random.uniform_dist(1, 100);
    // The customer is randomly selected 60% of the time by last name (C_W_ID ,
    // C_D_ID, C_LAST) and 40% of the time by number (C_W_ID , C_D_ID , C_ID).

    if (y <= 60 && context.payment_look_up) {
      // If y <= 60 a customer last name (C_LAST) is generated according to
      // Clause 4.3.2.3 from a non-uniform random value using the
      // NURand(255,0,999) function.

      std::string last_name =
          random.rand_last_name(random.non_uniform_distribution(255, 0, 999));
      query.C_LAST.assign(last_name);
      query.C_ID = 0;
    } else {
      // If y > 60 a non-uniform random customer number (C_ID) is selected using
      // the NURand(1023,1,3000) function.
      query.C_ID = random.non_uniform_distribution(1023, 1, 3000);
    }

    // The payment amount (H_AMOUNT) is randomly selected within [1.00 ..
    // 5,000.00].

    query.H_AMOUNT = random.uniform_dist(1, 5000);
    return query;
  }
};
} // namespace tpcc
} // namespace aria
