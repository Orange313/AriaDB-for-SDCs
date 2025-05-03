//
// Created by Yi Lu on 1/7/19.
//

#pragma once

#include "core/Partitioner.h"
#include "core/Table.h"
#include "protocol/Aria/AriaHelper.h"
#include "protocol/Aria/AriaMessage.h"
#include "protocol/Aria/AriaTransaction.h"
#include <fstream>
#include <sstream>
#include <iomanip>
#include <mutex>
#include <atomic>


namespace aria {

  inline std::string hex_encode(const void* data, size_t len) {
    std::ostringstream oss;
    const uint8_t* ptr = reinterpret_cast<const uint8_t*>(data);
    for (size_t i = 0; i < len; ++i) {
      oss << std::hex << std::setw(2) << std::setfill('0')
          << static_cast<int>(ptr[i]);
    }
    return oss.str();
  }

template <class Database> class Aria {
public:
  using DatabaseType = Database;
  using MetaDataType = std::atomic<uint64_t>;
  using ContextType = typename DatabaseType::ContextType;
  using MessageType = AriaMessage;
  using TransactionType = AriaTransaction;

  using MessageFactoryType = AriaMessageFactory;
  using MessageHandlerType = AriaMessageHandler;

  Aria(DatabaseType &db, const ContextType &context, Partitioner &partitioner)
      : db(db), context(context), partitioner(partitioner) {
    std::lock_guard<std::mutex> lock(csv_mutex);
    static bool header_written = false;
    if (!header_written) {
      std::ofstream outFile("log_02.csv", std::ios::app);
      if (outFile.is_open()) {
        outFile << "LSN,TxnID,TableID,PartitionID,Key,Value\n";
        header_written = true;
      }
    }
  }

  void abort(TransactionType &txn,
             std::vector<std::unique_ptr<Message>> &messages) {
    // nothing needs to be done
  }

  bool commit(TransactionType &txn,
              std::vector<std::unique_ptr<Message>> &messages) {

    //LOG(INFO) << "Committing txn " << txn.get_id();

    std::lock_guard<std::mutex> lock(csv_mutex);
    std::ofstream outFile("log_02.csv", std::ios::app);
    bool log_enabled = outFile.is_open();

    auto &writeSet = txn.writeSet;
    for (auto i = 0u; i < writeSet.size(); i++) {
      auto &writeKey = writeSet[i];
      auto tableId = writeKey.get_table_id();
      auto partitionId = writeKey.get_partition_id();
      auto table = db.find_table(tableId, partitionId);

      size_t key_len = table->key_size();
      size_t value_len = table->value_size();

      if (log_enabled) {
        uint64_t LSN = global_lsn.fetch_add(1);
        outFile << LSN << "," << txn.get_id()  << "," << tableId << "," << partitionId << ","
                << hex_encode(writeKey.get_key(), key_len) << ","
                << hex_encode(writeKey.get_value(), value_len) << "\n";
      }

      if (partitioner.has_master_partition(partitionId)) {
        auto key = writeKey.get_key();
        auto value = writeKey.get_value();
        table->update(key, value);
      } else {
        auto coordinatorID = partitioner.master_coordinator(partitionId);
        txn.network_size += MessageFactoryType::new_write_message(
            *messages[coordinatorID], *table, writeKey.get_key(),
            writeKey.get_value());
      }
    }

    return true;
  }

private:
  DatabaseType &db;
  const ContextType &context;
  Partitioner &partitioner;
  static std::mutex csv_mutex;
  static std::atomic<uint64_t> global_lsn;
};
  template <class Database>
  std::mutex Aria<Database>::csv_mutex;

  template <class Database>
  std::atomic<uint64_t> Aria<Database>::global_lsn{1};

} // namespace aria