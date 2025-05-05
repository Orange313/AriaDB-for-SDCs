# # 基于事务ID的全局比对
# function compare_logs_global_order(logA, logB):
#     sortedA = sort_by_transaction_id_then_sequence(logA)
#     sortedB = sort_by_transaction_id_then_sequence(logB)
#     for i from 0 to len(sortedA)-1:
#         if not compare_log_entry(sortedA[i], sortedB[i]):
#             return f"Mismatch at entry {i}"
#     return "Logs match globally"
# # 基于事务ID的分块比对
# function compare_logs_by_transaction_block(logA, logB):
#     blocksA = group_by_transaction_id(logA)
#     blocksB = group_by_transaction_id(logB)
#     blockA = blocksA[txn_id]
#     blockB = blocksB[txn_id]
#     for i from 0 to len(blockA) - 1:
#     if not compare_log_entry(blockA[i], blockB[i]):
#         return f"Mismatch in transaction {txn_id} at entry {i}"
#     return "Logs match by transaction blocks"