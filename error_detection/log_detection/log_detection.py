import csv
from collections import Counter
"""
使用集合存储log,通过集合运算对比
"""
def parse_log(log_file):
    entries = []
    with open(log_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.append(row)
    return entries

def normalize_operation(entry):
    return (
        entry['TxnID'],
        entry['TableID'],
        entry['PartitionID'],
        entry['Key'],
        entry['Value']
    )

def compare_unordered_ops(log1, log2):

    # 统计操作出现次数
    ops1 = Counter(normalize_operation(entry) for entry in log1)
    ops2 = Counter(normalize_operation(entry) for entry in log2)
    
    # 计算差异
    only_in_log1 = dict(ops1 - ops2)
    only_in_log2 = dict(ops2 - ops1)
    
    return not (only_in_log1 or only_in_log2), only_in_log1, only_in_log2

def print_operation(op, count=1):
    txn_id, table_id, part_id, key, value = op
    print(f"Txn {txn_id} -> Table {table_id} Partition {part_id}")
    print(f"Key: {key}")
    print(f"Value: {value}")
    if count > 1:
        print(f"出现次数: {count}")
    print("-" * 40)

def detailed_comparison(log1_path, log2_path):
    #print(f"正在比对日志文件: {log1_path} 和 {log2_path}")
    # 解析日志
    log1 = parse_log(log1_path)
    log2 = parse_log(log2_path)
    
    # 执行比对
    is_match, only_in_log1, only_in_log2 = compare_unordered_ops(log1, log2)
    
    # 输出结果
    if is_match:
        print("\n Result: The set of operations in the two logs is completely consistent.")
        return True
    else:
        print("\n Result: There are differences in the set of actions")
        
        if only_in_log1:
            print(f"\n only in {log1_path}: ({len(only_in_log1)} 处):")
            for op, count in only_in_log1.items():
                print_operation(op, count)
        
        if only_in_log2:
            print(f"\nonly in {log2_path}: ({len(only_in_log2)} 处):")
            for op, count in only_in_log2.items():
                print_operation(op, count)
        
        return False

if __name__ == "__main__":
    log1_path = "log1_with_sdc_33021.csv"
    log2_path = "log2_33021.csv"
    detailed_comparison(log1_path, log2_path)