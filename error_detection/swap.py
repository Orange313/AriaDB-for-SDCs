import csv
from collections import defaultdict
import random

def parse_log(log_file):
    entries = []
    with open(log_file, 'r') as f:
        reader = csv.DictReader(f)
        for row in reader:
            entries.append(row)
    return entries

def create_blocks(entries):
    # 创建事务块(连续相同事务ID为一个块)，事务ID，事务条目，访问集
    blocks = []
    if not entries:
        return blocks
    
    current_block = {
        'txn_id': entries[0]['TxnID'],
        'entries': [entries[0]],
        'access': set([(entries[0]['TableID'], entries[0]['PartitionID'])])
    }
    
    for entry in entries[1:]:
        if entry['TxnID'] == current_block['txn_id']:
            current_block['entries'].append(entry)
            current_block['access'].add((entry['TableID'], entry['PartitionID']))
        else:
            blocks.append(current_block)
            current_block = {
                'txn_id': entry['TxnID'],
                'entries': [entry],
                'access': set([(entry['TableID'], entry['PartitionID'])])
            }
    
    if current_block['entries']:
        blocks.append(current_block)
    
    return blocks

def blocks_conflict(block1, block2):
    # 检查两个块是否有冲突（访问相同表+分区），集合是否有交集
    return not block1['access'].isdisjoint(block2['access'])

def reorder_adjacent_blocks(blocks, max_passes=3):
    # 重新排序相邻无冲突的块
    print("\n=== 初始块顺序 ===")
    for i, block in enumerate(blocks):
        print(f"块 {i}: Txn {block['txn_id']} (条目数: {len(block['entries'])})")
    
    total_swaps = 0
    
    for pass_num in range(1, max_passes+1):
        print(f"\n=== 第 {pass_num} 轮交换扫描 ===")
        swaps_in_pass = 0
        
        i = 0
        while i < len(blocks) - 1:
            if not blocks_conflict(blocks[i], blocks[i+1]):
                # 随机决定是否交换
                if random.random() < 0.5:  # 50%交换概率
                    blocks[i], blocks[i+1] = blocks[i+1], blocks[i]
                    swaps_in_pass += 1
                    print(f"交换 块 {i}(Txn {blocks[i+1]['txn_id']}) ↔ "
                          f"块 {i+1}(Txn {blocks[i]['txn_id']})")
                    i += 1  # 跳过下一个块
            i += 1
        
        total_swaps += swaps_in_pass
        print(f"本轮完成 {swaps_in_pass} 次交换")
    
    print(f"\n=== 交换总结 ===")
    print(f"总共完成 {total_swaps} 次相邻块交换")
    
    return blocks

def write_log(blocks, output_file):
    # 将块合并并写入日志文件
    fieldnames = ['LSN', 'TxnID', 'TableID', 'PartitionID', 'Key', 'Value']
    
    # 合并所有块并重新生成LSN
    entries = []
    for block in blocks:
        entries.extend(block['entries'])
    
    for i, entry in enumerate(entries, start=1):
        entry['LSN'] = str(i)
    
    # 写入文件
    with open(output_file, 'w', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(entries)

def main():
    input_file = "error_detection\data\original\A\log01.csv"
    output_file = "error_detection\data\original\B\log01b.csv"
    
    random.seed(42)
    
    print(f"处理日志文件: {input_file}")
    entries = parse_log(input_file)

    blocks = create_blocks(entries)

    reordered_blocks = reorder_adjacent_blocks(blocks)

    write_log(reordered_blocks, output_file)
    print(f"\n结果已写入: {output_file}")

if __name__ == "__main__":
    main()