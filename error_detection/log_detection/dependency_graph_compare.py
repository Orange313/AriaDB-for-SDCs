import threading
from collections import defaultdict
from queue import Queue

def parse_log_file(filename, result_queue):
    # 解析日志文件
    try:
        transactions = defaultdict(list)
        all_entries = []
        transaction_blocks = []
        current_block = []
        current_txn_id = None
        
        with open(filename, 'r') as f:
            is_header = True  
            for line in f:
                line = line.strip()
                if not line:
                    continue
                    
                if is_header:
                    is_header = False
                    continue
                    
                parts = line.split(',')
                if len(parts) < 6:
                    continue
                txn_id, table_id, partition_id, key, value = parts[1:6]
                
                try:
                    txn_id = int(txn_id)
                    entry = {
                        'TxnID': txn_id,
                        'TableID': int(table_id),
                        'PartitionID': int(partition_id),
                        'Key': key,
                        'Value': value,
                        'raw': ','.join(parts[1:]) 
                    }
                    # 新的事务ID，创建新的块
                    if txn_id != current_txn_id:
                        if current_block:
                            transaction_blocks.append({
                                'TxnID': current_txn_id,
                                'Entries': current_block.copy()
                            })
                        current_block = [entry]
                        current_txn_id = txn_id
                    else:
                        current_block.append(entry)
                    
                    transactions[txn_id].append(entry)
                    all_entries.append(entry)
                    
                except ValueError as e:
                    print(f"Warning: Unable to parse line: {line}, error: {e}")
                    continue
        
        # 添加最后一个事务块
        if current_block:
            transaction_blocks.append({
                'TxnID': current_txn_id,
                'Entries': current_block
            })
        
        # 构建事务依赖图
        dependency_graph, transaction_blocks = build_block_dependency_graph(transaction_blocks)
        
        result_queue.put({
            'filename': filename,
            'transactions': transactions,
            'blocks': transaction_blocks,
            'entries': all_entries,
            'graph': dependency_graph,
            'error': None
        })
    except Exception as e:
        result_queue.put({
            'filename': filename,
            'error': str(e)
        })

def build_block_dependency_graph(transaction_blocks):
    # 构建事务依赖图
    key_to_blocks = defaultdict(list)
    block_order = {}
    
    # 遍历确定事务块顺序和每个事务块修改的键
    block_to_keys = defaultdict(set)
    for i, block in enumerate(transaction_blocks):
        block_id = i  # 块索引
        block['BlockID'] = block_id
        block_order[block_id] = i
        
        for entry in block['Entries']:
            key = entry['Key']
            block_to_keys[block_id].add(key)
    
    # 建立依赖关系
    dependency_graph = defaultdict(set)
    for i, block in enumerate(transaction_blocks):
        block_id = block['BlockID']
        
        for entry in block['Entries']:
            key = entry['Key']
            
            # 对于当前事务块修改的键，检查之前修改过这个键的事务块
            for prev_block_id, prev_block in [(b['BlockID'], b) for b in key_to_blocks[key]]:
                if prev_block_id != block_id and block_order[prev_block_id] < block_order[block_id]:
                    # 检查是否是相同事务ID
                    if prev_block['TxnID'] != block['TxnID']:
                        dependency_graph[block_id].add(prev_block_id)
            
            key_to_blocks[key].append(block)
    
    return dependency_graph, transaction_blocks

def find_block_cycles(graph):
    # 判断是否循环依赖
    cycles = []
    
    def dfs(node, path, visited):
        if node in path:
            cycle_start = path.index(node)
            cycles.append(path[cycle_start:] + [node])
            return
        
        if node in visited:
            return
        
        visited.add(node)
        path.append(node)
        
        for neighbor in graph.get(node, set()):
            dfs(neighbor, path.copy(), visited)
    
    visited = set()
    for node in graph:
        if node not in visited:
            dfs(node, [], visited)
    
    return cycles

def compare_block_dependency_graphs(graph1, blocks1, graph2, blocks2):
    # 比较块依赖图
    if len(blocks1) != len(blocks2):
        return False, "The number of transaction blocks is different."
    
    # 创建事务ID到块ID的映射
    txn_to_block1 = {}
    for block in blocks1:
        txn_id = block['TxnID']
        block_id = block['BlockID']
        if txn_id not in txn_to_block1:
            txn_to_block1[txn_id] = []
        txn_to_block1[txn_id].append(block_id)
    
    txn_to_block2 = {}
    for block in blocks2:
        txn_id = block['TxnID']
        block_id = block['BlockID']
        if txn_id not in txn_to_block2:
            txn_to_block2[txn_id] = []
        txn_to_block2[txn_id].append(block_id)
    
    # 比较依赖关系
    for txn_id in set(txn_to_block1.keys()) | set(txn_to_block2.keys()):
        blocks_1 = txn_to_block1.get(txn_id, [])
        blocks_2 = txn_to_block2.get(txn_id, [])
        
        if len(blocks_1) != len(blocks_2):
            return False, f"The number of blocks for transaction ID {txn_id} is different: file 1 has {len(blocks_1)}, file 2 has {len(blocks_2)}"
        
        # 对每个块比较依赖关系
        for i in range(len(blocks_1)):
            block_id1 = blocks_1[i]
            block_id2 = blocks_2[i]
            
            deps1 = set()
            for dep_id in graph1.get(block_id1, set()):
                deps1.add(blocks1[dep_id]['TxnID'])
            
            deps2 = set()
            for dep_id in graph2.get(block_id2, set()):
                deps2.add(blocks2[dep_id]['TxnID'])
            
            if deps1 != deps2:
                return False, f"The {i+1} block dependencies of transaction ID {txn_id} are different."
    
    return True, "Transaction block dependencies are the same."

def compare_log_content(entries1, entries2):
    # 比较日志内容
    if len(entries1) != len(entries2):
        return False, f"The number of log entries is different.", []
    
    diff_count = 0
    diff_details = []
    
    # 按事务ID排序后比较
    sorted_entries1 = sorted(entries1, key=lambda x: x['TxnID'])
    sorted_entries2 = sorted(entries2, key=lambda x: x['TxnID'])
    
    for i, (e1, e2) in enumerate(zip(sorted_entries1, sorted_entries2)):
        if e1['raw'] != e2['raw']:
            diff_count += 1
            if diff_count <= 10:  
                diff_details.append({
                    "index": i,
                    "file1": e1,
                    "file2": e2
                })
    
    if diff_count > 0:
        return False, f"The number of differences is {diff_count}", diff_details
    
    return True, "The log content is completely the same.", []

def compare_transaction_blocks(blocks1, blocks2):
    # 比较事务块
    # 按事务ID分组
    blocks_by_txn1 = defaultdict(list)
    for block in blocks1:
        blocks_by_txn1[block['TxnID']].append(block)
    
    blocks_by_txn2 = defaultdict(list)
    for block in blocks2:
        blocks_by_txn2[block['TxnID']].append(block)
    
    # 比较每个事务ID的块
    all_txn_ids = set(blocks_by_txn1.keys()) | set(blocks_by_txn2.keys())
    diff_count = 0
    diff_details = []
    
    for txn_id in sorted(all_txn_ids):
        txn_blocks1 = blocks_by_txn1.get(txn_id, [])
        txn_blocks2 = blocks_by_txn2.get(txn_id, [])
        
        if len(txn_blocks1) != len(txn_blocks2):
            diff_count += 1
            if diff_count <= 10:
                diff_details.append({
                    "type": "block_count",
                    "txn_id": txn_id,
                    "file1_count": len(txn_blocks1),
                    "file2_count": len(txn_blocks2)
                })
            continue
        
        # 比较每个块的内容
        for i, (block1, block2) in enumerate(zip(txn_blocks1, txn_blocks2)):
            if len(block1['Entries']) != len(block2['Entries']):
                diff_count += 1
                if diff_count <= 10:
                    diff_details.append({
                        "type": "entry_count",
                        "txn_id": txn_id,
                        "block_index": i,
                        "file1_count": len(block1['Entries']),
                        "file2_count": len(block2['Entries'])
                    })
                continue
            
            # 比较每个条目的内容
            for j, (entry1, entry2) in enumerate(zip(block1['Entries'], block2['Entries'])):
                if entry1['raw'] != entry2['raw']:
                    diff_count += 1
                    if diff_count <= 10:
                        diff_details.append({
                            "type": "entry_content",
                            "txn_id": txn_id,
                            "block_index": i,
                            "entry_index": j,
                            "file1_entry": entry1,
                            "file2_entry": entry2
                        })
    
    if diff_count > 0:
        return False, f"The number of differences is {diff_count}", diff_details
    
    return True, "The transaction blocks are the same.", []
# def def compare_log_content_brute(entries1, entries2)
# diff_count = 0
# for entry1 in log1_entries:       # O(N)
#     found = False
#     for entry2 in log2_entries:   # O(M)
#         if entry1['raw'] == entry2['raw']:
#             found = True
#             break
#     if not found:
#         diff_count += 1

def main():
    file1 = "log_01a.csv"
    file2 = "log_01b.csv"
    

    result_queue = Queue()
    
    thread1 = threading.Thread(target=parse_log_file, args=(file1, result_queue))
    thread2 = threading.Thread(target=parse_log_file, args=(file2, result_queue))
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()
    
    # 收集结果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # 检查是否有错误
    errors = [r for r in results if r['error']]
    if errors:
        for error in errors:
            print(f"Error processing {error['filename']}: {error['error']}")
        return
    
    # 提取解析结果
    file1_data = next(r for r in results if r['filename'] == file1)
    file2_data = next(r for r in results if r['filename'] == file2)
    
    txns1 = file1_data['transactions']
    blocks1 = file1_data['blocks']
    entries1 = file1_data['entries']
    graph1 = file1_data['graph']
    
    txns2 = file2_data['transactions']
    blocks2 = file2_data['blocks']
    entries2 = file2_data['entries']
    graph2 = file2_data['graph']
    
    # 比较事务块数量和结构
    print(f"\nComparing the constructure of transaction-blocks:")
    is_same_blocks, reason_blocks, details_blocks = compare_transaction_blocks(blocks1, blocks2)
    if not is_same_blocks:
        print(f"{reason_blocks}")
        print("The differences of transaction-blocks:")
        for diff in details_blocks:
            if diff["type"] == "block_count":
                print(f" TxnID {diff['txn_id']}: log1 block counts:{diff['file1_count']}, log2 block counts:{diff['file2_count']}")
            elif diff["type"] == "entry_count":
                print(f" TxnID {diff['txn_id']} block{diff['block_index']+1}: log1 counts {diff['file1_count']}; log2 counts {diff['file2_count']}")
            elif diff["type"] == "entry_content":
                print(f" TxnID {diff['txn_id']} block{diff['block_index']+1} entry{diff['entry_index']+1} differs:")
                print(f"    log1: {diff['file1_entry']['raw']}")
                print(f"    log2: {diff['file2_entry']['raw']}")
    else:
        print(f"{reason_blocks}")
    
    # 比较日志内容
    print(f"\nComparing log content:")
    is_same_content, reason_content, details_content = compare_log_content(entries1, entries2)
    if not is_same_content:
        print(f"{reason_content}")
        if details_content:
            print("Details of the discrepancies in the log content")
            for diff in details_content:
                print(f"    log1: {diff['file1']['raw']}")
                print(f"    log2: {diff['file2']['raw']}")
    else:
        print(f"{reason_content}")
    
    # 比较依赖关系
    print(f"\nComparing log dependency:")
    is_same_deps, reason_deps = compare_block_dependency_graphs(graph1, blocks1, graph2, blocks2)
    if not is_same_deps:
        print(f"{reason_deps}")
        
        # 找出不同的依赖关系 (按事务ID分组)
        txn_ids1 = set(block['TxnID'] for block in blocks1)
        txn_ids2 = set(block['TxnID'] for block in blocks2)
        all_txn_ids = txn_ids1 | txn_ids2
        
        print("Dependency Differences by Transaction ID:")
        for txn_id in sorted(all_txn_ids):
            # 获取该事务ID的所有块
            blocks_in_txn1 = [b for b in blocks1 if b['TxnID'] == txn_id]
            blocks_in_txn2 = [b for b in blocks2 if b['TxnID'] == txn_id]
            
            if len(blocks_in_txn1) != len(blocks_in_txn2):
                print(f"TxnID {txn_id}: log1 block counts:{len(blocks_in_txn1)}, log2 block counts:{len(blocks_in_txn2)}")
                continue
            
            # 比较每个块的依赖
            for i in range(min(len(blocks_in_txn1), len(blocks_in_txn2))):
                block1 = blocks_in_txn1[i]
                block2 = blocks_in_txn2[i]
                
                block_id1 = block1['BlockID']
                block_id2 = block2['BlockID']
                
                deps1 = {blocks1[dep_id]['TxnID'] for dep_id in graph1.get(block_id1, set())}
                deps2 = {blocks2[dep_id]['TxnID'] for dep_id in graph2.get(block_id2, set())}
                
                if deps1 != deps2:
                    print(f"TxnID {txn_id} block{i+1}:")
                    print(f"  log1 dependencies: {sorted(deps1)}")
                    print(f"  log2 dependencies: {sorted(deps2)}")
    else:
        print(f"{reason_deps}")
    
    # 对比结果
    if is_same_blocks and is_same_content and is_same_deps:
        print("\nLogs are completely the same.")
    else:
        print("\nThere is a difference between the two log files:")
        if not is_same_blocks:
            print(f"- Transaction block constructure: {reason_blocks}")
        if not is_same_content:
            print(f"- Log content: {reason_content}")
        if not is_same_deps:
            print(f"- Transaction dependencies: {reason_deps}")

if __name__ == "__main__":
    main()