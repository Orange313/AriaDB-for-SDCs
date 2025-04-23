# from collections import defaultdict

# def parse_log_file(filename):
#     """解析日志文件，返回事务列表和日志条目（忽略LSN）"""
#     transactions = defaultdict(list)
#     all_entries = []
    
#     with open(filename, 'r') as f:
#         is_header = True  # 标记第一行为标题行
#         for line in f:
#             line = line.strip()
#             if not line:
#                 continue
                
#             # 跳过标题行
#             if is_header:
#                 is_header = False
#                 continue
                
#             parts = line.split(',')
#             if len(parts) < 6:
#                 continue
#             # 忽略LSN，直接从TxnID开始
#             txn_id, table_id, partition_id, key, value = parts[1:6]
            
#             try:
#                 entry = {   
#                     'TxnID': int(txn_id),
#                     'TableID': int(table_id),
#                     'PartitionID': int(partition_id),
#                     'Key': key,
#                     'Value': value,
#                     'raw': ','.join(parts[1:])  # 忽略LSN的原始行
#                 }
#                 transactions[int(txn_id)].append(entry)
#                 all_entries.append(entry)
#             except ValueError as e:
#                 print(f"警告：无法解析行：{line}，错误：{e}")
#                 continue
    
#     return transactions, all_entries

# def build_dependency_graph(transactions, all_entries):
#     """构建事务依赖关系图"""
#     key_to_txns = defaultdict(list)
#     txn_order = {}
    
#     # 第一次遍历：确定事务顺序和每个事务修改的键
#     txn_to_keys = defaultdict(set)
#     for entry in all_entries:
#         txn_id = entry['TxnID']
#         key = entry['Key']
#         txn_to_keys[txn_id].add(key)
#         if txn_id not in txn_order:
#             txn_order[txn_id] = len(txn_order)
    
#     # 第二次遍历：建立依赖关系
#     dependency_graph = defaultdict(set)
#     for entry in all_entries:
#         txn_id = entry['TxnID']
#         key = entry['Key']
        
#         # 对于当前事务修改的键，检查之前修改过这个键的事务
#         for prev_txn in key_to_txns[key]:
#             if prev_txn != txn_id and txn_order[prev_txn] < txn_order[txn_id]:
#                 dependency_graph[txn_id].add(prev_txn)
        
#         key_to_txns[key].append(txn_id)
    
#     return dependency_graph

# def compare_dependency_graphs(graph1, graph2):
#     """比较两个依赖关系图是否相同"""
#     if set(graph1.keys()) != set(graph2.keys()):
#         return False
    
#     for txn_id in graph1:
#         if graph1[txn_id] != graph2[txn_id]:
#             return False
    
#     return True

# def compare_log_content(entries1, entries2):
#     """比较排序后的日志内容是否相同（忽略LSN）"""
#     if len(entries1) != len(entries2):
#         return False
    
#     for e1, e2 in zip(entries1, entries2):
#         if e1['raw'] != e2['raw']:
#             return False
    
#     return True

# def main():
#     # 直接指定要比较的两个文件
#     file1 = "log1_with_sdc_33021.csv"
#     file2 = "log2_33021.csv"
    
#     try:
#         # 解析第一个日志文件
#         txns1, entries1 = parse_log_file(file1)
#         graph1 = build_dependency_graph(txns1, entries1)
        
#         # 解析第二个日志文件
#         txns2, entries2 = parse_log_file(file2)
#         graph2 = build_dependency_graph(txns2, entries2)
        
#         # 比较依赖关系
#         if not compare_dependency_graphs(graph1, graph2):
#             print("事务依赖关系不相同")
#             return
        
#         print("事务依赖关系相同")
        
#         # 按事务ID排序日志条目（忽略LSN）
#         sorted_entries1 = sorted(entries1, key=lambda x: x['TxnID'])
#         sorted_entries2 = sorted(entries2, key=lambda x: x['TxnID'])
        
#         # 比较日志内容
#         if compare_log_content(sorted_entries1, sorted_entries2):
#             print("日志内容完全相同")
#         else:
#             print("日志内容不同")
            
#     except FileNotFoundError as e:
#         print(f"文件未找到: {e}")
#     except Exception as e:
#         print(f"发生错误: {e}")

# if __name__ == "__main__":
#     main()
from collections import defaultdict

def parse_log_file(filename):
    """解析日志文件，返回事务列表、事务块和日志条目（忽略LSN）"""
    transactions = defaultdict(list)
    all_entries = []
    transaction_blocks = []
    current_block = []
    current_txn_id = None
    
    with open(filename, 'r') as f:
        is_header = True  # 标记第一行为标题行
        for line in f:
            line = line.strip()
            if not line:
                continue
                
            # 跳过标题行
            if is_header:
                is_header = False
                continue
                
            parts = line.split(',')
            if len(parts) < 6:
                continue
            # 忽略LSN，直接从TxnID开始
            txn_id, table_id, partition_id, key, value = parts[1:6]
            
            try:
                txn_id = int(txn_id)
                entry = {
                    'TxnID': txn_id,
                    'TableID': int(table_id),
                    'PartitionID': int(partition_id),
                    'Key': key,
                    'Value': value,
                    'raw': ','.join(parts[1:])  # 忽略LSN的原始行
                }
                
                # 如果是新的事务ID或第一条记录，创建新的事务块
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
                print(f"警告：无法解析行：{line}，错误：{e}")
                continue
    
    # 添加最后一个事务块
    if current_block:
        transaction_blocks.append({
            'TxnID': current_txn_id,
            'Entries': current_block
        })
    
    return transactions, transaction_blocks, all_entries

def build_block_dependency_graph(transaction_blocks):
    """构建事务块依赖关系图"""
    key_to_blocks = defaultdict(list)
    block_order = {}
    
    # 第一次遍历：确定事务块顺序和每个事务块修改的键
    block_to_keys = defaultdict(set)
    for i, block in enumerate(transaction_blocks):
        block_id = i  # 使用块索引作为标识符
        block['BlockID'] = block_id
        block_order[block_id] = i
        
        for entry in block['Entries']:
            key = entry['Key']
            block_to_keys[block_id].add(key)
    
    # 第二次遍历：建立依赖关系
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
    """查找依赖图中的环"""
    cycles = []
    
    def dfs(node, path, visited):
        if node in path:
            # 发现环
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
    """比较两个事务块依赖关系图是否相同"""
    if len(blocks1) != len(blocks2):
        return False, "事务块数量不同"
    
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
    
    # 比较每个事务ID的依赖关系
    for txn_id in set(txn_to_block1.keys()) | set(txn_to_block2.keys()):
        blocks_1 = txn_to_block1.get(txn_id, [])
        blocks_2 = txn_to_block2.get(txn_id, [])
        
        if len(blocks_1) != len(blocks_2):
            return False, f"事务ID {txn_id} 的块数量不同: 文件1有{len(blocks_1)}个，文件2有{len(blocks_2)}个"
        
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
                return False, f"事务ID {txn_id} 的第{i+1}个块依赖关系不同"
    
    return True, "事务块依赖关系相同"

def compare_log_content(entries1, entries2):
    """比较排序后的日志内容是否相同（忽略LSN）"""
    if len(entries1) != len(entries2):
        return False, f"日志条目数量不同: 文件1有{len(entries1)}条，文件2有{len(entries2)}条"
    
    diff_count = 0
    diff_details = []
    
    # 按事务ID排序后比较
    sorted_entries1 = sorted(entries1, key=lambda x: x['TxnID'])
    sorted_entries2 = sorted(entries2, key=lambda x: x['TxnID'])
    
    for i, (e1, e2) in enumerate(zip(sorted_entries1, sorted_entries2)):
        if e1['raw'] != e2['raw']:
            diff_count += 1
            if diff_count <= 10:  # 只显示前10个差异
                diff_details.append({
                    "index": i,
                    "file1": e1,
                    "file2": e2
                })
    
    if diff_count > 0:
        return False, f"日志内容有{diff_count}处不同", diff_details
    
    return True, "日志内容完全相同", []

def compare_transaction_blocks(blocks1, blocks2):
    """比较事务块的结构和内容"""
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
        return False, f"事务块内容有{diff_count}处不同", diff_details
    
    return True, "事务块内容完全相同", []

def main():
    # 直接指定要比较的两个文件
    file1 = "log1_with_sdc_33021.csv"
    file2 = "log2_33021.csv"
    
    try:
        # 解析第一个日志文件
        txns1, blocks1, entries1 = parse_log_file(file1)
        graph1, blocks1 = build_block_dependency_graph(blocks1)
        
        # 解析第二个日志文件
        txns2, blocks2, entries2 = parse_log_file(file2)
        graph2, blocks2 = build_block_dependency_graph(blocks2)
        
        # 比较事务块数量和结构
        print(f"\n比较事务块结构:")
        is_same_blocks, reason_blocks, details_blocks = compare_transaction_blocks(blocks1, blocks2)
        if not is_same_blocks:
            print(f"{reason_blocks}")
            print("事务块差异详情:")
            for diff in details_blocks:
                if diff["type"] == "block_count":
                    print(f"  事务ID {diff['txn_id']}: 文件1有{diff['file1_count']}个块，文件2有{diff['file2_count']}个块")
                elif diff["type"] == "entry_count":
                    print(f"  事务ID {diff['txn_id']} 的第{diff['block_index']+1}个块: 文件1有{diff['file1_count']}个条目，文件2有{diff['file2_count']}个条目")
                elif diff["type"] == "entry_content":
                    print(f"  事务ID {diff['txn_id']} 的第{diff['block_index']+1}个块的第{diff['entry_index']+1}个条目不同:")
                    print(f"    文件1: {diff['file1_entry']['raw']}")
                    print(f"    文件2: {diff['file2_entry']['raw']}")
        else:
            print(f"{reason_blocks}")
        
        # 比较日志内容
        print(f"\n比较日志内容:")
        is_same_content, reason_content, details_content = compare_log_content(entries1, entries2)
        if not is_same_content:
            print(f"{reason_content}")
            if details_content:
                print("日志内容差异详情:")
                for diff in details_content:
                    print(f"  第{diff['index']+1}条:")
                    print(f"    文件1: {diff['file1']['raw']}")
                    print(f"    文件2: {diff['file2']['raw']}")
        else:
            print(f"{reason_content}")
        
        # 比较依赖关系
        print(f"\n比较事务块依赖关系:")
        is_same_deps, reason_deps = compare_block_dependency_graphs(graph1, blocks1, graph2, blocks2)
        if not is_same_deps:
            print(f"{reason_deps}")
            
            # 找出不同的依赖关系 (按事务ID分组)
            txn_ids1 = set(block['TxnID'] for block in blocks1)
            txn_ids2 = set(block['TxnID'] for block in blocks2)
            all_txn_ids = txn_ids1 | txn_ids2
            
            print("依赖关系差异(按事务ID):")
            for txn_id in sorted(all_txn_ids):
                # 获取该事务ID的所有块
                blocks_in_txn1 = [b for b in blocks1 if b['TxnID'] == txn_id]
                blocks_in_txn2 = [b for b in blocks2 if b['TxnID'] == txn_id]
                
                if len(blocks_in_txn1) != len(blocks_in_txn2):
                    print(f"事务ID {txn_id}: 文件1有{len(blocks_in_txn1)}个块，文件2有{len(blocks_in_txn2)}个块")
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
                        print(f"事务ID {txn_id} 的第{i+1}个块:")
                        print(f"  文件1中依赖的事务: {sorted(deps1)}")
                        print(f"  文件2中依赖的事务: {sorted(deps2)}")
        else:
            print(f"{reason_deps}")
        
        # 总结对比结果
        print("\n对比总结:")
        if is_same_blocks and is_same_content and is_same_deps:
            print("两个日志文件完全相同")
        else:
            print("两个日志文件存在差异:")
            if not is_same_blocks:
                print(f"- 事务块结构: {reason_blocks}")
            if not is_same_content:
                print(f"- 日志内容: {reason_content}")
            if not is_same_deps:
                print(f"- 事务依赖关系: {reason_deps}")
        
    except FileNotFoundError as e:
        print(f"文件未找到: {e}")
    except Exception as e:
        print(f"发生错误: {e}")

if __name__ == "__main__":
    main()