import threading
from collections import defaultdict
from queue import Queue
import time

ERROR_INJECTION_COUNT = 18
REPEAT_TIMES = 5           # 重复执行次数
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
           
            diff_details.append({
                "index": i,
                "file1": e1,
                "file2": e2
            })

    if diff_count > 0:
        return False, f"The number of differences is {diff_count}", diff_details

    return True, "The log content is completely the same.", []

def run_comparison(file1, file2):
    start_time = time.time()
    
    result_queue = Queue()
    thread1 = threading.Thread(target=parse_log_file, args=(file1, result_queue))
    thread2 = threading.Thread(target=parse_log_file, args=(file2, result_queue))

    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()

    results = []
    while not result_queue.empty():
        results.append(result_queue.get())

    errors = [r for r in results if r['error']]
    if errors:
        for error in errors:
            print(f"Error processing {error['filename']}: {error['error']}")
        return None, None, None

    file1_data = next(r for r in results if r['filename'] == file1)
    file2_data = next(r for r in results if r['filename'] == file2)

    entries1 = file1_data['entries']
    entries2 = file2_data['entries']
    blocks1 = file1_data['blocks']
    blocks2 = file2_data['blocks']
    graph1 = file1_data['graph']
    graph2 = file2_data['graph']

    # 比较依赖关系
    is_same_deps, reason_deps = compare_block_dependency_graphs(graph1, blocks1, graph2, blocks2)
    dep_error_count = 0 if is_same_deps else 1 

    if not is_same_deps:
        print("\n发现事务依赖差异:")
        print(reason_deps) 

    # 比较日志内容
    is_same_content, reason_content, details_content = compare_log_content(entries1, entries2)
    content_error_count = 0 if is_same_content else len(details_content)

    total_error_count = content_error_count + dep_error_count
    detection_rate = min(total_error_count / ERROR_INJECTION_COUNT, 1.0)
    
    elapsed_time = time.time() - start_time
    return elapsed_time, detection_rate, total_error_count

def main():
    file1 = "error_detection\data\different_sdc_possibility\log02_3333.csv"
    file2 = "error_detection\data\different_sdc_possibility\log02b.csv"

    total_time = 0
    total_detection_rate = 0
    total_error_detected = 0

    print(f"开始比对测试，共重复{REPEAT_TIMES}次，每次注入错误数: {ERROR_INJECTION_COUNT}")
    
    for i in range(REPEAT_TIMES):
        print(f"\n第 {i+1} 次执行...")
        elapsed_time, detection_rate, error_count = run_comparison(file1, file2)
        
        if elapsed_time is None:
            continue
            
        total_time += elapsed_time
        total_detection_rate += detection_rate
        total_error_detected += error_count
        
        print(f"本次结果 - 耗时: {elapsed_time:.4f}秒, 检出错误: {error_count}个, 错误检出率: {detection_rate*100:.2f}%")

    avg_time = total_time / REPEAT_TIMES
    avg_detection_rate = total_detection_rate / REPEAT_TIMES * 100
    avg_error_detected = total_error_detected / REPEAT_TIMES
    
    print("\n最终平均结果:")
    print(f"平均比对时间: {avg_time:.4f}秒")
    print(f"平均检出错误数: {avg_error_detected:.2f}个")
    print(f"平均错误检出率: {avg_detection_rate:.2f}%")

if __name__ == "__main__":
    main()