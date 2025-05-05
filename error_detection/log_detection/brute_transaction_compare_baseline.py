import threading
from queue import Queue
import time

# 配置参数
ERROR_INJECTION_COUNT = 7
REPEAT_TIMES = 5       

def parse_log_file(filename, result_queue):
    """解析日志文件（仅提取原始内容）"""
    try:
        entries = []
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
                # 只提取TxnID,TableID,PartitionID,Key,Value
                raw_entry = ','.join(parts[1:6])  
                entries.append(raw_entry)
        
        result_queue.put({
            'filename': filename,
            'entries': entries,
            'error': None
        })
    except Exception as e:
        result_queue.put({
            'filename': filename,
            'error': str(e)
        })

def brute_force_compare(entries1, entries2):
    """纯暴力双层循环比对"""
    diff_count = 0
    missing_in_log2 = []
    
    # 第一层循环：遍历log1中的每个条目
    for i, entry1 in enumerate(entries1):
        found = False
        
        # 第二层循环：在log2中寻找匹配项
        for entry2 in entries2:
            if entry1 == entry2:
                found = True
                break
                
        if not found:
            diff_count += 1
            if diff_count <= 10:  # 最多记录10个差异
                missing_in_log2.append((i, entry1))
    
    return diff_count, missing_in_log2

def run_comparison(file1, file2):
    """执行单次比对"""
    start_time = time.time()
    result_queue = Queue()
    
    # 使用双线程解析文件
    thread1 = threading.Thread(target=parse_log_file, args=(file1, result_queue))
    thread2 = threading.Thread(target=parse_log_file, args=(file2, result_queue))
    
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    
    # 获取结果
    results = []
    while not result_queue.empty():
        results.append(result_queue.get())
    
    # 检查错误
    errors = [r for r in results if r['error']]
    if errors:
        for error in errors:
            print(f"Error processing {error['filename']}: {error['error']}")
        return None, None
    
    # 提取数据
    file1_data = next(r for r in results if r['filename'] == file1)
    file2_data = next(r for r in results if r['filename'] == file2)
    entries1 = file1_data['entries']
    entries2 = file2_data['entries']
    
    # 执行暴力比对
    diff_count, missing_entries = brute_force_compare(entries1, entries2)
    
    # 计算指标
    is_same = diff_count == 0
    detection_rate = min(diff_count / ERROR_INJECTION_COUNT, 1.0)
    end_time = time.time()
    duration = end_time - start_time
    
    # 输出结果
    print(f"\n比对结果: 发现{diff_count}处差异")
    if not is_same and missing_entries:
        print("差异详情(最多显示10条):")
        for idx, entry in missing_entries:
            print(f"  log1第{idx+1}条在log2中缺失: {entry}")
    
    print(f"比对耗时: {duration:.4f}秒")
    print(f"错误检出率: {detection_rate*100:.2f}%")
    
    return duration, diff_count

def main():
    file1 = "log_01a.csv"
    file2 = "log_01b.csv"
    
    total_time = 0
    total_errors = 0
    
    print(f"开始暴力双层循环比对测试，共重复{REPEAT_TIMES}次")
    print(f"假设错误注入数量: {ERROR_INJECTION_COUNT}\n")
    
    for i in range(REPEAT_TIMES):
        print(f"第{i+1}次执行:")
        duration, errors = run_comparison(file1, file2)
        if duration is not None:
            total_time += duration
            total_errors += errors
        print()
    
    # 计算统计结果
    avg_time = total_time / REPEAT_TIMES
    avg_errors = total_errors / REPEAT_TIMES
    avg_detection_rate = min(avg_errors / ERROR_INJECTION_COUNT, 1.0)
    
    print("\n最终统计结果:")
    print(f"平均比对时间: {avg_time:.4f}秒")
    print(f"平均检测到错误数: {avg_errors:.2f}")
    print(f"平均错误检出率: {avg_detection_rate*100:.2f}%")
    print(f"总执行次数: {REPEAT_TIMES}")

if __name__ == "__main__":
    main()