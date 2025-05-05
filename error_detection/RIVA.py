import hashlib
import time
import csv
import threading
from queue import Queue


ERROR_INJECTION_COUNT = 7
REPEAT_TIMES = 5         

def sort_log_file(input_file, output_file):
    with open(input_file, 'r') as f:
        reader = csv.reader(f)
        header = next(reader)
        sorted_rows = sorted(reader, key=lambda x: int(x[1]))  
    with open(output_file, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerow(header)
        writer.writerows(sorted_rows)

def compute_file_hash(file_path, result_queue):
    try:
        temp_file = f"{file_path}.sorted"
        sort_log_file(file_path, temp_file)
        sha256_hash = hashlib.sha256()
        with open(temp_file, 'rb') as f:
            for byte_block in iter(lambda: f.read(4096), b""):
                sha256_hash.update(byte_block)
        result_queue.put((file_path, sha256_hash.hexdigest(), None))
    except Exception as e:
        result_queue.put((file_path, None, str(e)))

def verify_files_integrity(file1, file2):
    start_time = time.time()
    result_queue = Queue()
    
    thread1 = threading.Thread(target=compute_file_hash, args=(file1, result_queue))
    thread2 = threading.Thread(target=compute_file_hash, args=(file2, result_queue))
    
    thread1.start()
    thread2.start()
    thread1.join()
    thread2.join()
    
    results = {}
    while not result_queue.empty():
        file_path, file_hash, error = result_queue.get()
        if error:
            print(f"Error processing {file_path}: {error}")
            return None, None
        results[file_path] = file_hash
    
    file1_hash = results.get(file1)
    file2_hash = results.get(file2)
    
    print(f"\n{file1} hash1: {file1_hash}")
    print(f"{file2} hash2: {file2_hash}")
    
    is_consistent = file1_hash == file2_hash
    if is_consistent:
        print("Files are consistent.")
    else:
        print("Files are inconsistent.")

    end_time = time.time()
    duration = end_time - start_time
    print(f"Total time: {duration:.4f} seconds")
    
    return is_consistent, duration

if __name__ == "__main__":
    file1_path = 'log_01a.csv'  
    file2_path = 'log_01b.csv'
    
    total_time = 0
    error_count = 0
    
    print(f"Starting verification (will repeat {REPEAT_TIMES} times)")
    print(f"Assumed error injection count: {ERROR_INJECTION_COUNT}\n")
    
    for i in range(REPEAT_TIMES):
        print(f"Run {i+1}:")
        consistent, duration = verify_files_integrity(file1_path, file2_path)
        total_time += duration
        if not consistent:
            error_count += 1
        print()
    
    # 计算统计结果
    avg_time = total_time / REPEAT_TIMES
    error_count = error_count/ REPEAT_TIMES
    detection_rate = error_count / ERROR_INJECTION_COUNT 
    detection_rate = min(detection_rate, 100)  
    
    print("\n统计结果:")
    print(f"平均检测时间: {avg_time:.4f} seconds")
    print(f"错误个数: {error_count}")
    print(f"错误检出率: {detection_rate:.2f}%")