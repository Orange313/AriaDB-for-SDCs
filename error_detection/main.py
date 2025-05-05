from Bplustree.tree import build_bepsilon_tree
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bepsilon_tree
from snapshot_detection.tree_hash import generate_partition_hashes
from snapshot_detection.snapshot_based_merkle import LayeredHashTree
from snapshot_detection.hash_comparator import compare_trees_bfs, compare_partition_hashes
import threading
import time

ERROR_INJECTION_COUNT = 4
REPEAT_TIMES = 5   

'''单线程 基础版'''
# if __name__ == "__main__":

#     bepsilon_tree = build_bepsilon_tree("log2_33021.csv")
#     print("Bepsilon tree generated successfully.")

#     prefix_tree = generate_incremental_snapshot_from_bplus_tree(bepsilon_tree)
#     # snapshort_based_merkle_tree = build_from_b_epsilon_tree(bepsilon_tree) #升级版
#     # output_csv_filename = "prefix_tree_data.csv"
#     # prefix_tree.export_to_csv(output_csv_filename)

#     hash_output_filename = "partition_hashes2.csv"
#     generate_partition_hashes(prefix_tree, hash_output_filename)
'''多线程 基础版'''
class TreeBuilder:
    def __init__(self, log_file):
        self.log_file = log_file
        self.prefix_tree = None
        self.error = None
        
    def build(self):
        try:
            start_bepsilon = time.time()
            bepsilon_tree = build_bepsilon_tree(self.log_file)
            end_bepsilon = time.time()
            self.bepsilon_time = end_bepsilon - start_bepsilon
            self.prefix_tree = generate_incremental_snapshot_from_bepsilon_tree(bepsilon_tree)
        except Exception as e:
            self.error = e
            print(f"Error: {e}")

def run_comparison():
    results = []
    
    builder1 = TreeBuilder("error_detection\data\cut\A\log02s_a.csv")
    builder2 = TreeBuilder("error_detection\data\cut\B\log02s_b.csv")
    
    # 创建并启动线程
    thread1 = threading.Thread(target=builder1.build)
    thread2 = threading.Thread(target=builder2.build)
    
    thread1.start()
    thread2.start()
    
    thread1.join()
    thread2.join()
    
    results.extend([builder1, builder2])
    
    # 检查构建结果
    if results[0].error or results[1].error:
        print("Error!")
        if results[0].error:
            print(f"Tree1: {results[0].error}")
        if results[1].error:
            print(f"Tree2: {results[1].error}")
        return None, None, None
    
    bepsilon_time = max(results[0].bepsilon_time, results[1].bepsilon_time)
    start_time = time.time()
    tree1 = results[0].prefix_tree
    tree2 = results[1].prefix_tree

    print("Generating hash...")
    generate_partition_hashes(tree1)
    generate_partition_hashes(tree2)
    
    # 比较两棵树
    print("\nStart to compare two trees...")
    start_time_compare = time.time()
    total_differences = compare_partition_hashes(tree1, tree2)
    end_time = time.time()
    
    total_time = end_time - start_time
    comparison_time = end_time - start_time_compare
    
    # 计算错误检出率
    error_count = len(total_differences) if total_differences else 0
    detection_rate = min(error_count / ERROR_INJECTION_COUNT, 1.0)
    
    print(f"Total time: {total_time:.4f} seconds, Compare time: {comparison_time:.4f} seconds")
    print(f"Detected errors: {error_count}, Detection rate: {detection_rate*100:.2f}%")
    
    return total_time, comparison_time, detection_rate, error_count

def main():
    total_duration = 0
    total_compare_time = 0
    total_detection_rate = 0
    total_error_detected = 0

    print(f"Starting comparison test, will repeat {REPEAT_TIMES} times, error injection count: {ERROR_INJECTION_COUNT}")
    
    for i in range(REPEAT_TIMES):
        print(f"\nRun {i+1}...")
        duration, compare_time, detection_rate, error_count = run_comparison()
        
        if duration is None:
            continue
            
        total_duration += duration
        total_compare_time += compare_time
        total_detection_rate += detection_rate
        total_error_detected += error_count
        
    if REPEAT_TIMES > 0:
        avg_duration = total_duration / REPEAT_TIMES
        avg_compare_time = total_compare_time / REPEAT_TIMES
        avg_detection_rate = total_detection_rate / REPEAT_TIMES * 100
        avg_error_detected = total_error_detected / REPEAT_TIMES
        
        print("\nFinal average results:")
        print(f"Average total time: {avg_duration:.4f} seconds")
        print(f"Average compare time: {avg_compare_time:.4f} seconds")
        print(f"Average detected errors: {avg_error_detected:.2f}")
        print(f"Average detection rate: {avg_detection_rate:.2f}%")




# 单线程 改良版
# def build_tree(log_file):
  
#     b_epsilon_tree = build_bepsilon_tree(log_file)
#     hash_tree = LayeredHashTree()
#     hash_tree.build_from_b_epsilon_tree(b_epsilon_tree)
#     # hash_tree.print_hash_tree()
#     return hash_tree


# if __name__ == "__main__":
#     print("Building tree from log1......")
#     tree1 = build_tree("log1_with_sdc_33021.csv")

#     print("Building tree from log1......")
#     tree2 = build_tree("log1_with_sdc_33021.csv")

'''多线程 改良版'''
# class TreeBuilder:
#     def __init__(self, log_file):
#         self.log_file=log_file
#         self.tree = None
#         self.error = None
#         self.bepsilon_time = 0
#         self.hash_tree_time = 0 

#     def build(self):
#         try:
#             start_bepsilon = time.time()
#             b_epsilon_tree = build_bepsilon_tree(self.log_file)
#             self.bepsilon_time = time.time() - start_bepsilon

#             start_hash_tree = time.time()
#             hash_tree = LayeredHashTree()
#             hash_tree.build_from_b_epsilon_tree(b_epsilon_tree)
#             self.hash_tree_time = time.time() - start_hash_tree

#             self.tree = hash_tree
#         except Exception as e:
#             self.error = e
    
# def run_comparison():
#     results = []
#     builder1 = TreeBuilder("error_detection\data\cut\A\log02s_a.csv")
#     builder2 = TreeBuilder("error_detection\data\cut\B\log02s_b.csv")
    
#     # 创建并启动线程
#     thread1 = threading.Thread(target=builder1.build)
#     thread2 = threading.Thread(target=builder2.build)
    
#     thread1.start()
#     thread2.start()
    
#     thread1.join()
#     thread2.join()
    
#     results.extend([builder1, builder2])
    
#     if len(results) != 2:
#         print("Error: Failed to build two trees!")
#         return None, None, None, None, None, None
    
#     if results[0].error or results[1].error:
#         print("An error occurred during the build process!")
#         if results[0].error:
#             print(f"NodeA Error: {results[0].error}")
#         if results[1].error:
#             print(f"NodeB Error: {results[1].error}")
#         return None, None, None, None, None, None

#     max_bepsilon_time = max(results[0].bepsilon_time, results[1].bepsilon_time)
#     max_hash_tree_time = max(results[0].hash_tree_time, results[1].hash_tree_time)
    
#     start_compare = time.time()
    
#     tree1 = results[0].tree
#     tree2 = results[1].tree
    
#     # 比较两棵树
#     print("\nBegin to compare...")
#     differences,differences['partitions'] = compare_trees_bfs(tree1, tree2)
#     compare_time = time.time() - start_compare
    
#     # 计算总时间（不包括树构建）
#     # total_time_without_build = compare_time
#     # 计算总时间（包括树构建）
#     total_time =  max_hash_tree_time + compare_time
    
#     # 计算错误检出率
#     error_count = len(differences['partitions']) if len(differences['partitions']) else 0
#     detection_rate = min(error_count / ERROR_INJECTION_COUNT, 1.0)
    
#     print("\nTime breakdown:")
#     # print(f"B-epsilon tree construction: {max_bepsilon_time:.4f} seconds")
#     # print(f"Hash tree construction: {max_hash_tree_time:.4f} seconds")
#     print(f"Tree comparison: {compare_time:.4f} seconds")
#     # print(f"Total time (without tree build): {total_time_without_build:.4f} seconds")
#     print(f"Total time (with tree build): {total_time:.4f} seconds")
#     print(f"Detected errors: {error_count}, Detection rate: {detection_rate*100:.2f}%")
    
#     return (total_time, compare_time, 
#             detection_rate, error_count)

# def main():
#     total_duration = 0
#     total_compare_time = 0
#     total_detection_rate = 0
#     total_error_detected = 0
#     # total_bepsilon_time = 0
#     # total_hash_tree_time = 0

#     print(f"Starting comparison test, will repeat {REPEAT_TIMES} times, error injection count: {ERROR_INJECTION_COUNT}")
    
#     for i in range(REPEAT_TIMES):
#         print(f"\nRun {i+1}...")
#         result = run_comparison()
        
#         if result is None:
#             continue
            
#         (duration, compare_time, detection_rate, error_count) = result
        
#         total_duration += duration
#         total_compare_time += compare_time
#         total_detection_rate += detection_rate
#         total_error_detected += error_count
#         # total_bepsilon_time += bepsilon_time
#         # total_hash_tree_time += hash_tree_time
        
#     if REPEAT_TIMES > 0:
#         avg_duration = total_duration / REPEAT_TIMES
#         avg_compare_time = total_compare_time / REPEAT_TIMES
#         avg_detection_rate = total_detection_rate / REPEAT_TIMES * 100
#         avg_error_detected = total_error_detected / REPEAT_TIMES
#         # avg_bepsilon_time = total_bepsilon_time / REPEAT_TIMES
#         # avg_hash_tree_time = total_hash_tree_time / REPEAT_TIMES
        
#         print("\nFinal average results:")
#         # print(f"Average B-epsilon construction time: {avg_bepsilon_time:.4f} seconds")
#         # print(f"Average hash tree construction time: {avg_hash_tree_time:.4f} seconds")
#         print(f"Average comparison time: {avg_compare_time:.4f} seconds")
#         print(f"Average total time (without tree build): {avg_duration:.4f} seconds")
#         print(f"Average detected errors: {avg_error_detected:.2f}")
#         print(f"Average detection rate: {avg_detection_rate:.2f}%")


if __name__ == "__main__":
    main()