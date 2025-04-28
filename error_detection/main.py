from Bplustree.tree import build_bepsilon_tree
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bplus_tree
from snapshot_detection.tree_hash import generate_partition_hashes
from snapshot_detection.snapshot_based_merkle import LayeredHashTree
from snapshot_detection.hash_comparator import compare_trees_bfs, compare_partition_hashes
import threading
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

            bepsilon_tree = build_bepsilon_tree(self.log_file)
            self.prefix_tree = generate_incremental_snapshot_from_bplus_tree(bepsilon_tree)
        except Exception as e:
            self.error = e
            print(f"Error: {e}")

def main():
    results = []
    builder1 = TreeBuilder("log1_with_sdc_33021.csv")
    builder2 = TreeBuilder("log2_33021.csv")
    
    # 创建并启动线程
    print('Strating...')
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
        return
    
    tree1 = results[0].prefix_tree
    tree2 = results[1].prefix_tree

    # print("正在生成哈希值...")
    # hash_output_filename1 = "partition_hashes1.csv"
    # hash_output_filename2 = "partition_hashes2.csv"
    print("Generating hash...")
    generate_partition_hashes(tree1)
    generate_partition_hashes(tree2)
    
    # 比较两棵树
    print("\nStart to compare two trees...")
    differences = compare_partition_hashes(tree1, tree2)
    
    
    return differences


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

'''多线程'''
# class TreeBuilder:
#     def __init__(self, log_file):
#         self.log_file=log_file
#         self.tree = None
#         self.error = None

#     def build(self):
#         try:
#             b_epsilon_tree = build_bepsilon_tree(self.log_file)
#             hash_tree = LayeredHashTree()
#             hash_tree.build_from_b_epsilon_tree(b_epsilon_tree)
#             self.tree = hash_tree
#         except Exception as e:
#             self.error = e
    
# def main():
#     results = []
    
#     builder1 = TreeBuilder("log1_with_sdc_33021.csv")
#     builder2 = TreeBuilder("log2_33021.csv")
    
#     # 创建并启动线程
#     print("Start...")
#     thread1 = threading.Thread(target=builder1.build)
#     thread2 = threading.Thread(target=builder2.build)
    
#     thread1.start()
#     thread2.start()
    
#     thread1.join()
#     thread2.join()
    
#     results.extend([builder1, builder2])
    
#     # 检查构建结果
#     if len(results) != 2:
#         print("Error: Failed to  build two trees!")
#         exit(1)
    
#     if results[0].error or results[1].error:
#         print("An error occurred during the build process!")
#         if results[0].error:
#             print(f"NodeA Error: {results[0].error}")
#         if results[1].error:
#             print(f"NodeB Error: {results[1].error}")
#         exit(1)

#     tree1 = results[0].tree
#     tree2 = results[1].tree
    
#     # 比较两棵树
#     print("\nBegin to compare...")
#     compare_trees_bfs(tree1, tree2)


if __name__ == "__main__":
    main()