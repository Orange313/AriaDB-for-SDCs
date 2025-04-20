from Bplustree.tree import build_bepsilon_tree_from_csv
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bplus_tree
from snapshot_detection.tree_hash import generate_partition_hashes
from snapshot_detection.snapshot_based_merkle import LayeredHashTree

# if __name__ == "__main__":

#     bepsilon_tree = build_bepsilon_tree_from_csv("log2_33021.csv")
#     print("Bepsilon tree generated successfully.")

#     prefix_tree = generate_incremental_snapshot_from_bplus_tree(bepsilon_tree)
#     # snapshort_based_merkle_tree = build_from_b_epsilon_tree(bepsilon_tree) #升级版
#     # output_csv_filename = "prefix_tree_data.csv"
#     # prefix_tree.export_to_csv(output_csv_filename)

#     hash_output_filename = "partition_hashes2_md5.csv"
#     generate_partition_hashes(prefix_tree, hash_output_filename)

if __name__ == "__main__":
  
    b_tree = build_bepsilon_tree_from_csv("log1_33021.csv")
   
    hash_tree = LayeredHashTree()
    hash_tree.build_from_b_epsilon_tree(b_tree)
    hash_tree.print_hash_tree()