from Bplustree.tree import build_bplus_tree_from_csv
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bplus_tree
from snapshot_detection.tree_hash import generate_partition_hashes

if __name__ == "__main__":

    bplus_tree = build_bplus_tree_from_csv("log2_33021.csv")
    print("Bplus tree generated successfully.")

    prefix_tree = generate_incremental_snapshot_from_bplus_tree(bplus_tree)
    # output_csv_filename = "prefix_tree_data.csv"
    # prefix_tree.export_to_csv(output_csv_filename)

    hash_output_filename = "partition_hashes2.csv"
    generate_partition_hashes(prefix_tree, hash_output_filename)