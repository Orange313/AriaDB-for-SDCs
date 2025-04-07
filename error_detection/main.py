from Bplustree.tree import build_bplus_tree_from_csv
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bplus_tree

if __name__ == "__main__":

    bplus_tree = build_bplus_tree_from_csv("log1_with_sdc_33021.csv")
    print("Bplus tree generated successfully.")

    prefix_tree = generate_incremental_snapshot_from_bplus_tree(bplus_tree)
    output_csv_filename = "prefix_tree_data.csv"
    prefix_tree.export_to_csv(output_csv_filename)
