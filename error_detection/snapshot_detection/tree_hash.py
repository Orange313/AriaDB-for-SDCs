import hashlib
import csv

def calculate_node_hash(node):

    hasher = hashlib.sha256()
    if hasattr(node, 'value') and node.value is not None:
        hasher.update(str(node.value).encode())
    if hasattr(node, 'children') and node.children:
        sorted_keys = sorted(node.children.keys())
        for key in sorted_keys:
            child_hash = calculate_node_hash(node.children[key])
            hasher.update(str(key).encode())
            hasher.update(child_hash.encode())
    
    return hasher.hexdigest()

def collect_partition_hashes(prefix_tree):

    partition_hashes = []
    for table_id, table_node in prefix_tree.root.children.items():
        for partition_id, partition_node in table_node.children.items():
            hash_value = calculate_node_hash(partition_node)
            partition_hashes.append((table_id, partition_id, hash_value))
    
    return partition_hashes

def write_hashes_to_file(partition_hashes, filename='partition_hashes.csv'):
    with open(filename, 'w', newline='') as csvfile:
        writer = csv.writer(csvfile)
        writer.writerow(['TableID', 'PartitionID', 'HashValue'])
        for table_id, partition_id, hash_value in partition_hashes:
            writer.writerow([table_id, partition_id, hash_value])
    
    print(f"分区哈希值已写入文件: {filename}")
    return filename

def generate_partition_hashes(prefix_tree, output_filename='partition_hashes.csv'):

    print("Generating hash...")
    partition_hashes = collect_partition_hashes(prefix_tree)
    return write_hashes_to_file(partition_hashes, output_filename)