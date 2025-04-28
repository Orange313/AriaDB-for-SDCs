import csv
class TrieNode:
    def __init__(self):
        self.children = {}  
        self.value = None
        self.lsn = -1  

class PrefixTree:
    def __init__(self):
        self.root = TrieNode()
    
    def insert(self, lsn, record):
        table_id = record['TableID']
        partition_id = record['PartitionID']
        key = record['Key']
        value = record['Value']
        
        if table_id not in self.root.children:
            self.root.children[table_id] = TrieNode()
        table_node = self.root.children[table_id]
        
        if partition_id not in table_node.children:
            table_node.children[partition_id] = TrieNode()
        partition_node = table_node.children[partition_id]
        
        if key not in partition_node.children:
            partition_node.children[key] = TrieNode()
        key_node = partition_node.children[key]

        if lsn >= key_node.lsn:
            key_node.value = value
            key_node.lsn = lsn
    
    def get_value(self, table_id, partition_id, key):
        node = self.root
        try:
            node = node.children[table_id]
            node = node.children[partition_id]
            node = node.children[key]
            return node.value
        except KeyError:
            return None
        
    def export_to_csv(self, filename):
        print(f"将前缀树数据导出到 {filename}...")
        rows = []
        self.collect_data_for_csv(self.root, [], 0, rows)
        with open(filename, 'w', newline='') as csvfile:
            fieldnames = ['TableID', 'PartitionID', 'Key', 'Value', 'LSN']
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            
            writer.writeheader()
            for row in rows:
                writer.writerow(row)
                
        print(f"成功导出 {len(rows)} 条记录到 {filename}")
        
    def collect_data_for_csv(self, node, path, level, rows):
        if level == 3 and node.value is not None:  # 到达叶子节点
            # 确保路径中有三个元素: TableID, PartitionID, Key
            if len(path) == 3:
                rows.append({
                    'TableID': path[0],
                    'PartitionID': path[1],
                    'Key': path[2],
                    'Value': node.value,
                    'LSN': node.lsn
                })
            return
            
        for key, child in node.children.items():
            new_path = path.copy()
            new_path.append(key)
            self.collect_data_for_csv(child, new_path, level + 1, rows)

def generate_incremental_snapshot_from_bplus_tree(bplus_tree):

    print("start to generate incremental snapshot...")
    prefix_tree = PrefixTree()
    
    all_records = []
    leaf = bplus_tree.root
    while not leaf.is_leaf:
        leaf = leaf.children[0]
     
    record_count = 0
    leaf_count = 0
    while leaf:
        leaf_count += 1
        # print(f"Leaf {leaf_count}: LSN range {leaf.records[0][0]} to {leaf.records[-1][0]}")
        for lsn, record in leaf.records:
            prefix_tree.insert(lsn, record)
            record_count += 1
        leaf = leaf.next_leaf
    print(f"Total {record_count} records processed from {leaf_count} leaves.")

    # for record in all_records:
    #     prefix_tree.insert(record)

    print("Incremental snapshot generated successfully.")
    return prefix_tree


