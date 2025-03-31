
class TrieNode:
    def __init__(self):
        self.children = {}  
        self.value = None  

class PrefixTree:
    def __init__(self):
        self.root = TrieNode()
    
    def insert(self, record):
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
        
        key_node.value = value
    
    def get_value(self, table_id, partition_id, key):
        node = self.root
        try:
            node = node.children[table_id]
            node = node.children[partition_id]
            node = node.children[key]
            return node.value
        except KeyError:
            return None

def generate_incremental_snapshot_from_bplus_tree(bplus_tree):

    print("start to generate incremental snapshot...")
    prefix_tree = PrefixTree()
    
    all_records = []
    leaf = bplus_tree.root
    while not leaf.is_leaf:
        leaf = leaf.children[0]
    
    # record_count = 0
    # while leaf:
    #     for lsn, record in leaf.records:
    #         all_records.append(record)
    #         record_count += 1
    #     leaf = leaf.next_leaf
    # print(f"Total {record_count} records processed.")    
    record_count = 0
    leaf_count = 0
    while leaf:
        leaf_count += 1
        print(f"Leaf {leaf_count}: LSN range {leaf.records[0][0]} to {leaf.records[-1][0]}")
        for lsn, record in leaf.records:
            all_records.append(record)
            record_count += 1
        leaf = leaf.next_leaf
    print(f"Total {record_count} records processed from {leaf_count} leaves.")

    for record in all_records:
        prefix_tree.insert(record)

    print("Incremental snapshot generated successfully.")
    return prefix_tree


