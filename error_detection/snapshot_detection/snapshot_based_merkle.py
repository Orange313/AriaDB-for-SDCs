import hashlib
import xxhash
from typing import Dict, Any, Optional, List, Tuple

class LayeredHashTree:
    
    def __init__(self):
        self.root = None
        self.data = {} 

    def build_from_b_epsilon_tree(self, b_epsilon_tree):
        self.collect_latest_records(b_epsilon_tree)
        self.construct_hash_tree()
    
    def collect_latest_records(self, b_epsilon_tree):
        leaf = b_epsilon_tree.find_leftmost_leaf()
        while leaf:
            for lsn, record in leaf.records:
                composite_key = (
                    str(record['TableID']),
                    str(record['PartitionID']),
                    str(record['Key'])
                )
                # 只保留LSN最大的记录
                if (composite_key not in self.data or 
                    lsn > self.data[composite_key][1]):
                    self.data[composite_key] = (record['Value'], lsn)
            leaf = leaf.next_leaf
    
    def construct_hash_tree(self):
        # 第一层：组织原始数据
        tables = {}
        for (table_id, partition_id, key), (value, _) in self.data.items():
            if table_id not in tables:
                tables[table_id] = {}
            if partition_id not in tables[table_id]:
                tables[table_id][partition_id] = {}
            tables[table_id][partition_id][key] = value
        
        # 第二层：构建KeyValue节点
        kv_nodes = {}
        for table_id, partitions in tables.items():
            for partition_id, keys in partitions.items():
                for key, value in keys.items():
                    kv_nodes[(table_id, partition_id, key)] = KeyValueNode(key, value)
        
        # 第三层：构建Partition节点
        partition_nodes = {}
        for table_id, partitions in tables.items():
            partition_nodes[table_id] = {}
            for partition_id, keys in partitions.items():
                partition_kv = {
                    k: kv_nodes[(table_id, partition_id, k)]
                    for k in keys
                }
                partition_nodes[table_id][partition_id] = PartitionNode(partition_id, partition_kv)
        
        # 第四层：构建Table节点
        table_nodes = {
            tid: TableNode(tid, partitions)
            for tid, partitions in partition_nodes.items()
        }
        
        # 最终构建Root节点
        self.root = RootNode(table_nodes)
    
    def get_all_hashes(self) -> Dict[str, Dict[str, Dict[str, str]]]:
        if not self.root:
            return {}
        
        hash_tree = {
            'root_hash': self.root.hash,
            'tables': {}
        }
        
        # 遍历所有表节点
        for table_id, table_node in self.root.children.items():
            hash_tree['tables'][table_id] = {
                'table_hash': table_node.hash,
                'partitions': {}
            }
            
            # 遍历表下的所有分区节点
            for partition_id, partition_node in table_node.children.items():
                hash_tree['tables'][table_id]['partitions'][partition_id] = {
                    'partition_hash': partition_node.hash,
                    'keys': {k: v.hash for k, v in partition_node.children.items()}
                }
        
        return hash_tree

    def print_hash_tree(self):
        hash_tree = self.get_all_hashes()
        
        print(f"Root Hash: {hash_tree['root_hash']}")
        print("=" * 50)
        
        for table_id, table_data in hash_tree['tables'].items():
            print(f"Table: {table_id}")
            print(f"  Table Hash: {table_data['table_hash']}")
            
            for partition_id, partition_data in table_data['partitions'].items():
                print(f"  Partition: {partition_id}")
                print(f"    Partition Hash: {partition_data['partition_hash']}")
                print(f"    Contained Keys: {len(partition_data['keys'])} keys")
            
            print("-" * 40)

class KeyValueNode:
    # 键值节点（存储原始数据）
    def __init__(self, key: str, value: Any):
        self.key = key
        self.value = value
        self.hash = self.calculate_hash()
    
    def calculate_hash(self):
        # 使用SHA256计算哈希（64字符）
        return hashlib.sha256(f"{self.key}:{self.value}".encode()).hexdigest()

class PartitionNode:
    # 分区节点（组织键值节点）
    def __init__(self, partition_id: str, children: Dict[str, KeyValueNode]):
        self.partition_id = partition_id
        self.children = children  # {key: KeyValueNode}
        self.hash = self.calculate_hash()
    
    def calculate_hash(self):
        # 使用xxhash64计算分区哈希（16字符）
        sorted_hashes = [f"{k}:{v.hash}" for k, v in sorted(self.children.items())]
        content = f"{self.partition_id}:" + ",".join(sorted_hashes)
        return xxhash.xxh64(content.encode()).hexdigest()[:16]

class TableNode:
    # 表节点（组织分区节点）
    def __init__(self, table_id: str, children: Dict[str, PartitionNode]):
        self.table_id = table_id
        self.children = children  # {partition_id: PartitionNode}
        self.hash = self.calculate_hash()
    
    def calculate_hash(self):
        # 使用xxhash64计算表哈希（16字符）
        sorted_hashes = [f"{pid}:{p.hash}" for pid, p in sorted(self.children.items())]
        content = f"{self.table_id}:" + ",".join(sorted_hashes)
        return xxhash.xxh64(content.encode()).hexdigest()#[:16]

class RootNode:
    # 根节点（组织表节点）
    def __init__(self, children: Dict[str, TableNode]):
        self.children = children  # {table_id: TableNode}
        self.hash = self.calculate_hash()
    
    def calculate_hash(self):
        # 使用xxhash32计算根哈希（8字符）
        sorted_hashes = [f"{tid}:{t.hash}" for tid, t in sorted(self.children.items())]
        content = "|".join(sorted_hashes)
        return xxhash.xxh32(content.encode()).hexdigest()#[:8]

