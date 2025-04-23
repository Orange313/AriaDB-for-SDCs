#tree.py
import pandas as pd
from Bplustree.node import LeafNode, InternalNode
# from node import LeafNode, InternalNode

DEFAULT_ORDER = 256 #256
DEFAULT_LEAF_SIZE = 512 #512
DEFAULT_BUFFER_SIZE = 512 #512

class BEpsilonTree:
    def __init__(self, order=DEFAULT_ORDER, leaf_size=DEFAULT_LEAF_SIZE, buffer_size=DEFAULT_BUFFER_SIZE):
        self.order = order
        self.leaf_size = leaf_size
        self.buffer_size = buffer_size
        self.root = LeafNode(leaf_size)
        self.insert_count = 0
        # self.stats={
        #     "leaf_nodes":0,
        #     "internal_nodes":0,
        #     "height": 1
        # }

    def insert(self, lsn, record):
        self.insert_count += 1
        node = self.root
        if node.is_leaf:
            node.insert_record(lsn, record)
            if node.is_full():
                new_leaf = node.split()
                new_root = InternalNode(self.order, self.buffer_size)
                new_root.children = [node, new_leaf]
                new_root.keys = [new_leaf.records[0][0]] # 第一个键值对的key
                node.parent = new_root
                new_leaf.parent = new_root
                self.root = new_root
        else:
            node.insert_into_buffer(lsn, record)
            if node.is_full():
                new_root = node.split_self()
                if new_root:
                    self.root = new_root

    def flush_all(self):
        if not self.root.is_leaf:
            self.root.flush_all_buffers()

    def search(self, lsn):
        self.flush_all()
        node = self.root
        # if not node.is_leaf:
        #     node.flush_buffer()
        while not node.is_leaf:
            # node.flush_buffer()
            idx = node.find_child_index(lsn)
            node = node.children[idx]
        result = []
        for key, rec in node.records:
            if key == lsn:
                result.append(rec)
        return result

    def range_search(self, start_lsn, end_lsn):
        self.flush_all()
        results = []
        node = self.root
        # if not node.is_leaf:
        #     node.flush_buffer()
        while not node.is_leaf:
            # node.flush_buffer()
            idx = node.find_child_index(start_lsn)
            node = node.children[idx]
        while node:
            for key, rec in node.records:
                if start_lsn <= key <= end_lsn:
                    results.append((key, rec))
                elif key > end_lsn:
                    return results
            node = node.next_leaf
        return results
    
    def robust_search(self, lsn):
        self.flush_all()
        results = []
        
        # 遍历所有叶子节点
        leaf = self.find_leftmost_leaf()
        while leaf:
            for key, rec in leaf.records:
                if key == lsn:
                    results.append(rec)
            leaf = leaf.next_leaf
        
        return results
    
    
    #统计一些树的信息
    def get_stats(self):
        leaf_count = 0
        leaf = self.find_leftmost_leaf()
        while leaf:
            leaf_count += 1
            leaf = leaf.next_leaf
        
        self.stats["leaf_nodes"] = leaf_count
        self.stats["record_count"] = self._count_records()
        
        return {
            "total_inserts": self.insert_count,
            "actual_records": self._count_records(),
            "height": self.stats["height"],
            "leaf_nodes": self.stats["leaf_nodes"],
            "internal_nodes": self.stats["internal_nodes"]
        }
    def _count_records(self):
        """计算树中实际存储的记录数"""
        count = 0
        leaf = self.find_leftmost_leaf()
        while leaf:
            count += len(leaf.records)
            leaf = leaf.next_leaf
        return count
    
    def find_leftmost_leaf(self):
        """查找最左侧的叶子节点"""
        node = self.root
        while not node.is_leaf:
            node = node.children[0]
        return node
    
    def validate(self):
        """验证B+树结构的正确性"""
        # 验证叶子节点链表的完整性
        leaf = self.find_leftmost_leaf()
        prev_max_lsn = -1
        
        while leaf:
            # 检查记录是否按LSN排序
            for i in range(1, len(leaf.records)):
                if leaf.records[i-1][0] > leaf.records[i][0]:
                    return False, f"叶子节点中的记录未排序: {leaf.records[i-1][0]} > {leaf.records[i][0]}"
            
            # 检查节点间的LSN顺序
            if leaf.records and prev_max_lsn != -1:
                if prev_max_lsn > leaf.records[0][0]:
                    return False, f"叶子节点间的LSN不连续: {prev_max_lsn} > {leaf.records[0][0]}"
            
            if leaf.records:
                prev_max_lsn = leaf.records[-1][0]
            
            leaf = leaf.next_leaf
        
        return True, "B+树结构验证通过"
    
    def fix_leaf_chain(self):
        all_leaves = []
        current = self.find_leftmost_leaf()
        visited = set()
        # 防止循环引用导致无限循环
        while current and id(current) not in visited:
            visited.add(id(current))
            all_leaves.append(current)
            next_node = current.next_leaf
            current.next_leaf = None  # 断开所有链接
            current = next_node
        # 按LSN排序所有叶子节点
        all_leaves.sort(key=lambda leaf: leaf.records[0][0] if leaf.records else float('inf'))   
        # 重新链接叶子节点
        for i in range(len(all_leaves) - 1):
            all_leaves[i].next_leaf = all_leaves[i + 1]
        
        return True

def build_bepsilon_tree_from_csv(csv_path, order=DEFAULT_ORDER, leaf_size=DEFAULT_LEAF_SIZE, buffer_size=DEFAULT_BUFFER_SIZE):
    df = pd.read_csv(csv_path)
    print(f"csv length: {len(df)}.")
    #df.sort_values(by='LSN', inplace=True)
    tree = BEpsilonTree(order=order, leaf_size=leaf_size, buffer_size=buffer_size)
    for _, row in df.iterrows():
        lsn = int(row['LSN'])
        record = {
            'TxnID': row['TxnID'],
            'TableID': row['TableID'],
            'PartitionID': row['PartitionID'],
            'Key': row['Key'],
            'Value': row['Value']
        }
        tree.insert(lsn, record)

    tree.flush_all()

    valid, message = tree.validate()
    if not valid:
        print(f"警告: {message}")
    
    # stats = tree.get_stats()
    # print(f"插入记录数: {stats['total_inserts']}")
    # print(f"树中实际记录数: {stats['actual_records']}")
    # print(f"树高: {stats['height']}")
    # print(f"叶子节点数: {stats['leaf_nodes']}")

    print(f"insert_count: {tree.insert_count}")
    return tree
