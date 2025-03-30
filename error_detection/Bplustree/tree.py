import pandas as pd
from node import LeafNode, InternalNode

DEFAULT_ORDER = 256
DEFAULT_LEAF_SIZE = 512
DEFAULT_BUFFER_SIZE = 1024

class BPlusTree:
    def __init__(self, order=DEFAULT_ORDER, leaf_size=DEFAULT_LEAF_SIZE, buffer_size=DEFAULT_BUFFER_SIZE):
        self.order = order
        self.leaf_size = leaf_size
        self.buffer_size = buffer_size
        self.root = LeafNode(leaf_size)

    def insert(self, lsn, record):
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

    def search(self, lsn):
        node = self.root
        if not node.is_leaf:
            node.flush_buffer()
        while not node.is_leaf:
            node.flush_buffer()
            idx = node.find_child_index(lsn)
            node = node.children[idx]
        result = []
        for key, rec in node.records:
            if key == lsn:
                result.append(rec)
        return result

    def range_search(self, start_lsn, end_lsn):
        results = []
        node = self.root
        if not node.is_leaf:
            node.flush_buffer()
        while not node.is_leaf:
            node.flush_buffer()
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

def build_bplus_tree_from_csv(csv_path, order=DEFAULT_ORDER, leaf_size=DEFAULT_LEAF_SIZE, buffer_size=DEFAULT_BUFFER_SIZE):
    df = pd.read_csv(csv_path)
    df.sort_values(by='LSN', inplace=True)
    tree = BPlusTree(order=order, leaf_size=leaf_size, buffer_size=buffer_size)
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
    return tree
