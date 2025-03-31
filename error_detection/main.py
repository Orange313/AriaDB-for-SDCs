from Bplustree.tree import build_bplus_tree_from_csv
from snapshot_detection.incremental_snapshot import generate_incremental_snapshot_from_bplus_tree

if __name__ == "__main__":

    bplus_tree = build_bplus_tree_from_csv("log_with_sdc_03281841.csv")
    print("Bplus tree generated successfully.")
    
    # test
    # class MockBPlusTree:
    #     def __init__(self):
    #         class MockLeaf:
    #             def __init__(self, records):
    #                 self.is_leaf = True
    #                 self.records = records
    #                 self.next_leaf = None
            
            
    #         records1 = [
    #             (1, {'TxnID': 101, 'TableID': 'users', 'PartitionID': 'p1', 'Key': 'user1', 'Value': 'Alice'}),
    #             (2, {'TxnID': 102, 'TableID': 'users', 'PartitionID': 'p1', 'Key': 'user2', 'Value': 'Bob'}),
    #             (3, {'TxnID': 103, 'TableID': 'products', 'PartitionID': 'p1', 'Key': 'item1', 'Value': 'Phone'}),
    #         ]
            
    #         records2 = [
    #             (4, {'TxnID': 104, 'TableID': 'users', 'PartitionID': 'p1', 'Key': 'user1', 'Value': 'Alice_updated'}),
    #             (5, {'TxnID': 105, 'TableID': 'orders', 'PartitionID': 'p2', 'Key': 'order1', 'Value': '1001'}),
    #         ]
            
    #         leaf1 = MockLeaf(records1)
    #         leaf2 = MockLeaf(records2)
    #         leaf1.next_leaf = leaf2
            
    #         self.root = leaf1
    
    # 构建前缀树
    # mock_tree = MockBPlusTree()
    prefix_tree = generate_incremental_snapshot_from_bplus_tree(bplus_tree)
    
    # 打印前缀树结构
    # print("\n前缀树结构:")
    #prefix_tree.print_tree()
    
    # 查询示例
    # print("\n查询测试:")
    # print("users/p1/user1:", prefix_tree.get_value('users', 'p1', 'user1'))  # 应该返回 Alice_updated
    # print("users/p1/user2:", prefix_tree.get_value('users', 'p1', 'user2'))  # 应该返回 Bob
    # print("non_existent:", prefix_tree.get_value('non', 'existent', 'key'))  # 应该返回 None