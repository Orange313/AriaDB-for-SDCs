from collections import deque

def compare_trees_bfs(tree1, tree2):

    hashes1 = tree1.get_all_hashes()
    hashes2 = tree2.get_all_hashes()
    
    differences = {
        'tables': {},
        'partitions': {}
    }
    
    # 比较根节点
    if hashes1['root_hash'] == hashes2['root_hash']:
        print("The root node hash is the same!")
        return differences
    
    print("Differences between the two trees...")
    
    #  (节点类型, 节点路径, 节点1数据, 节点2数据)
    # queue = deque([('root', '', hashes1, hashes2)])
    queue = deque([('root','')])
    
    while queue:
        node_type, path = queue.popleft()
        
        if node_type == 'root':
            # 对称结构下可以直接遍历任意一棵树的tables
            for table_id in hashes1['tables']:
                hash1 = hashes1['tables'][table_id]['table_hash']
                hash2 = hashes2['tables'][table_id]['table_hash']
                
                if hash1 != hash2:
                    differences['tables'][table_id] = {
                        'tree1': hash1,
                        'tree2': hash2
                    }
                    queue.append(('table', table_id))
        
        elif node_type == 'table':
            table_id = path
            # 对称结构下可以直接遍历该表的所有分区
            for partition_id in hashes1['tables'][table_id]['partitions']:
                hash1 = hashes1['tables'][table_id]['partitions'][partition_id]['partition_hash']
                hash2 = hashes2['tables'][table_id]['partitions'][partition_id]['partition_hash']
                
                if hash1 != hash2:
                    full_path = f"{table_id}/{partition_id}"
                    differences['partitions'][full_path] = {
                        'tree1': hash1,
                        'tree2': hash2
                    }

    

    print("\nDiffrences report:")
    print("=" * 80)
    
    if differences['tables']:
        print("\nDifferent table:")
        for table_id, hashes in differences['tables'].items():
            print(f"table {table_id}:")
            print(f"  tree1_hash: {hashes['tree1']}")
            print(f"  tree2_hash: {hashes['tree2']}")
    
    if differences['partitions']:
        print("\nDiffrent partition:")
        for partition_path, hashes in differences['partitions'].items():
            print(f"partition {partition_path}:")
            print(f"  tree1_hash: {hashes['tree1']}")
            print(f"  tree1_hash: {hashes['tree2']}")
    
    print(f"Different table counts: {len(differences['tables'])}")
    print(f"Different partition counts: {len(differences['partitions'])}")
    
    return differences