from collections import deque
from snapshot_detection.tree_hash import collect_partition_hashes
import time

def compare_trees_bfs(tree1, tree2):

    hashes1 = tree1.get_all_hashes()
    hashes2 = tree2.get_all_hashes()
    
    differences = {
        'tables': {},
        'partitions': {}
    }
    start_time_compare = time.time()
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

            for table_id in hashes1['tables']:
                hash1 = hashes1['tables'][table_id]['table_hash']
                hash2 = hashes2['tables'][table_id]['table_hash']
                #根节点哈希不同时，比对表节点哈希
                if hash1 != hash2:
                    differences['tables'][table_id] = {
                        'tree1': hash1,
                        'tree2': hash2
                    }
                    queue.append(('table', table_id))
        
        elif node_type == 'table':
            table_id = path

            for partition_id in hashes1['tables'][table_id]['partitions']:
                hash1 = hashes1['tables'][table_id]['partitions'][partition_id]['partition_hash']
                hash2 = hashes2['tables'][table_id]['partitions'][partition_id]['partition_hash']
                
                if hash1 != hash2:
                    full_path = f"{table_id}/{partition_id}"
                    differences['partitions'][full_path] = {
                        'tree1': hash1,
                        'tree2': hash2
                    }
    end_time = time.time()
    duration_compare = end_time - start_time_compare
    print(f"Compare time : {duration_compare:.4f} seconds")
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
    
    return differences,differences['partitions']

# 前缀树+哈希法比较哈希
def compare_partition_hashes(tree1,tree2):
    hashes1 = collect_partition_hashes(tree1)
    hashes2 = collect_partition_hashes(tree2)

    hash_dict1 = {(table_id, partition_id): hash_value for table_id, partition_id, hash_value in hashes1}
    hash_dict2 = {(table_id, partition_id): hash_value for table_id, partition_id, hash_value in hashes2}

    differences = {
        # 'only_in_tree1':[],
        # 'only_in_tree2':[],
        'different_hash':[]
    }

    for (table_id, partition_id), hash1 in hash_dict1.items():
        # if (table_id, partition_id) not in hash_dict2:
        #     differences['only_in_tree1'].append((table_id, partition_id, hash1))
        if hash1 != hash_dict2[(table_id, partition_id)]:
            hash2 = hash_dict2[(table_id, partition_id)]
            differences['different_hash'].append((table_id, partition_id, hash1, hash2))

    # for (table_id, partition_id), hash2 in hash_dict2.items():
    #     if (table_id, partition_id) not in hash_dict1:
    #         differences['only_in_tree2'].append((table_id, partition_id, hash2))

    print(f"\ndifferent hashes count: {len(differences['different_hash'])}")
    if differences['different_hash']:
        print("partition:")
        for table_id, partition_id, hash1, hash2 in differences['different_hash']:
            print(f"  table{table_id}/partition{partition_id}:")
            print(f"    tree1 hash: {hash1}")
            print(f"    tree2 hash: {hash2}")

    # total_differences = len(differences['only_in_tree1']) + len(differences['only_in_tree2']) + len(differences['different_hash'])
    total_differences = len(differences['different_hash'])
    if total_differences == 0:
        print("\n结论: Two tree hashes are completely the same.")
    else:
        print(f"\n结论: Different partition counts:{total_differences}")
    
    return differences