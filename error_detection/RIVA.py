import hashlib
import time


def compute_sha256(file_path):
    # 计算文件的 SHA-256 校验和
    sha256_hash = hashlib.sha256()
    with open(file_path, 'rb') as f:
        for byte_block in iter(lambda: f.read(4096), b""):
            sha256_hash.update(byte_block)
    return sha256_hash.hexdigest()

def verify_files_integrity(file1, file2):
    #比较原文件与传输后文件的完整性
    start_time = time.time()
    file1_hash = compute_sha256(file1)
    file2_hash = compute_sha256(file2)

    print(f"file1 checksum: {file1_hash}")
    print(f"file2 checksum: {file2_hash}")
    
    if file1_hash == file2_hash:
        print("Files are consistent , no silent data corruption。")
    else:
        print("Files are unconsistent , there may be some silent data corruption. ")

    end_time = time.time()
    duration = end_time - start_time
    print(f"Total time : {duration:.4f} seconds")

file1_path = 'log1_with_sdc_33021.csv'  
file2_path = 'log2_33021.csv'  

verify_files_integrity(file1_path, file2_path)
