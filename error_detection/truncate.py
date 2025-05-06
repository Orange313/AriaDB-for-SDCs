import pandas as pd
import os

def keep_lsn_up_to(input_file, output_file, max_lsn):
    try:
        df = pd.read_csv(input_file)
        
        filtered_df = df[df['LSN'] <= max_lsn]
        
        filtered_df.to_csv(output_file, index=False)
        
        print(f"已保留 LSN ≤ {max_lsn} 的记录，共 {len(filtered_df)} 行")
        print(f"结果已写入到: {output_file}")
        
    except Exception as e:
        print(f"处理失败: {e}")

keep_lsn_up_to(
    input_file='error_detection\data\different_partition\log01_14.csv',
    output_file='error_detection\data\different_partition\log01_14.csv',
    max_lsn=62231
)