import pandas as pd

def keep_lsn_up_to(input_file, max_lsn=33014):

    try:
  
        df = pd.read_csv(input_file)
        
        filtered_df = df[df['LSN'] <= max_lsn]
        
        filtered_df.to_csv(input_file, index=False)
        print(f"已保留 LSN ≤ {max_lsn} 的记录，共 {len(filtered_df)} 行")
        
    except Exception as e:
        print(f"处理失败: {e}")

# 使用示例
keep_lsn_up_to('log_01.csv', max_lsn=33014)