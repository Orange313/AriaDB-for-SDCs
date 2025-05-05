import pandas as pd
import os
def keep_lsn_up_to(input_file,output_file=None, max_lsn=33014):

    try:
  
        df = pd.read_csv(input_file)
        
        filtered_df = df[df['LSN'] <= max_lsn]
        if output_file is None:
            base, ext = os.path.splitext(input_file)
            output_file = f"{base}s"
        
        filtered_df.to_csv(input_file, index=False)
        print(f"已保留 LSN ≤ {max_lsn} 的记录，共 {len(filtered_df)} 行")
        
    except Exception as e:
        print(f"处理失败: {e}")

# 使用示例
keep_lsn_up_to('error_detection\data\original\A\log01.csv', max_lsn=33014)