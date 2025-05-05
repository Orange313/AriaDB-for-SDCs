
import pandas as pd
import random
import math

input_csv = 'error_detection\data\cut\A\log10s.csv' 
output_csv = 'error_detection\data\cut\A\log10s_a.csv'
log_file = 'error_detection/data/sdc_log/log10s_sdc.csv'
sdc_probability = 0.0001  # 万分之一概率
expected_errors = 3       

# 读取 CSV
df = pd.read_csv(input_csv)
total_rows = len(df)

# 计算实际需要的错误数量（确保至少达到期望值）
actual_errors = max(math.floor(total_rows * sdc_probability), expected_errors)

# 存储变更记录
logs = []

def tweak_hex_value(hex_str):
    try:
        value = int(hex_str, 16)
        delta = random.choice([-3, -2, -1, 1, 2, 3])
        tweaked = max(0, value + delta)
        return hex(tweaked)[2:]  
    except ValueError:
        return hex_str 


error_indices = random.sample(range(total_rows), actual_errors)
for idx in error_indices:
    row = df.iloc[idx]
    original_value = row['Value']
    modified_value = tweak_hex_value(original_value)
    df.at[idx, 'Value'] = modified_value
    
    logs.append({
        'LSN': row['LSN'],
        'TxnID': row['TxnID'],
        'TableID': row['TableID'],
        'PartitionID': row['PartitionID'],
        'Key': row['Key'],
        'OriginalValue': original_value,
        'InjectedValue': modified_value
    })


df.to_csv(output_csv, index=False)

if logs:
    log_df = pd.DataFrame(logs)
    log_df.to_csv(log_file, index=False)
    print(f"成功注入 {len(logs)} 个错误（预期: {expected_errors}）")
    print(f"处理后的文件保存至: {output_csv}")
    print(f"错误日志保存至: {log_file}")
else:
    print("没有注入任何静默错误。")