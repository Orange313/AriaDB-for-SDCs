import pandas as pd
import random


input_csv = 'write_set_log03281841.csv' 
output_csv = 'log_with_sdc_03281841.csv'
log_file = 'sdc_injection_log.csv'
sdc_probability = 0.00005  # 例如 0.00005 对应每两万条注入一条

# 读取 CSV
df = pd.read_csv(input_csv)

# 存储变更记录
logs = []

def tweak_hex_value(hex_str):
    try:
        value = int(hex_str, 16)
        # ±1~±3 的微调
        delta = random.choice([-3, -2, -1, 1, 2, 3])
        tweaked = max(0, value + delta)#结果若为负，那么改为0
        return hex(tweaked)
    except ValueError:
        return hex_str 

# 遍历注入
for idx, row in df.iterrows():
    if random.random() < sdc_probability:
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
else:
    print("没有注入任何静默错误。")

print(f"log injected saved in: {output_csv}")
print(f" injection log saved in: {log_file}")
