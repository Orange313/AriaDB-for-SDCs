import os
import matplotlib.pyplot as plt
import numpy as np

os.makedirs('picture', exist_ok=True)

# 例
record_counts = [100, 200, 300, 400, 500, 600, 700, 800, 900, 1000]  # 记录条数
accuracy_rates = [0.65, 0.72, 0.78, 0.82, 0.85, 0.87, 0.89, 0.91, 0.92, 0.93]  # 对应准确率


plt.figure(figsize=(10, 6))
plt.plot(record_counts, accuracy_rates, 'b-o', linewidth=2, markersize=8, label='Accuracy')

# 标题和标签
plt.title('Accuracy vs Record Count', fontsize=14)
plt.xlabel('Number of Records', fontsize=12)
plt.ylabel('Accuracy Rate', fontsize=12)

# 坐标轴范围
plt.xlim(min(record_counts)-50, max(record_counts)+50)
plt.ylim(0, 1.05)  # 准确率范围0-1

plt.grid(True, linestyle='--', alpha=0.7)
plt.legend(fontsize=12)
plt.tight_layout()

# 保存图片
save_path = os.path.join('picture', 'accuracy_vs_records.png')
plt.savefig(save_path, dpi=300, bbox_inches='tight')
print(f'图片已保存到: {save_path}')

plt.show()