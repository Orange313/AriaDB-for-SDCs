# 三种错误检测方法比较
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

# 设置全局样式
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 数据准备
groups = np.arange(1, 11)  # 10组实验
methods = ['逐条比对', '分块比对', 'RIVA']

# 错误检出率 (%)
detection_rates = {
    '逐条比对': [100]*10,
    '分块比对': [100]*10,
    'RIVA': [14, 20, 20, 17, 17, 17, 14, 17, 20, 17]
}

# 总检测时间 (秒)
total_times = {
    '逐条比对': [2.8334, 2.1957, 1.4791, 2.2118, 1.9278, 1.9514, 2.4565, 2.0720, 1.7867, 1.9199],
    '分块比对': [2.9051, 2.1474, 1.4704, 2.2380, 2.1542, 2.1110, 2.5892, 2.2677, 1.8845, 1.9438],
    'RIVA': [3.2947, 2.8465, 2.5179, 3.1785, 2.9834, 3.0132, 4.0471, 3.6957, 2.7734, 2.9072]
}

# 创建画布和双轴
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 10), dpi=120)
# fig.suptitle('三种日志比对方法性能对比', fontsize=16, fontweight='bold', y=0.98)

# ========== 错误检出率对比 ==========
colors = ['#4C78A8', '#F58518', '#54A24B']  # 蓝,橙,绿
width = 0.30

# 为每种方法创建柱状图
for i, method in enumerate(methods):
    offset = width * (i - 1)  # 居中显示
    bars = ax1.bar(groups + offset, detection_rates[method], width, 
                  color=colors[i], alpha=0.8, label=method)
    
    # 添加数据标签（RIVA数据太小，单独处理）
    for x, y in zip(groups + offset, detection_rates[method]):
        if method == 'RIVA':
            ax1.text(x, y+0.5, f'{y:.2f}%', ha='center', va='bottom', 
                    fontsize=8, color='black')
        else:
            ax1.text(x, y+1, f'{y:.0f}%', ha='center', va='bottom', 
                    fontsize=9, color='black')

ax1.set_xticks(groups)
ax1.set_ylabel('错误检出率 (%)', fontsize=12)
ax1.set_ylim(0, 110)
ax1.yaxis.set_major_formatter(PercentFormatter(100))
ax1.grid(axis='y', linestyle=':', alpha=0.6)
ax1.legend(loc='upper right', frameon=True)

# ========== 总检测时间对比 ==========
markers = ['o', 's', 'D']  # 圆形,方形,菱形

# for i, method in enumerate(methods):
#     ax2.plot(groups, total_times[method], color=colors[i],
#             marker=markers[i], markersize=8, linewidth=2, 
#             label=method, zorder=3)
    
#     # 添加数据标签
#     for x, y in zip(groups, total_times[method]):
#         ax2.text(x, y+0.1, f'{y:.2f}s', ha='center', va='bottom', 
#                 fontsize=9, color=colors[i])

label_offsets = [-0.30, 0.25, 0.10]  # 为每种方法设置不同的垂直偏移量
valigns = ['bottom', 'top', 'bottom']  # 为每种方法设置不同的垂直对齐方式

for i, method in enumerate(methods):
    ax2.plot(groups, total_times[method], color=colors[i],
            marker=markers[i], markersize=8, linewidth=2, 
            label=method, zorder=3)
    
    # 添加数据标签，每种方法使用独立的偏移和对齐
    for x, y in zip(groups, total_times[method]):
        ax2.text(x, y + label_offsets[i], f'{y:.2f}s', 
                ha='center', va=valigns[i],  # 使用独立的对齐方式
                fontsize=9, color=colors[i])

ax2.set_xticks(groups)
ax2.set_xlabel('实验组别', fontsize=12)
ax2.set_ylabel('总检测时间 (秒)', fontsize=12)
ax2.set_ylim(0, 4.5)
ax2.grid(axis='y', linestyle=':', alpha=0.6)
ax2.legend(loc='upper right', frameon=True)

# 调整布局
plt.tight_layout(rect=[0, 0, 1, 0.96])

# 保存图像
plt.savefig('log_comparison.png', dpi=300, bbox_inches='tight', transparent=True)
plt.show()