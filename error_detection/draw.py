import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

# 设置全局样式
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 实验数据准备
groups = np.arange(1, 6)  # 5组实验
compare_times = [2.1058, 2.1957, 2.0867, 1.9816, 2.2382]  # 比对时间(秒)
detected_errors = [3, 5, 9, 12, 15]  # 检出错误数
detection_rates = [100, 100, 100, 100, 100]  # 错误检出率(%)

# 创建画布和主坐标轴
fig, ax1 = plt.subplots(figsize=(10, 6), dpi=120)
fig.suptitle('日志比对性能实验结果', fontsize=15, fontweight='bold', y=0.98)

# 柱状图：检出错误数（左轴）
bars = ax1.bar(groups - 0.15, detected_errors, width=0.3,
              color='#4C78A8', alpha=0.8, label='检出错误数')
ax1.set_xlabel('实验组别', fontsize=12)
ax1.set_ylabel('检出错误数 (个)', fontsize=12)
ax1.set_ylim(0, max(detected_errors)*1.2)
ax1.set_xticks(groups)

# 添加柱状图数据标签
for bar in bars:
    height = bar.get_height()
    ax1.text(bar.get_x() + bar.get_width()/2., height,
             f'{int(height)}', ha='center', va='bottom',
             fontsize=10, color='black')

# 折线图：比对时间（右轴）
ax2 = ax1.twinx()
line, = ax2.plot(groups + 0.15, compare_times, color='#E45756',
                marker='o', markersize=8, linewidth=2.5,
                label='比对时间')
ax2.set_ylabel('比对时间 (秒)', fontsize=12)
ax2.set_ylim(0, max(compare_times)*1.3)

# 添加折线图数据标签
for x, y in zip(groups + 0.15, compare_times):
    ax2.text(x, y + 0.05, f'{y:.3f}s', ha='center', va='bottom',
            fontsize=10, color='#E45756')

# 错误检出率标注（顶部）
for i, rate in enumerate(detection_rates):
    ax1.text(groups[i], max(detected_errors)*1.1, f'{rate}%',
            ha='center', va='bottom', fontsize=11,
            color='#59A14F', weight='bold')

# 合并图例
lines = [bars, line]
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='upper left', frameon=True)

# 网格线设置
ax1.grid(axis='y', linestyle=':', alpha=0.6)
ax2.grid(False)  # 避免右轴网格线重叠

plt.tight_layout()
plt.savefig('log_comparison_performance.png', dpi=300, bbox_inches='tight')
plt.show()