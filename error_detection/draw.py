# 分区数量对错误检测效果的影响
# 本文两种增量快照方法比较
import numpy as np
import matplotlib.pyplot as plt
from matplotlib.ticker import PercentFormatter

# 设置全局样式
plt.style.use('seaborn-v0_8-whitegrid')
plt.rcParams['font.sans-serif'] = ['Microsoft YaHei']
plt.rcParams['axes.unicode_minus'] = False

# 数据准备
groups = np.arange(2, 18, 2)  # 横坐标: 2,4,6,8,10,12,14,16

# 基础版数据
basic_rates = [20.00, 50.00, 50.00, 50.00, 83.33, 66.67, 33.33, 66.67]
basic_compare_times = [147.6, 196.3, 190.1, 210.8, 229.2, 227.9, 245.8, 303.9]  # ms

# 升级版数据
improved_rates = [20.00, 50.00, 50.00, 50.00, 83.33, 66.67, 33.33, 66.67]
improved_compare_times = [10.2, 13.9, 17.6, 18.8, 17.7, 17.6, 18.5, 18.2]  # ms

# 创建画布（改为2行1列）
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(12, 8), dpi=120)
plt.subplots_adjust(hspace=0.3)  # 调整子图间距

# ========== 错误检出率对比 ==========
colors = ['#4C78A8', '#F58518']  # 蓝橙色系
width = 0.35  # 柱状图宽度

bars1 = ax1.bar(groups - width/2, basic_rates, width, 
               color=colors[0], alpha=0.8, label='前缀树分区校验')
bars2 = ax1.bar(groups + width/2, improved_rates, width, 
               color=colors[1], alpha=0.8, label='分层哈希校验')

ax1.set_xticks(groups)
ax1.set_ylabel('错误检出率 (%)', fontsize=12)
ax1.set_ylim(0, 100)
ax1.yaxis.set_major_formatter(PercentFormatter(100))
ax1.grid(axis='y', linestyle=':', alpha=0.6)

# 添加数据标签
for bars in [bars1, bars2]:
    for bar in bars:
        height = bar.get_height()
        ax1.text(bar.get_x() + bar.get_width()/2., height+2,
                 f'{height:.0f}%', ha='center', va='bottom', 
                 fontsize=9)

# ========== 比对时间对比 ==========
line1, = ax2.plot(groups, basic_compare_times, color=colors[0],
                 marker='o', markersize=8, linewidth=2.5, label='前缀树分区校验')
line2, = ax2.plot(groups, improved_compare_times, color=colors[1],
                 marker='s', markersize=8, linewidth=2.5, label='分层哈希校验')

ax2.set_xticks(groups)
ax2.set_xlabel('分区数量', fontsize=12)
ax2.set_ylabel('比对时间 (ms)', fontsize=12)
ax2.set_ylim(0, 350)
ax2.grid(axis='y', linestyle=':', alpha=0.6)

# 添加数据标签
for x, y1, y2 in zip(groups, basic_compare_times, improved_compare_times):
    ax2.text(x, y1+10, f'{y1:.1f}', ha='center', va='bottom', 
            fontsize=9, color=colors[0])
    ax2.text(x, y2+10, f'{y2:.1f}', ha='center', va='bottom', 
            fontsize=9, color=colors[1])

# 统一图例位置
ax1.legend(loc='upper right', frameon=True, fontsize=10)
ax2.legend(loc='upper right', frameon=True, fontsize=10)

# 优化布局
plt.tight_layout()

# 保存图像
plt.savefig('method_comparison_core_metrics.png', 
           dpi=300, 
           bbox_inches='tight', 
           transparent=True)
plt.show()