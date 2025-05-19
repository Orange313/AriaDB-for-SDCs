# 分区数量对错误检测效果的影响
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
basic_total_times = [297.8, 396.4, 378.9, 414.9, 466.4, 448.3, 470.6, 600.3]  # ms

# 升级版数据
improved_rates = [20.00, 50.00, 50.00, 50.00, 83.33, 66.67, 33.33, 66.67]
improved_compare_times = [10.2, 13.9, 17.6, 18.8, 17.7, 17.6, 18.5, 18.2]  # ms
improved_total_times = [555.8, 513.9, 528.5, 630.9, 493.9, 655.8, 716.4, 555.7]  # ms

# 创建画布（3行1列）
fig, (ax1, ax2, ax3) = plt.subplots(3, 1, figsize=(12, 12), dpi=120)
plt.subplots_adjust(hspace=0.35)  # 调整子图间距

# ========== 错误检出率对比 ==========
colors = ['#4C78A8', '#F58518']  # 蓝橙色系
width = 0.50  # 柱状图宽度

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
                 fontsize=10)

# ========== 比对时间对比 ==========
line1, = ax2.plot(groups, basic_compare_times, color=colors[0],
                 marker='o', markersize=8, linewidth=2.5, label='前缀树分区校验')
line2, = ax2.plot(groups, improved_compare_times, color=colors[1],
                 marker='s', markersize=8, linewidth=2.5, label='分层哈希校验')

ax2.set_xticks(groups)
ax2.set_ylabel('比对时间 (ms)', fontsize=18)
ax2.set_ylim(0, 350)
ax2.grid(axis='y', linestyle=':', alpha=0.6)

# 添加数据标签（分层哈希标签下移避免重叠）
for x, y1, y2 in zip(groups, basic_compare_times, improved_compare_times):
    ax2.text(x, y1+10, f'{y1:.1f}', ha='center', va='bottom', 
            fontsize=9, color=colors[0])
    ax2.text(x, y2+10, f'{y2:.1f}', ha='center', va='bottom',  # 修改为下方显示
            fontsize=9, color=colors[1])

# ========== 总检测时间对比 ==========
line3, = ax3.plot(groups, basic_total_times, color=colors[0],
                 marker='o', markersize=12, linewidth=2.5, label='前缀树分区校验')
line4, = ax3.plot(groups, improved_total_times, color=colors[1],
                 marker='s', markersize=12, linewidth=2.5, label='分层哈希校验')

ax3.set_xticks(groups)
ax3.set_xlabel('分区数量', fontsize=18)
ax3.set_ylabel('总检测时间 (ms)', fontsize=18)
ax3.set_ylim(0, 800)
ax3.grid(axis='y', linestyle=':', alpha=0.6)

# 添加数据标签（特殊处理重叠点）
for x, y1, y2 in zip(groups, basic_total_times, improved_total_times):
    if x==10:  # 处理重叠严重的点
        ax3.text(x, y1-20, f'{y1:.1f}', ha='center', va='top', 
                fontsize=9, color=colors[0])
        ax3.text(x, y2+20, f'{y2:.1f}', ha='center', va='bottom',
                fontsize=9, color=colors[1])
    elif x==16:
        ax3.text(x, y1+20, f'{y1:.1f}', ha='center', va='bottom', 
        fontsize=9, color=colors[0])
        ax3.text(x, y2-20, f'{y2:.1f}', ha='center', va='top',
                fontsize=9, color=colors[1])
    else:
        ax3.text(x, y1+20, f'{y1:.1f}', ha='center', va='bottom',
                fontsize=9, color=colors[0])
        ax3.text(x, y2+20, f'{y2:.1f}', ha='center', va='bottom',
                fontsize=9, color=colors[1])

# 统一图例位置
for ax in [ax1, ax2, ax3]:
    ax.legend(loc='upper right', frameon=True, fontsize=12)

# 优化布局
plt.tight_layout()

# 保存图像
plt.savefig('partition_impact_comparison.png', 
           dpi=300, 
           bbox_inches='tight', 
           transparent=True)
plt.show()