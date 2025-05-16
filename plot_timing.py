import pandas as pd
import matplotlib.pyplot as plt

# 读取数据
df = pd.read_csv('timing_results.csv')

# 节点数量分组
node_sizes = sorted(df['nodes'].unique())

# 要画的字段及图名
metrics = {
    'bfs_time': 'BFS Execution Time',
    'dfs_time': 'DFS Execution Time',
    'pagerank_time': 'PageRank Execution Time',
    'mst_time': 'Minimum Spanning Tree Execution Time',
    'shortest_path_time': 'Single Source Shortest Path Execution Time'
}

# 每种算法一张图
for metric, title in metrics.items():
    plt.figure()
    for n in node_sizes:
        subset = df[df['nodes'] == n]
        plt.plot(subset['workers'], subset[metric], label=f'{n} nodes')
    plt.xlabel('Number of Workers')
    plt.ylabel('Time (s)')
    plt.title(title)
    plt.legend()
    plt.grid(True)
    plt.savefig(f'{metric}.png')  # 可选：保存为图像
    plt.show()
