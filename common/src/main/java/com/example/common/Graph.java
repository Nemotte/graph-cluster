package com.example.common;

import java.util.*;
import java.util.concurrent.*;

public class Graph {

    // --- CSR 结构字段 ---
    private int[] csrIndex;
    private int[] csrEdges;
    private int[] nodeMap;              // index -> realNodeId
    private Map<Integer, Integer> nodeToIndex; // realNodeId -> index
    private double[] edgeWeights; // 与 csrEdges 一一对应


    // --- 邻接表（仅用于构建阶段） ---
    private final Map<Integer, List<Integer>> adjList = new ConcurrentHashMap<>();

    // --- 添加边 ---
    public void addEdge(int u, int v) {
        adjList.computeIfAbsent(u, k -> new ArrayList<>()).add(v);
    }

    // --- 构建 CSR 结构 ---
    public void buildCSR() {
        List<Integer> nodes = new ArrayList<>(adjList.keySet());
        Collections.sort(nodes);

        nodeToIndex = new HashMap<>();
        for (int i = 0; i < nodes.size(); i++) {
            nodeToIndex.put(nodes.get(i), i);
        }

        int n = nodes.size();
        csrIndex = new int[n + 1];
        List<Integer> edges = new ArrayList<>();

        for (int i = 0; i < n; i++) {
            int u = nodes.get(i);
            csrIndex[i] = edges.size();
            for (int v : adjList.getOrDefault(u, List.of())) {
                Integer vi = nodeToIndex.get(v);
                if (vi != null) edges.add(vi);
            }
        }
        csrIndex[n] = edges.size();
        csrEdges = new int[edges.size()];
        edgeWeights = new double[edges.size()];
        Random rand = new Random();

        for (int i = 0; i < edges.size(); i++) {
            csrEdges[i] = edges.get(i);
            edgeWeights[i] = 1.0 + rand.nextDouble() * 9.0; // 权重 ∈ [1, 10)
        }

        nodeMap = nodes.stream().mapToInt(Integer::intValue).toArray();

        // 构建完成后可释放邻接表内存（可选）
        adjList.clear();
    }

    public void clear() {
        adjList.clear();
        nodeToIndex = null;
        nodeMap = null;
        csrIndex = null;
        csrEdges = null;
    }


    // --- CSR 获取邻居 ---
    public List<Integer> getCSRNeighbors(int nodeId) {
        Integer idx = nodeToIndex.get(nodeId);
        if (idx == null) return List.of();
        int start = csrIndex[idx];
        int end = csrIndex[idx + 1];
        List<Integer> neighbors = new ArrayList<>(end - start);
        for (int i = start; i < end; i++) {
            neighbors.add(nodeMap[csrEdges[i]]);
        }
        return neighbors;
    }

    // --- 并行 BFS 使用 CSR ---
    public Set<Integer> parallelBFS(int startNodeId) {
        Set<Integer> visited = ConcurrentHashMap.newKeySet();
        Queue<Integer> queue = new ConcurrentLinkedQueue<>();
        visited.add(startNodeId);
        queue.add(startNodeId);

        ExecutorService executor = Executors.newFixedThreadPool(4);
        while (!queue.isEmpty()) {
            int levelSize = queue.size();
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < levelSize; i++) {
                Integer node = queue.poll();
                if (node == null) continue;
                futures.add(executor.submit(() -> {
                    for (int neighbor : getCSRNeighbors(node)) {
                        if (visited.add(neighbor)) {
                            queue.add(neighbor);
                        }
                    }
                }));
            }
            for (Future<?> f : futures) {
                try { f.get(); } catch (Exception ignored) {}
            }
        }
        executor.shutdown();
        return visited;
    }

    // --- 全图并行 BFS 使用 CSR ---
    public Set<Integer> fullParallelBFS() {
        Set<Integer> visited = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> tasks = new ArrayList<>();

        for (int nodeId : nodeToIndex.keySet()) {
            if (visited.add(nodeId)) {
                tasks.add(executor.submit(() -> {
                    Queue<Integer> queue = new ArrayDeque<>();
                    queue.add(nodeId);
                    while (!queue.isEmpty()) {
                        Integer current = queue.poll();
                        for (Integer neighbor : getCSRNeighbors(current)) {
                            if (visited.add(neighbor)) {
                                queue.add(neighbor);
                            }
                        }
                    }
                }));
            }
        }

        for (Future<?> task : tasks) {
            try { task.get(); } catch (Exception ignored) {}
        }
        executor.shutdown();
        return visited;
    }

    // --- 并行 DFS 使用 CSR ---
    public Set<Integer> parallelDFS(int startNodeId) {
        Set<Integer> visited = ConcurrentHashMap.newKeySet();
        Deque<Integer> stack = new ConcurrentLinkedDeque<>();
        visited.add(startNodeId);
        stack.push(startNodeId);

        ExecutorService executor = Executors.newFixedThreadPool(4);

        while (!stack.isEmpty()) {
            int levelSize = stack.size();
            List<Future<?>> futures = new ArrayList<>();

            for (int i = 0; i < levelSize; i++) {
                Integer node = stack.poll();
                if (node == null) continue;

                futures.add(executor.submit(() -> {
                    for (Integer neighbor : getCSRNeighbors(node)) {
                        if (visited.add(neighbor)) {
                            stack.push(neighbor);
                        }
                    }
                }));
            }

            for (Future<?> f : futures) {
                try { f.get(); } catch (Exception e) { e.printStackTrace(); }
            }
        }

        executor.shutdown();
        return visited;
    }

    // --- 全图 DFS 使用 CSR ---
    public Set<Integer> fullParallelDFS() {
        Set<Integer> visited = ConcurrentHashMap.newKeySet();
        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Future<?>> tasks = new ArrayList<>();

        for (int nodeId : nodeToIndex.keySet()) {
            if (visited.add(nodeId)) {
                tasks.add(executor.submit(() -> {
                    Deque<Integer> stack = new ArrayDeque<>();
                    stack.push(nodeId);
                    while (!stack.isEmpty()) {
                        Integer current = stack.pop();
                        for (Integer neighbor : getCSRNeighbors(current)) {
                            if (visited.add(neighbor)) {
                                stack.push(neighbor);
                            }
                        }
                    }
                }));
            }
        }

        for (Future<?> task : tasks) {
            try { task.get(); } catch (Exception e) { e.printStackTrace(); }
        }

        executor.shutdown();
        return visited;
    }

    // --- 提取连通分量（BFS） ---
    public List<Set<Integer>> getConnectedComponentsBFS() {
        Set<Integer> visited = ConcurrentHashMap.newKeySet();
        List<Set<Integer>> components = new ArrayList<>();

        for (int nodeId : nodeToIndex.keySet()) {
            if (visited.contains(nodeId)) continue;

            Set<Integer> component = ConcurrentHashMap.newKeySet();
            Queue<Integer> queue = new ArrayDeque<>();
            queue.add(nodeId);
            visited.add(nodeId);
            component.add(nodeId);

            while (!queue.isEmpty()) {
                Integer current = queue.poll();
                for (Integer neighbor : getCSRNeighbors(current)) {
                    if (visited.add(neighbor)) {
                        queue.add(neighbor);
                        component.add(neighbor);
                    }
                }
            }

            components.add(component);
        }

        return components;
    }

    // --- 获取边列表（用于主控归并） ---
    public List<int[]> getAllEdges() {
        List<int[]> edges = new ArrayList<>();
        if (csrIndex == null || csrEdges == null) return List.of();

        for (int i = 0; i < nodeMap.length; i++) {
            int u = nodeMap[i];
            for (int j = csrIndex[i]; j < csrIndex[i + 1]; j++) {
                int v = nodeMap[csrEdges[j]];
                edges.add(new int[]{u, v});
            }
        }
        return edges;
    }

    public Map<Integer, Double> computePageRank(Set<Integer> component, int maxIter, double damping) {
        Set<Integer> compSet = new HashSet<>(component);
        int n = nodeMap.length;
        double[] rank = new double[n];
        double[] next = new double[n];
        boolean[] active = new boolean[n];
        int[] outDegree = new int[n];

        // 初始化：计算出度和激活节点
        for (int i = 0; i < n; i++) {
            int nodeId = nodeMap[i];
            if (compSet.contains(nodeId)) {
                active[i] = true;
                rank[i] = 1.0 / component.size();
                outDegree[i] = csrIndex[i + 1] - csrIndex[i];
            }
        }

        // 主迭代
        for (int iter = 0; iter < maxIter; iter++) {
            Arrays.fill(next, 0.0);
            for (int i = 0; i < n; i++) {
                if (!active[i] || outDegree[i] == 0) continue;
                double share = rank[i] / outDegree[i];
                int start = csrIndex[i];
                int end = csrIndex[i + 1];
                for (int j = start; j < end; j++) {
                    next[csrEdges[j]] += damping * share;
                }
            }

            double leak = 0.0;
            for (int i = 0; i < n; i++) {
                if (active[i]) leak += rank[i] * (outDegree[i] == 0 ? 1.0 : 0.0);
            }

            double correction = (1.0 - damping + damping * leak) / component.size();
            for (int i = 0; i < n; i++) {
                if (active[i]) {
                    next[i] += correction;
                }
            }

            double[] tmp = rank;
            rank = next;
            next = tmp;
        }

        // 输出
        Map<Integer, Double> result = new HashMap<>();
        for (int i = 0; i < n; i++) {
            if (active[i]) {
                result.put(nodeMap[i], rank[i]);
            }
        }
        return result;
    }

    public List<double[]> minimumSpanningTree() {
        List<double[]> weightedEdges = new ArrayList<>();
        Set<String> seen = new HashSet<>();

        for (int i = 0; i < nodeMap.length; i++) {
            int u = nodeMap[i];
            for (int j = csrIndex[i]; j < csrIndex[i + 1]; j++) {
                int v = nodeMap[csrEdges[j]];
                double w = edgeWeights[j];

                // 去重无向边
                String key = u < v ? u + "_" + v : v + "_" + u;
                if (seen.add(key)) {
                    weightedEdges.add(new double[]{u, v, w});
                }
            }
        }

        weightedEdges.sort(Comparator.comparingDouble(e -> e[2]));

        Map<Integer, Integer> parent = new HashMap<>();
        for (int node : nodeMap) parent.put(node, node);

        List<double[]> mst = new ArrayList<>();
        for (double[] edge : weightedEdges) {
            int u = (int) edge[0], v = (int) edge[1];
            int pu = find(parent, u), pv = find(parent, v);
            if (pu != pv) {
                mst.add(edge);
                parent.put(pu, pv);
            }
        }
        return mst;
    }

    private int find(Map<Integer, Integer> parent, int x) {
        if (parent.get(x) != x) parent.put(x, find(parent, parent.get(x)));
        return parent.get(x);
    }

    public Map<Integer, Integer> singleSourceShortestPath(int start) {
        Map<Integer, Integer> dist = new HashMap<>();
        for (int node : nodeMap) dist.put(node, Integer.MAX_VALUE);
        dist.put(start, 0);

        PriorityQueue<int[]> pq = new PriorityQueue<>(Comparator.comparingInt(a -> a[1]));
        pq.add(new int[]{start, 0});

        while (!pq.isEmpty()) {
            int[] curr = pq.poll();
            int u = curr[0], d = curr[1];
            if (d > dist.get(u)) continue;

            for (int v : getCSRNeighbors(u)) {
                double weight = getEdgeWeight(u, v);
                int alt = d + (int)Math.round(weight);
                if (alt < dist.get(v)) {
                    dist.put(v, alt);
                    pq.add(new int[]{v, alt});
                }
            }
        }

        return dist;
    }

    public double getEdgeWeight(int fromNodeId, int toNodeId) {
        Integer fromIdx = nodeToIndex.get(fromNodeId);
        Integer toIdx = nodeToIndex.get(toNodeId);
        if (fromIdx == null || toIdx == null) return Double.POSITIVE_INFINITY;

        for (int i = csrIndex[fromIdx]; i < csrIndex[fromIdx + 1]; i++) {
            if (csrEdges[i] == toIdx) return edgeWeights[i];
        }
        return Double.POSITIVE_INFINITY;
    }

}
