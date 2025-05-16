package com.example.master;

import com.example.master.utils.DSU;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

@SpringBootApplication
public class MasterApp {

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        SpringApplication.run(MasterApp.class, args);

        List<String> resultLines = new ArrayList<>();
        resultLines.add("workers,nodes,bfs_time,dfs_time,pagerank_time,mst_time,shortest_path_time");

        // 定义不同规模测试
        int[] nodeCounts = {100000, 200000, 500000, 1000000};
        int avgDegree = 3;

        List<String> allWorkers = List.of(
                "http://localhost:8081",
                "http://localhost:8082",
                "http://localhost:8083",
                "http://localhost:8084",
                "http://localhost:8085",
                "http://localhost:8086",
                "http://localhost:8087",
                "http://localhost:8088",
                "http://localhost:8089",
                "http://localhost:8090",
                "http://localhost:8091",
                "http://localhost:8092",
                "http://localhost:8093",
                "http://localhost:8094",
                "http://localhost:8095",
                "http://localhost:8096"
        );
        // 遍历不同 Worker 数量
        for (int numWorkers = 1; numWorkers <= 16; numWorkers++) {
            List<String> workers = allWorkers.subList(0, numWorkers);

            System.out.println("\n====== Testing with " + numWorkers + " workers ======");

            for (int totalNodes : nodeCounts) {
                Map<Integer, List<int[]>> assignments = generateConnectedGraphEdges(
                        totalNodes, avgDegree, workers.size());

                for (int i = 0; i < workers.size(); i++) {
                    sendGraphToWorker(workers.get(i), assignments.get(i));
                    finalizeWorker(workers.get(i));
                }

                // 触发 BFS 测试
                System.out.println("\n=== Testing BFS for " + totalNodes + " nodes ===");
                long bfsStart = System.currentTimeMillis();
                parallelTraverse(workers, "/bfs_all");
                long bfsEnd = System.currentTimeMillis();
                System.out.printf("BFS time for %d nodes with %d workers: %.2f seconds\n",
                        totalNodes, numWorkers, (bfsEnd - bfsStart) / 1000.0);

                // 触发 DFS 测试
                System.out.println("\n=== Testing DFS for " + totalNodes + " nodes ===");
                long dfsStart = System.currentTimeMillis();
                parallelTraverse(workers, "/dfs_all");
                long dfsEnd = System.currentTimeMillis();
                System.out.printf("DFS time for %d nodes with %d workers: %.2f seconds\n",
                        totalNodes, numWorkers, (dfsEnd - dfsStart) / 1000.0);

                // 触发 PageRank 测试
                System.out.println("\n=== Testing PageRank for " + totalNodes + " nodes ===");
                long prStart = System.currentTimeMillis();
                for (String worker : workers) {
                    triggerPageRankCSV(worker);
                }
                aggregatePageRankCSVs(workers);
                long prEnd = System.currentTimeMillis();

                System.out.printf("Total PageRank time for %d nodes with %d workers: %.2f seconds\n",
                        totalNodes, numWorkers, (prEnd - prStart) / 1000.0);

                // 触发 MST 测试
                System.out.println("\n=== Testing MST for " + totalNodes + " nodes ===");
                long mstStart = System.currentTimeMillis();
                for (String worker : workers) {
                    triggerMSTCSV(worker);
                }
                aggregateMSTCSVs(workers);
                long mstEnd = System.currentTimeMillis();
                System.out.printf("Total MST time for %d nodes with %d workers: %.2f seconds\n",
                        totalNodes, numWorkers, (mstEnd - mstStart) / 1000.0);

                // 触发 ShortestPath 测试
                System.out.println("\n=== Testing ShortestPath for " + totalNodes + " nodes ===");
                int source = 0;
                long sspStart = System.currentTimeMillis();
                for (String worker : workers) {
                    triggerSSPCSV(worker, source);
                }
                aggregateSSPCSVs(workers);
                long sspEnd = System.currentTimeMillis();
                System.out.printf("Total ShortestPath time for %d nodes with %d workers: %.2f seconds\n",
                        totalNodes, numWorkers, (sspEnd - sspStart) / 1000.0);

                String line = String.format(Locale.US,
                        "%d,%d,%.3f,%.3f,%.3f,%.3f,%.3f",
                        numWorkers, totalNodes,
                        (bfsEnd - bfsStart) / 1000.0,
                        (dfsEnd - dfsStart) / 1000.0,
                        (prEnd - prStart) / 1000.0,
                        (mstEnd - mstStart) / 1000.0,
                        (sspEnd - sspStart) / 1000.0);
                resultLines.add(line);
                System.out.println(">>> " + line);

                for (String worker : workers) {
                    clearWorkerGraph(worker);
                }
            }
        }
        try (PrintWriter writer = new PrintWriter(new File("timing_results.csv"))) {
            for (String line : resultLines) {
                writer.println(line);
            }
            System.out.println("Exported timing_results.csv");
        } catch (IOException e) {
            e.printStackTrace();
        }
        try {
            Process p = new ProcessBuilder("python", "plot_timing.py")
                    .inheritIO()
                    .start();
            p.waitFor();
            System.out.println("Generated timing plots.");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void sendGraphToWorker(String url, List<int[]> edges) {
        RestTemplate rest = new RestTemplate();
        rest.postForEntity(url + "/load", edges, String.class);
    }

    private static void finalizeWorker(String url) {
        RestTemplate rest = new RestTemplate();
        rest.postForEntity(url + "/finalize", null, String.class);
    }

    private static void clearWorkerGraph(String url) {
        RestTemplate rest = new RestTemplate();
        rest.postForEntity(url + "/clear", null, String.class);
    }

    private static Set<Integer> parallelTraverse(List<String> workers, String path) throws ExecutionException, InterruptedException {
        ExecutorService exec = Executors.newFixedThreadPool(workers.size());
        List<Future<Set<Integer>>> futures = new ArrayList<>();
        for (String w : workers) {
            futures.add(exec.submit(() -> fetchSet(w + path)));
        }
        Set<Integer> total = new HashSet<>();
        for (Future<Set<Integer>> f : futures) total.addAll(f.get());
        exec.shutdown();
        return total;
    }

    private static Set<Integer> fetchSet(String url) {
        RestTemplate rest = new RestTemplate();
        ResponseEntity<Set> resp = rest.postForEntity(url, null, Set.class);
        Set<Integer> body = resp.getBody();
        return body != null ? body : Set.of();
    }

    private static List<int[]> fetchEdges(String url) {
        RestTemplate rest = new RestTemplate();
        ResponseEntity<List> resp = rest.getForEntity(url + "/edges", List.class);
        List<int[]> edges = new ArrayList<>();
        List<?> raw = resp.getBody();
        if (raw != null) {
            for (Object o : raw) {
                @SuppressWarnings("unchecked")
                List<Integer> e = (List<Integer>) o;
                edges.add(new int[]{e.get(0), e.get(1)});
            }
        }
        return edges;
    }

    private static void writeComponentsCSV(Map<Integer, Set<Integer>> components) {
        try (PrintWriter writer = new PrintWriter(new File("components.csv"))) {
            writer.println("component_id,size,hash_summary");
            for (var entry : components.entrySet()) {
                int id = entry.getKey();
                int size = entry.getValue().size();
                long hash = entry.getValue().stream().mapToLong(i -> i).sum();
                writer.println(id + "," + size + "," + hash);
            }
            System.out.println("Exported components.csv with " + components.size() + " components");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void triggerPageRankCSV(String url) {
        RestTemplate rest = new RestTemplate();
        rest.postForEntity(url + "/pagerank_csv", null, String.class);
    }

    private static void aggregatePageRankCSVs(List<String> workers) {
        RestTemplate rest = new RestTemplate();
        List<String> aggregated = new ArrayList<>();
        boolean first = true;
        for (String worker : workers) {
            String csv = rest.getForObject(worker + "/pagerank_csv", String.class);
            if (csv == null) continue;
            String[] lines = csv.split("\\r?\\n");
            for (int i = 0; i < lines.length; i++) {
                if (i == 0) {
                    if (first) { aggregated.add(lines[i]); first = false; }
                } else {
                    aggregated.add(lines[i]);
                }
            }
        }
        try (PrintWriter writer = new PrintWriter(new File("pagerank.csv"))) {
            for (String line : aggregated) writer.println(line);
            System.out.println("Exported pagerank.csv with " + (aggregated.size()-1) + " rows");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Map<Integer, List<int[]>> generateConnectedGraphEdges(
            int totalNodes, int avgDegree, int numWorkers) {

        Map<Integer, List<int[]>> workerEdges = new HashMap<>();
        for (int i = 0; i < numWorkers; i++) workerEdges.put(i, new ArrayList<>());
        Random rand = new Random();

        // 1. 生成一棵连通图的生成树（保证连通）
        for (int i = 1; i < totalNodes; i++) {
            int u = i;
            int v = rand.nextInt(i); // 保证 u 和 0~i-1 中的某个点连接
            int w = u % numWorkers;
            workerEdges.get(w).add(new int[]{u, v});
        }

        // 2. 随机补充边（增强稀疏图的结构复杂性）
        for (int u = 0; u < totalNodes; u++) {
            int deg = rand.nextInt(avgDegree); // 控制在树结构基础上补边
            for (int j = 0; j < deg; j++) {
                int v = rand.nextInt(totalNodes);
                if (u != v) {
                    int w = u % numWorkers;
                    workerEdges.get(w).add(new int[]{u, v});
                }
            }
        }

        // 3. 添加自环，保证每个点有出边
        for (int i = 0; i < totalNodes; i++) {
            int w = i % numWorkers;
            workerEdges.get(w).add(new int[]{i, i});
        }

        return workerEdges;
    }

    public static Map<Integer, List<int[]>> generateComponentAssignedEdges(
            int totalNodes, int numComponents, int avgDegree, int numWorkers) {
        Map<Integer, List<int[]>> workerEdges = new HashMap<>();
        for (int i = 0; i < numWorkers; i++) workerEdges.put(i, new ArrayList<>());
        Random rand = new Random();
        int nodesPerComp = totalNodes / numComponents;
        for (int c = 0; c < numComponents; c++) {
            int off = c * nodesPerComp;
            int w = c % numWorkers;
            var compEdges = workerEdges.get(w);
            for (int i = 0; i < nodesPerComp; i++) {
                int u = off + i;
                int deg = rand.nextInt(avgDegree) + 1;
                for (int j = 0; j < deg; j++) {
                    int v = off + rand.nextInt(nodesPerComp);
                    if (u != v) compEdges.add(new int[]{u, v});
                }
            }
            for (int i = 0; i < nodesPerComp; i++) {
                compEdges.add(new int[]{off + i, off + i});
            }
        }
        return workerEdges;
    }

    private static void triggerMSTCSV(String url) {
        RestTemplate rest = new RestTemplate();
        rest.postForEntity(url + "/mst_csv", null, String.class);
    }

    private static void aggregateMSTCSVs(List<String> workers) {
        RestTemplate rest = new RestTemplate();
        List<String> aggregated = new ArrayList<>();
        boolean first = true;
        for (String worker : workers) {
            String csv = rest.getForObject(worker + "/mst_csv", String.class);
            if (csv == null) continue;
            String[] lines = csv.split("\\r?\\n");
            for (int i = 0; i < lines.length; i++) {
                if (i == 0 && first) {
                    aggregated.add(lines[i]);
                    first = false;
                } else if (i > 0) {
                    aggregated.add(lines[i]);
                }
            }
        }
        try (PrintWriter writer = new PrintWriter(new File("mst.csv"))) {
            for (String line : aggregated) writer.println(line);
            System.out.println("Exported mst.csv with " + (aggregated.size() - 1) + " edges");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    private static void triggerSSPCSV(String url, int startNode) {
        RestTemplate rest = new RestTemplate();
        Map<String, Integer> body = Map.of("start", startNode);
        rest.postForEntity(url + "/ssp_csv", body, String.class);
    }

    private static void aggregateSSPCSVs(List<String> workers) {
        RestTemplate rest = new RestTemplate();
        List<String> allLines = new ArrayList<>();
        boolean first = true;
        for (String worker : workers) {
            String csv = rest.getForObject(worker + "/ssp_csv", String.class);
            if (csv == null) continue;
            String[] lines = csv.split("\\r?\\n");
            for (int i = 0; i < lines.length; i++) {
                if (i == 0 && first) {
                    allLines.add(lines[i]);
                    first = false;
                } else if (i > 0) {
                    allLines.add(lines[i]);
                }
            }
        }
        try (PrintWriter writer = new PrintWriter(new File("ssp.csv"))) {
            for (String line : allLines) writer.println(line);
            System.out.println("Exported ssp.csv with " + (allLines.size() - 1) + " rows");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


}