package com.example.worker;

import com.example.common.Graph;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;

@RestController
@SpringBootApplication
public class WorkerApp {

    private final Graph graph = new Graph();

    @Value("${server.port}")
    private String serverPort;

    public static void main(String[] args) {
        SpringApplication.run(WorkerApp.class, args);
    }

    // ---------- 图加载与初始化 ----------

    @PostMapping("/load")
    public ResponseEntity<String> loadGraph(@RequestBody List<int[]> edges) {
        for (int[] edge : edges) graph.addEdge(edge[0], edge[1]);
        return ResponseEntity.ok("Edges loaded.");
    }

    @PostMapping("/finalize")
    public ResponseEntity<String> finalizeGraph() {
        graph.buildCSR();
        return ResponseEntity.ok("CSR structure built.");
    }

    // ---------- 单点遍历 ----------

    @PostMapping("/bfs")
    public ResponseEntity<Set<Integer>> bfs(@RequestBody Map<String, Integer> body) {
        return ResponseEntity.ok(graph.parallelBFS(body.get("start")));
    }

    @PostMapping("/dfs")
    public ResponseEntity<Set<Integer>> dfs(@RequestBody Map<String, Integer> body) {
        return ResponseEntity.ok(graph.parallelDFS(body.get("start")));
    }

    // ---------- 全图遍历 ----------

    @PostMapping("/bfs_all")
    public ResponseEntity<Set<Integer>> bfsAll() {
        return ResponseEntity.ok(graph.fullParallelBFS());
    }

    @PostMapping("/dfs_all")
    public ResponseEntity<Set<Integer>> dfsAll() {
        return ResponseEntity.ok(graph.fullParallelDFS());
    }

    // ---------- 图分析 ----------

    @PostMapping("/components")
    public ResponseEntity<List<Set<Integer>>> connectedComponents() {
        return ResponseEntity.ok(graph.getConnectedComponentsBFS());
    }

    @GetMapping("/edges")
    public ResponseEntity<List<int[]>> getAllEdges() {
        return ResponseEntity.ok(graph.getAllEdges());
    }

    // ---------- PageRank + 写文件 (数组优化版) ----------

    @PostMapping("/pagerank_csv")
    public ResponseEntity<String> pageRankToCSV() {
        List<Set<Integer>> components = graph.getConnectedComponentsBFS();
        int maxIter = 10;
        double damping = 0.85;
        String fileName = "pagerank_worker_" + serverPort + ".csv";

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(components.size(), 4));
        List<Future<List<String>>> futures = new ArrayList<>();

        for (int cid = 0; cid < components.size(); cid++) {
            final int finalCid = cid;
            Set<Integer> comp = components.get(cid);
            futures.add(executor.submit(() -> {
                Map<Integer, Double> ranks = graph.computePageRank(comp, maxIter, damping);
                List<String> lines = new ArrayList<>();
                for (var entry : ranks.entrySet()) {
                    lines.add(entry.getKey() + "," + finalCid + "," + entry.getValue());
                }
                return lines;
            }));
        }

        List<String> allLines = new ArrayList<>();
        allLines.add("node_id,component_id,pagerank");
        for (Future<List<String>> f : futures) {
            try { allLines.addAll(f.get()); }
            catch (InterruptedException | ExecutionException e) { e.printStackTrace(); }
        }
        executor.shutdown();

        // 写入本地 CSV 文件
        try (PrintWriter writer = new PrintWriter(new File(fileName))) {
            for (String line : allLines) writer.println(line);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to write CSV");
        }

        return ResponseEntity.ok(serverPort + ": CSV written with " + (allLines.size() - 1) + " rows.");
    }

    // ---------- 获取 PageRank CSV 内容 ----------

    @GetMapping("/pagerank_csv")
    public ResponseEntity<String> getPageRankCSV() {
        String fileName = "pagerank_worker_" + serverPort + ".csv";
        try {
            String content = Files.readString(Path.of(fileName));
            return ResponseEntity.ok(content);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Error reading " + fileName);
        }
    }

    // ---------- 最小生成树 CSV ----------

    @PostMapping("/mst_csv")
    public ResponseEntity<String> mstToCSV() {
        String fileName = "mst_worker_" + serverPort + ".csv";
        List<double[]> mst = graph.minimumSpanningTree();
        try (PrintWriter writer = new PrintWriter(new File(fileName))) {
            writer.println("u,v,weight");
            for (double[] edge : mst) {
                writer.printf(Locale.US, "%d,%d,%.4f\n", (int) edge[0], (int) edge[1], edge[2]);
            }
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to write MST CSV");
        }
        return ResponseEntity.ok(serverPort + ": MST CSV written with " + mst.size() + " edges.");
    }

    @GetMapping("/mst_csv")
    public ResponseEntity<String> getMSTCSV() {
        String fileName = "mst_worker_" + serverPort + ".csv";
        try {
            String content = Files.readString(Path.of(fileName));
            return ResponseEntity.ok(content);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Error reading " + fileName);
        }
    }

    @PostMapping("/ssp_csv")
    public ResponseEntity<String> sspToCSV(@RequestBody Map<String, Integer> body) {
        int start = body.get("start");
        Map<Integer, Integer> distances = graph.singleSourceShortestPath(start);
        String fileName = "ssp_worker_" + serverPort + ".csv";
        try (PrintWriter writer = new PrintWriter(new File(fileName))) {
            writer.println("node,distance_from_" + start);
            for (Map.Entry<Integer, Integer> e : distances.entrySet()) {
                writer.println(e.getKey() + "," + e.getValue());
            }
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Failed to write SSP CSV");
        }
        return ResponseEntity.ok(serverPort + ": SSP CSV written with " + distances.size() + " entries.");
    }

    @GetMapping("/ssp_csv")
    public ResponseEntity<String> getSSPCSV() {
        String fileName = "ssp_worker_" + serverPort + ".csv";
        try {
            String content = Files.readString(Path.of(fileName));
            return ResponseEntity.ok(content);
        } catch (IOException e) {
            e.printStackTrace();
            return ResponseEntity.internalServerError().body("Error reading " + fileName);
        }
    }


    // ---------- 清理图结构 ----------

    @PostMapping("/clear")
    public ResponseEntity<String> clearGraph() {
        graph.clear();
        return ResponseEntity.ok("Graph cleared.");
    }

}