package com.example.master.utils;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class DSU {
    private final Map<Integer, Integer> parent = new HashMap<>();

    public int find(int x) {
        parent.putIfAbsent(x, x);
        if (x != parent.get(x)) {
            parent.put(x, find(parent.get(x)));
        }
        return parent.get(x);
    }

    public void union(int x, int y) {
        parent.putIfAbsent(x, x);
        parent.putIfAbsent(y, y);
        int rootX = find(x), rootY = find(y);
        if (rootX != rootY) parent.put(rootX, rootY);
    }

    public Map<Integer, Set<Integer>> getComponents() {
        Map<Integer, Set<Integer>> comps = new HashMap<>();
        for (int node : parent.keySet()) {
            int root = find(node);
            comps.computeIfAbsent(root, k -> new HashSet<>()).add(node);
        }
        return comps;
    }
}