class Graph:
    def __init__(self, size):
        self.adj_matrix = [[0] * size for _ in range(size)]
        self.size = size
        self.vertex_data = [''] * size

    def add_edge(self, u, v, capacity):
        self.adj_matrix[u][v] = capacity

    def add_vertex_data(self, vertex, data):
        if 0 <= vertex < self.size:
            self.vertex_data[vertex] = data

    def bfs(self, source, sink, parent):
        visited = [False] * self.size
        queue = []
        queue.append(source)
        visited[source] = True

        while queue:
            u = queue.pop(0)

            for ind, val in enumerate(self.adj_matrix[u]):
                if not visited[ind] and val > 0:
                    queue.append(ind)
                    visited[ind] = True
                    parent[ind] = u

        return visited[sink]
    
    def edmonds_karp(self, source, sink):
        parent = [-1] * self.size
        max_flow = 0

        while self.bfs(source, sink, parent):
            path_flow = float('Inf')
            s = sink

            while s != source:
                path_flow = min(path_flow, self.adj_matrix[parent[s]][s])
                s = parent[s]

            max_flow += path_flow
            v = sink
            while v != source:
                u = parent[v]
                self.adj_matrix[u][v] -= path_flow
                self.adj_matrix[v][u] += path_flow
                v = parent[v]

            path = []
            v = sink
            while v != source:
                path.append(v)
                v = parent[v]
            path.append(source)
            path.reverse()
            # path_names = [self.vertex_data[node] for node in path]
            # print("path: ", " -> ".join(path_names), ", flow: ", path_flow)

        return max_flow


# Time and Space Complexity Analysis
if __name__ == "__main__":
    import time
    import tracemalloc
    import matplotlib.pyplot as plt
    import random
    import statistics
    
    # ===== BASIC FUNCTIONALITY TEST =====
    g = Graph(6)
    vertex_names = ['s', 'v1', 'v2', 'v3', 'v4', 't']
    for i, name in enumerate(vertex_names):
        g.add_vertex_data(i, name)

    g.add_edge(0, 1, 3)  # s  -> v1, cap: 3
    g.add_edge(0, 2, 7)  # s  -> v2, cap: 7
    g.add_edge(1, 3, 3)  # v1 -> v3, cap: 3
    g.add_edge(1, 4, 4)  # v1 -> v4, cap: 4
    g.add_edge(2, 1, 5)  # v2 -> v1, cap: 5
    g.add_edge(2, 4, 3)  # v2 -> v4, cap: 3
    g.add_edge(3, 4, 3)  # v3 -> v4, cap: 3
    g.add_edge(3, 5, 2)  # v3 -> t,  cap: 2
    g.add_edge(4, 5, 6)  # v4 -> t,  cap: 6

    source = 0
    sink = 5
    max_flow = g.edmonds_karp(source, sink)
    print(f"Maximum flow: {max_flow} (Expected: 8)")
    

    # ===== TESTING WITH VARYING GRAPH SIZES AND RANDOMIZED EDGES =====
    print("\n2. Complexity Analysis with Varying Graph Sizes:")
    print("-" * 80)
    
    # The graph sizes that are tested
    graph_sizes = [5, 10, 20, 30, 40, 50, 75, 100, 200, 400]
    # Runs per size for average
    num_runs = 10
    
    results = {
        'sizes': [],
        'vertices': [],
        'edges': [],
        'times': [],
        'memory': [],
        'max_flows': [],
        'stdev_times': [],
        'stdev_memory': []
    }
    
    for size in graph_sizes:
        print(f"\nTesting graph with {size} vertices")
        # Create a SINGLE random graph for this size (used for all runs)
        source = 0
        sink = size - 1
        
        # Generate graph structure once
        edge_list = []
        edge_count = 0
        for u in range(size - 1):
            # Ensure at least some forward edges exist
            for v in range(u + 1, size):
                if random.random() < 0.3:  # 30% edge probability
                    capacity = random.randint(1, 20)
                    edge_list.append((u, v, capacity))
                    edge_count += 1
        
        # Ensure source has outgoing edges
        if edge_count == 0:
            for v in range(1, min(4, size)):
                capacity = random.randint(5, 15)
                edge_list.append((source, v, capacity))
                edge_count += 1
        
        run_times = []
        run_memory = []
        
        # Run the algorithm multiple times on the SAME graph structure
        for run in range(num_runs):
            # Create fresh copy of the SAME graph for each run
            g = Graph(size)
            for u, v, capacity in edge_list:
                g.add_edge(u, v, capacity)
            
            # Measure time, start memory tracking
            tracemalloc.start()
            start_time = time.perf_counter()
            max_flow = g.edmonds_karp(source, sink)
            elapsed_time = time.perf_counter() - start_time
            
            # Measure memory
            current, peak = tracemalloc.get_traced_memory()
            tracemalloc.stop()
            
            run_times.append(elapsed_time)
            run_memory.append(peak / 1024)  # Convert to KB
        
        # Calculate averages and standard deviations
        avg_time = statistics.mean(run_times)
        avg_memory = statistics.mean(run_memory)
        stdev_time = statistics.stdev(run_times) if num_runs > 1 else 0.0
        stdev_memory = statistics.stdev(run_memory) if num_runs > 1 else 0.0
        
        results['sizes'].append(size)
        results['vertices'].append(size)
        results['edges'].append(edge_count)
        results['times'].append(avg_time)
        results['memory'].append(avg_memory)
        results['max_flows'].append(max_flow)
        results['stdev_times'].append(stdev_time)
        results['stdev_memory'].append(stdev_memory)
        
        print(f"  Vertices: {size}, Edges: ~{edge_count}")
        print(f"  Avg Time: {avg_time:.4f}s (±{stdev_time:.4f}s)")
        print(f"  Avg Memory: {avg_memory:.2f} KB (±{stdev_memory:.2f} KB)")
        print(f"  Max Flow: {max_flow}")
    
    # Display results table
    print("\n3. Summary Table:")
    print("-" * 110)
    print(f"{'V':<8} {'E':<8} {'Time (s)':<12} {'Std Dev (s)':<13} {'Memory (KB)':<13} {'Std Dev (KB)':<14} {'Max Flow':<10}")
    print("-" * 110)
    for i in range(len(results['sizes'])):
        print(f"{results['vertices'][i]:<8} {results['edges'][i]:<8} "
              f"{results['times'][i]:<12.6f} {results['stdev_times'][i]:<13.6f} "
              f"{results['memory'][i]:<13.2f} {results['stdev_memory'][i]:<14.2f} "
              f"{results['max_flows'][i]:<10}")
    
    # Create visualizations
    print("\n4. Generating Visualizations:")
    print("-" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle('Edmonds-Karp Algorithm - Complexity Analysis', fontsize=16, fontweight='bold')
    
    # Plot 1: Time vs Vertices
    ax1 = axes[0, 0]
    ax1.plot(results['vertices'], results['times'], 'b-o', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Vertices (V)', fontsize=11)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=11)
    ax1.set_title('Time Complexity: O(V·E²)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale('log')
    
    # Plot 2: Memory vs Vertices
    ax2 = axes[0, 1]
    ax2.plot(results['vertices'], results['memory'], 'r-s', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Vertices (V)', fontsize=11)
    ax2.set_ylabel('Peak Memory (KB)', fontsize=11)
    ax2.set_title('Space Complexity: O(V²)', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)
    
    # Plot 3: Time vs Edges
    ax3 = axes[1, 0]
    ax3.scatter(results['edges'], results['times'], c=results['vertices'], 
                cmap='viridis', s=100, alpha=0.7)
    ax3.set_xlabel('Number of Edges (E)', fontsize=11)
    ax3.set_ylabel('Execution Time (seconds)', fontsize=11)
    ax3.set_title('Time vs Edge Count (colored by vertex count)', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)
    cbar = plt.colorbar(ax3.collections[0], ax=ax3)
    cbar.set_label('Vertices', fontsize=10)
    
    # Plot 4: Actual vs Theoretical Complexity
    ax4 = axes[1, 1]
    # Calculate theoretical complexity proportions
    theoretical = [v * e * e for v, e in zip(results['vertices'], results['edges'])]
    
    # Normalize to similar scale (divide by constant, not fitting perfectly)
    if len(theoretical) > 0 and len(results['times']) > 0:
        # Scale theoretical to be roughly in the same range as actual times
        scale_factor = results['times'][0] / theoretical[0] if theoretical[0] > 0 else 1
        theoretical_scaled = [t * scale_factor for t in theoretical]
        
        ax4.plot(results['vertices'], results['times'], 'b-o', 
                 label='Actual Time', linewidth=2, markersize=8)
        ax4.plot(results['vertices'], theoretical_scaled, 'r--^', 
                 label='O(V·E²) Theoretical', linewidth=2, markersize=6, alpha=0.7)
        ax4.set_xlabel('Number of Vertices (V)', fontsize=11)
        ax4.set_ylabel('Time (seconds, normalized scale)', fontsize=11)
        ax4.set_title('Actual vs Theoretical Complexity', fontsize=12, fontweight='bold')
        ax4.legend(fontsize=10)
        ax4.grid(True, alpha=0.3)
        ax4.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig('edmondskarp_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Visualization saved as 'edmondskarp_analysis.png'")
    
    # Display complexity summary
    print("\n5. Complexity Summary:")
    print("-" * 80)
    print("Time Complexity:  O(V·E²)")
    print("  - V: number of vertices")
    print("  - E: number of edges")
    print("  - Each BFS takes O(V + E) ≈ O(E) for dense graphs")
    print("  - Max flow can require O(E·f) iterations (f = max flow)")
    print("  - Worst case: O(V·E²)")
    print("\nSpace Complexity: O(V²)")
    print("  - Adjacency matrix: V² integers")
    print("  - Parent array: V integers")
    print("  - Visited array: V booleans")
    print("  - Queue: O(V) worst case")
    print("  - Total: O(V²) dominated by adjacency matrix")
    print("\n" + "="*80)
    
    plt.show()
