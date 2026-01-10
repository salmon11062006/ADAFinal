# Python implementation of Dinic's Algorithm
class Edge:
    def __init__(self, v, flow, C, rev):
        self.v = v
        self.flow = flow
        self.C = C
        self.rev = rev

# Residual Graph
class Graph:
    def __init__(self, V):
        self.adj = [[] for i in range(V)]
        self.V = V
        self.level = [0 for i in range(V)]
        self.vertex_data = [''] * V  # For compatibility with main.py

    def add_vertex_data(self, vertex, data):
        """Add vertex label/name for compatibility"""
        if 0 <= vertex < self.V:
            self.vertex_data[vertex] = data

    # add edge to the graph
    def addEdge(self, u, v, C):

        # Forward edge : 0 flow and C capacity
        a = Edge(v, 0, C, len(self.adj[v]))

        # Back edge : 0 flow and 0 capacity
        b = Edge(u, 0, 0, len(self.adj[u]))
        self.adj[u].append(a)
        self.adj[v].append(b)

    # Finds if more flow can be sent from s to t
    # Also assigns levels to nodes
    def BFS(self, s, t):
        for i in range(self.V):
            self.level[i] = -1

        # Level of source vertex
        self.level[s] = 0

        # Create a queue, enqueue source vertex
        # and mark source vertex as visited here
        # level[] array works as visited array also
        q = []
        q.append(s)
        while q:
            u = q.pop(0)
            for i in range(len(self.adj[u])):
                e = self.adj[u][i]
                if self.level[e.v] < 0 and e.flow < e.C:

                    # Level of current vertex is
                    # level of parent + 1
                    self.level[e.v] = self.level[u]+1
                    q.append(e.v)

        # If we can not reach to the sink we
        # return False else True
        return False if self.level[t] < 0 else True

# A DFS based function to send flow after BFS has
# figured out that there is a possible flow and
# constructed levels. This functions called multiple
# times for a single call of BFS.
# flow : Current flow send by parent function call
# start[] : To keep track of next edge to be explored
#           start[i] stores count of edges explored
#           from i
# u : Current vertex
# t : Sink
    def sendFlow(self, u, flow, t, start):
        # Sink reached
        if u == t:
            return flow

        # Traverse all adjacent edges one -by -one
        while start[u] < len(self.adj[u]):

            # Pick next edge from adjacency list of u
            e = self.adj[u][start[u]]
            if self.level[e.v] == self.level[u]+1 and e.flow < e.C:

                # find minimum flow from u to t
                curr_flow = min(flow, e.C-e.flow)
                temp_flow = self.sendFlow(e.v, curr_flow, t, start)

                # flow is greater than zero
                if temp_flow and temp_flow > 0:

                    # add flow to current edge
                    e.flow += temp_flow

                    # subtract flow from reverse edge
                    # of current edge
                    self.adj[e.v][e.rev].flow -= temp_flow
                    return temp_flow
            start[u] += 1

    # Returns maximum flow in graph
    def DinicMaxflow(self, s, t):

        # Corner case
        if s == t:
            return -1

        # Initialize result
        total = 0

        # Augument the flow while there is path
        # from source to sink
        while self.BFS(s, t) == True:

            # store how many edges are visited
            # from V { 0 to V }
            start = [0 for i in range(self.V+1)]
            while True:
                flow = self.sendFlow(s, float('inf'), t, start)
                if not flow:
                    break

                # Add path flow to overall flow
                total += flow

        # return maximum flow
        return total


# Time and Space Complexity Analysis
if __name__ == "__main__":
    import time
    import tracemalloc
    import matplotlib.pyplot as plt
    import random
    import statistics
    
    print("="*80)
    print("DINIC'S ALGORITHM - COMPLEXITY ANALYSIS")
    print("="*80)
    
    # First, run a simple example to verify correctness
    print("\n1. Verification with Example Graph:")
    print("-" * 80)
    g = Graph(6)
    source = 0
    sink = 5
    
    # Same graph as Edmonds-Karp example for comparison
    g.addEdge(0, 1, 3)  # s  -> v1, cap: 3
    g.addEdge(0, 2, 7)  # s  -> v2, cap: 7
    g.addEdge(1, 3, 3)  # v1 -> v3, cap: 3
    g.addEdge(1, 4, 4)  # v1 -> v4, cap: 4
    g.addEdge(2, 1, 5)  # v2 -> v1, cap: 5
    g.addEdge(2, 4, 3)  # v2 -> v4, cap: 3
    g.addEdge(3, 4, 3)  # v3 -> v4, cap: 3
    g.addEdge(3, 5, 2)  # v3 -> t,  cap: 2
    g.addEdge(4, 5, 6)  # v4 -> t,  cap: 6
    
    max_flow = g.DinicMaxflow(source, sink)
    print(f"Maximum flow: {max_flow} (Expected: 8)")
    
    # Now test with varying graph sizes
    print("\n2. Complexity Analysis with Varying Graph Sizes:")
    print("-" * 80)
    
    graph_sizes = [5, 10, 20, 30, 40, 50, 75, 100, 200, 400]
    # Runs per size for averaging
    num_runs = 10
    
    results = {
        'sizes': [],
        'vertices': [],
        'edges': [],
        'times': [],
        'memory': [],
        'max_flows': []
    }
    
    for size in graph_sizes:
        print(f"\nTesting graph with {size} vertices...")
        
        # Create a SINGLE random graph for this size (used for all runs)
        source = 0
        sink = size - 1
        
        # Generate graph structure once
        edge_list = []
        edge_count = 0
        for u in range(size - 1):
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
                g.addEdge(u, v, capacity)
            
            # Start memory tracking ONLY for algorithm execution
            tracemalloc.start()
            start_time = time.perf_counter()
            max_flow = g.DinicMaxflow(source, sink)
            elapsed_time = time.perf_counter() - start_time
            
            # Measure memory used by algorithm only
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
        
        print(f"  Vertices: {size}, Edges: ~{edge_count}")
        print(f"  Avg Time: {avg_time:.4f}s (±{stdev_time:.4f}s)")
        print(f"  Avg Memory: {avg_memory:.2f} KB (±{stdev_memory:.2f} KB)")
        print(f"  Max Flow: {max_flow}")
    
    # Display results table
    print("\n3. Summary Table:")
    print("-" * 80)
    print(f"{'V':<8} {'E':<8} {'Time (s)':<12} {'Memory (KB)':<15} {'Max Flow':<10}")
    print("-" * 80)
    for i in range(len(results['sizes'])):
        print(f"{results['vertices'][i]:<8} {results['edges'][i]:<8} "
              f"{results['times'][i]:<12.6f} {results['memory'][i]:<15.2f} "
              f"{results['max_flows'][i]:<10}")
    
    # Create visualizations
    print("\n4. Generating Visualizations...")
    print("-" * 80)
    
    fig, axes = plt.subplots(2, 2, figsize=(14, 10))
    fig.suptitle("Dinic's Algorithm - Complexity Analysis", fontsize=16, fontweight='bold')
    
    # Plot 1: Time vs Vertices
    ax1 = axes[0, 0]
    ax1.plot(results['vertices'], results['times'], 'b-o', linewidth=2, markersize=8)
    ax1.set_xlabel('Number of Vertices (V)', fontsize=11)
    ax1.set_ylabel('Execution Time (seconds)', fontsize=11)
    ax1.set_title('Time Complexity: O(V²·E)', fontsize=12, fontweight='bold')
    ax1.grid(True, alpha=0.3)
    ax1.set_yscale('log')
    
    # Plot 2: Memory vs Vertices
    ax2 = axes[0, 1]
    ax2.plot(results['vertices'], results['memory'], 'r-s', linewidth=2, markersize=8)
    ax2.set_xlabel('Number of Vertices (V)', fontsize=11)
    ax2.set_ylabel('Peak Memory (KB)', fontsize=11)
    ax2.set_title('Space Complexity: O(V + E)', fontsize=12, fontweight='bold')
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
    theoretical = [v * v * e for v, e in zip(results['vertices'], results['edges'])]
    
    # Normalize to similar scale (divide by constant, not fitting perfectly)
    if len(theoretical) > 0 and len(results['times']) > 0:
        # Scale theoretical to be roughly in the same range as actual times
        scale_factor = results['times'][0] / theoretical[0] if theoretical[0] > 0 else 1
        theoretical_scaled = [t * scale_factor for t in theoretical]
        
        ax4.plot(results['vertices'], results['times'], 'b-o', 
                 label='Actual Time', linewidth=2, markersize=8)
        ax4.plot(results['vertices'], theoretical_scaled, 'r--^', 
                 label='O(V²·E) Theoretical', linewidth=2, markersize=6, alpha=0.7)
        ax4.set_xlabel('Number of Vertices (V)', fontsize=11)
        ax4.set_ylabel('Time (seconds, normalized scale)', fontsize=11)
        ax4.set_title('Actual vs Theoretical Complexity', fontsize=12, fontweight='bold')
        ax4.legend(fontsize=10)
        ax4.grid(True, alpha=0.3)
        ax4.set_yscale('log')
    
    plt.tight_layout()
    plt.savefig('dinic_analysis.png', dpi=300, bbox_inches='tight')
    print("✓ Visualization saved as 'dinic_analysis.png'")
    
    # Display complexity summary
    print("\n5. Complexity Summary:")
    print("-" * 80)
    print("Time Complexity:  O(V²·E)")
    print("  - V: number of vertices")
    print("  - E: number of edges")
    print("  - Each BFS takes O(E) time")
    print("  - DFS phase takes O(V·E) per level")
    print("  - At most V levels (blocking flows)")
    print("  - Total: O(V²·E) worst case")
    print("\nSpace Complexity: O(V + E)")
    print("  - Adjacency list: O(V + E) for edges")
    print("  - Level array: V integers")
    print("  - Start array: V integers")
    print("  - BFS queue: O(V) worst case")
    print("  - Total: O(V + E) more space-efficient than adjacency matrix")
    print("\n" + "="*80)
    
    plt.show()
