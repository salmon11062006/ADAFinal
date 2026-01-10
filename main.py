from StopNetwork import StopNetwork
import time
import statistics
import tracemalloc

def main():
    # Initialize bus network
    print("="*80)
    print("TransJakarta BRT Max Flow Analysis")
    print("="*80)
    
    network = StopNetwork(gtfs_path='gtfs/')
    
    # Filter for major BRT lines 1-14
    brt_lines = list(range(1, 15))
    file_suffix_name = 'BRT_1-14'
    network.load_data(filter_brt_lines=brt_lines, save_filtered=True, output_suffix=file_suffix_name)
    
    # ========== CONFIGURE PARAMETERS HERE ==========
    
    # Assumed bus capacity
    bus_capacity = 85  # passengers per bus
    
    # Transfer constraints
    min_transfer_time = 120   # Minimum 2 minutes to transfer (120 seconds)
    max_transfer_time = 1800  # Maximum 30 minutes wait (1800 seconds)
    
    dict_of_routes = {'Pasar Senen ~ Monumen Nasional': ['G00106', 'P00017'], 'Kali Besar ~ Mangga Dua':['G00060', 'G00575'], 
                      'Bidara Cina ~ Makasar':['G00352', 'G00002']}

    # Time window for analysis
    time_start = '16:00:00'  
    time_end = '18:00:00'  
     
    # Source and destination stops
    route_name = 'Bidara Cina ~ Makasar' # Refer to the keys in the dictionary above, or create your own routes by referring to the GTFS data

    # Number of runs for averaging
    num_runs = 5 

    source_stop_id = dict_of_routes[route_name][0]
    dest_stop_id = dict_of_routes[route_name][1]

    # ================================================
    
    print("\n" + "="*80)
    print("Building Flow Network Graph")
    print("="*80)
    
    # Build the flow graph
    graph, node_to_index, index_to_node, source_idx, sink_idx = network.build_flow_graph(
        source_stop=source_stop_id,
        dest_stop=dest_stop_id,
        bus_capacity=bus_capacity,
        time_window_start=time_start,
        time_window_end=time_end,
        min_transfer_time=min_transfer_time,
        max_transfer_time=max_transfer_time
    )
    
    # Get stop names
    source_name = network.stop_info.get(source_stop_id, {}).get('name', source_stop_id)
    dest_name = network.stop_info.get(dest_stop_id, {}).get('name', dest_stop_id)
    
    print(f"\nNetwork: {source_name} → {dest_name}")
    print(f"Nodes: {graph.size}")
    
    # IF Edmonds-Karp is not runned: return dummy values
    max_flow_ek = 0
    ek_time = 1.0

    # ========== RUN EDMONDS-KARP ==========
    print("\n" + "="*80)
    print("1. EDMONDS-KARP ALGORITHM")
    print("="*80)
    print(f"From: {source_name} ({source_stop_id})")
    print(f"To: {dest_name} ({dest_stop_id})")
    print(f"Time window: {time_start} - {time_end}")
    print(f"Running {num_runs} times for average...")
    print("="*80 + "\n")
    
    # Deep copy the graph for Edmonds-Karp
    from edmondskarp import Graph as GraphEK
    
    ek_times = []
    ek_memory = []
    for run in range(num_runs):
        print(f"Edmonds-Karp run {run + 1}/{num_runs}...", end=" ", flush=True)
        
        # Start memory tracking
        tracemalloc.start()
        
        # Create fresh copy for each run
        graph_ek_copy = GraphEK(graph.size)
        for i in range(graph.size):
            for j in range(graph.size):
                if graph.adj_matrix[i][j] > 0:
                    graph_ek_copy.adj_matrix[i][j] = graph.adj_matrix[i][j]
            graph_ek_copy.vertex_data[i] = graph.vertex_data[i]
        
        start_time = time.perf_counter()
        max_flow_ek = graph_ek_copy.edmonds_karp(source_idx, sink_idx)
        elapsed = time.perf_counter() - start_time
        
        # Get peak memory usage
        current, peak = tracemalloc.get_traced_memory()
        ek_memory.append(peak / 1024 / 1024)  # Convert to MB
        tracemalloc.stop()
        
        ek_times.append(elapsed)
        print(f"{elapsed:.4f}s, {peak / 1024 / 1024:.2f} MB")
    
    # Calculate statistics
    mean_ek_time = statistics.mean(ek_times)
    median_ek_time = statistics.median(ek_times)
    stdev_ek_time = statistics.stdev(ek_times) if num_runs > 1 else 0.0
    min_ek_time = min(ek_times)
    max_ek_time = max(ek_times)
    
    mean_ek_memory = statistics.mean(ek_memory)
    median_ek_memory = statistics.median(ek_memory)
    stdev_ek_memory = statistics.stdev(ek_memory) if num_runs > 1 else 0.0
    min_ek_memory = min(ek_memory)
    max_ek_memory = max(ek_memory)
    
    print(f"\nEdmonds-Karp Results:")
    print(f"  Max Flow: {max_flow_ek:,} passengers")
    print(f"  Mean Runtime: {mean_ek_time:.4f}s")
    print(f"  Median Runtime: {median_ek_time:.4f}s")
    print(f"  Std Dev: {stdev_ek_time:.4f}s")
    print(f"  Min: {min_ek_time:.4f}s | Max: {max_ek_time:.4f}s")
    print(f"  Mean Memory: {mean_ek_memory:.2f} MB")
    print(f"  Median Memory: {median_ek_memory:.2f} MB")
    print(f"  Std Dev: {stdev_ek_memory:.2f} MB")
    print(f"  Min: {min_ek_memory:.2f} MB | Max: {max_ek_memory:.2f} MB")

    # ========== RUN DINIC ==========
    print("\n" + "="*80)
    print("2. DINIC'S ALGORITHM")
    print("="*80)
    print(f"From: {source_name} ({source_stop_id})")
    print(f"To: {dest_name} ({dest_stop_id})")
    print(f"Time window: {time_start} - {time_end}")
    print(f"Running {num_runs} times for average...")
    print("="*80 + "\n")
    
    # Build Dinic graph from adjacency matrix
    from dinic import Graph as GraphDinic
    
    dinic_times = []
    dinic_memory = []
    for run in range(num_runs):
        print(f"Dinic run {run + 1}/{num_runs}...", end=" ", flush=True)
        
        # Start memory tracking
        tracemalloc.start()
        
        # Create fresh copy for each run
        graph_dinic = GraphDinic(graph.size)
        for i in range(graph.size):
            for j in range(graph.size):
                if graph.adj_matrix[i][j] > 0:
                    graph_dinic.addEdge(i, j, graph.adj_matrix[i][j])
            graph_dinic.vertex_data[i] = graph.vertex_data[i]
        
        start_time = time.perf_counter()
        max_flow_dinic = graph_dinic.DinicMaxflow(source_idx, sink_idx)
        elapsed = time.perf_counter() - start_time
        
        # Get peak memory usage
        current, peak = tracemalloc.get_traced_memory()
        dinic_memory.append(peak / 1024 / 1024)  # Convert to MB
        tracemalloc.stop()
        
        dinic_times.append(elapsed)
        print(f"{elapsed:.4f}s, {peak / 1024 / 1024:.2f} MB")
    
    # Calculate statistics
    mean_dinic_time = statistics.mean(dinic_times)
    median_dinic_time = statistics.median(dinic_times)
    stdev_dinic_time = statistics.stdev(dinic_times) if num_runs > 1 else 0.0
    min_dinic_time = min(dinic_times)
    max_dinic_time = max(dinic_times)
    
    mean_dinic_memory = statistics.mean(dinic_memory)
    median_dinic_memory = statistics.median(dinic_memory)
    stdev_dinic_memory = statistics.stdev(dinic_memory) if num_runs > 1 else 0.0
    min_dinic_memory = min(dinic_memory)
    max_dinic_memory = max(dinic_memory)
    
    print(f"\nDinic's Results:")
    print(f"  Max Flow: {max_flow_dinic:,} passengers")
    print(f"  Mean Runtime: {mean_dinic_time:.4f}s")
    print(f"  Median Runtime: {median_dinic_time:.4f}s")
    print(f"  Std Dev: {stdev_dinic_time:.4f}s")
    print(f"  Min: {min_dinic_time:.4f}s | Max: {max_dinic_time:.4f}s")
    print(f"  Mean Memory: {mean_dinic_memory:.2f} MB")
    print(f"  Median Memory: {median_dinic_memory:.2f} MB")
    print(f"  Std Dev: {stdev_dinic_memory:.2f} MB")
    print(f"  Min: {min_dinic_memory:.2f} MB | Max: {max_dinic_memory:.2f} MB")

    # ========== COMPARISON ==========
    print("\n" + "="*80)
    print("COMPARISON")
    print("="*80)
    
    print(f"\nResults (averaged over {num_runs} runs):")
    print(f"\nTime Performance:")
    print(f"  Edmonds-Karp: {max_flow_ek:,} passengers in {mean_ek_time:.4f}s (±{stdev_ek_time:.4f}s)")
    print(f"  Dinic:        {max_flow_dinic:,} passengers in {mean_dinic_time:.4f}s (±{stdev_dinic_time:.4f}s)")
    
    print(f"\nSpace Performance:")
    print(f"  Edmonds-Karp: {mean_ek_memory:.2f} MB (±{stdev_ek_memory:.2f} MB)")
    print(f"  Dinic:        {mean_dinic_memory:.2f} MB (±{stdev_dinic_memory:.2f} MB)")
    
    if max_flow_ek == max_flow_dinic:
        print(f"\nBoth algorithms agree on max flow")
    else:
        print(f"\nWARNING: Algorithms disagree!")
    
    if mean_dinic_time < mean_ek_time:
        speedup = mean_ek_time / mean_dinic_time
        print(f"\n  → Dinic is {speedup:.2f}x FASTER than Edmonds-Karp")
    else:
        speedup = mean_dinic_time / mean_ek_time
        print(f"\n  → Edmonds-Karp is {speedup:.2f}x FASTER than Dinic")
    
    if mean_dinic_memory < mean_ek_memory:
        mem_ratio = mean_ek_memory / mean_dinic_memory
        print(f"  → Dinic uses {mem_ratio:.2f}x LESS memory than Edmonds-Karp")
    else:
        mem_ratio = mean_dinic_memory / mean_ek_memory
        print(f"  → Edmonds-Karp uses {mem_ratio:.2f}x LESS memory than Dinic")
    
    print(f"\nComplexity Analysis:")
    edges = sum(1 for i in range(graph.size) for j in range(graph.size) if graph.adj_matrix[i][j] > 0)
    nodes = graph.size
    print(f"Graph size: V={nodes:,}, E={edges:,}")
    print(f"\nTime Complexity:")
    print(f"  Edmonds-Karp: O(V·E²) ≈ {nodes * edges * edges:,} operations")
    print(f"  Dinic: O(V²·E) ≈ {nodes * nodes * edges:,} operations")
    print(f"\nSpace Complexity:")
    print(f"  Both algorithms use adjacency matrix: O(V²) = {nodes * nodes:,} entries")
    print(f"  Edmonds-Karp additional: O(V) for BFS queue and parent array")
    print(f"  Dinic additional: O(V + E) for adjacency list and level array")
    print(f"\nMeasured Memory:")
    print(f"  Edmonds-Karp: {mean_ek_memory:.2f} MB")
    print(f"  Dinic: {mean_dinic_memory:.2f} MB")
    
    print("="*80)


if __name__ == "__main__":
    max_flow = main()
