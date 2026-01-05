from StopNetwork import StopNetwork
import time

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
    
    # Time window for analysis
    time_start = '06:00:00'  # Morning peak start
    time_end = '07:00:00'    # Morning peak end
    
    # Bus capacity
    bus_capacity = 85  # passengers per bus
    
    # Transfer constraints
    min_transfer_time = 120   # Minimum 2 minutes to transfer (120 seconds)
    max_transfer_time = 1800  # Maximum 30 minutes wait (1800 seconds)
    
    # Source and destination stops
    source_stop_id = 'G00106' # Pasar Senen (example transit route)
    dest_stop_id = 'P00017'  # Blok M

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
    
    
    # ========== RUN EDMONDS-KARP ==========
    print("\n" + "="*80)
    print("1. EDMONDS-KARP ALGORITHM")
    print("="*80)
    print(f"From: {source_name} ({source_stop_id})")
    print(f"To: {dest_name} ({dest_stop_id})")
    print(f"Time window: {time_start} - {time_end}")
    print("="*80 + "\n")
    
    # Deep copy the graph for Edmonds-Karp
    from edmondskarp import Graph as GraphEK
    graph_ek_copy = GraphEK(graph.size)
    for i in range(graph.size):
        for j in range(graph.size):
            graph_ek_copy.adj_matrix[i][j] = graph.adj_matrix[i][j]
        graph_ek_copy.vertex_data[i] = graph.vertex_data[i]
    
    start_time = time.time()
    max_flow_ek = graph_ek_copy.edmonds_karp(source_idx, sink_idx)
    ek_time = time.time() - start_time
    
    print(f"\nEdmonds-Karp Result:")
    print(f"  Max Flow: {max_flow_ek:,} passengers")
    print(f"  Runtime: {ek_time:.4f} seconds")
    
    # ========== RUN DINIC ==========
    print("\n" + "="*80)
    print("2. DINIC'S ALGORITHM")
    print("="*80)
    print(f"From: {source_name} ({source_stop_id})")
    print(f"To: {dest_name} ({dest_stop_id})")
    print(f"Time window: {time_start} - {time_end}")
    print("="*80 + "\n")
    
    # Build Dinic graph from adjacency matrix
    from dinic import Graph as GraphDinic
    graph_dinic = GraphDinic(graph.size)
    
    # Copy edges from original graph
    for i in range(graph.size):
        for j in range(graph.size):
            if graph.adj_matrix[i][j] > 0:
                graph_dinic.addEdge(i, j, graph.adj_matrix[i][j])
        graph_dinic.vertex_data[i] = graph.vertex_data[i]
    
    start_time = time.time()
    max_flow_dinic = graph_dinic.DinicMaxflow(source_idx, sink_idx)
    dinic_time = time.time() - start_time
    
    print(f"\nDinic's Result:")
    print(f"  Max Flow: {max_flow_dinic:,} passengers")
    print(f"  Runtime: {dinic_time:.4f} seconds")
    
    # ========== COMPARISON ==========
    print("\n" + "="*80)
    print("COMPARISON")
    print("="*80)
    
    print(f"\nResults:")
    print(f"  Edmonds-Karp: {max_flow_ek:,} passengers in {ek_time:.4f}s")
    print(f"  Dinic:        {max_flow_dinic:,} passengers in {dinic_time:.4f}s")
    
    if max_flow_ek == max_flow_dinic:
        print(f"  ✓ Both algorithms agree on max flow")
    else:
        print(f"  ⚠️ WARNING: Algorithms disagree!")
    
    if dinic_time < ek_time:
        speedup = ek_time / dinic_time
        print(f"\n  Dinic is {speedup:.2f}x FASTER than Edmonds-Karp")
    else:
        speedup = dinic_time / ek_time
        print(f"\n  Edmonds-Karp is {speedup:.2f}x FASTER than Dinic")
    
    print(f"\nComplexity:")
    edges = sum(1 for i in range(graph.size) for j in range(graph.size) if graph.adj_matrix[i][j] > 0)
    nodes = graph.size
    print(f"  Graph size: V={nodes:,}, E={edges:,}")
    print(f"  Edmonds-Karp: O(V·E²) ≈ {nodes * edges * edges:,} operations")
    print(f"  Dinic:        O(V²·E) ≈ {nodes * nodes * edges:,} operations")
    
    print("="*80)
    
    return max_flow_ek


if __name__ == "__main__":
    max_flow = main()
