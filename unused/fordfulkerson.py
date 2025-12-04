import pandas as pd
import networkx as nx
from networkx.algorithms.flow import maximum_flow
import time

class TransjakartaMaxFlowAnalyzer:
    """
    Analyzes Transjakarta transit network using Maximum Flow algorithm
    to identify capacity bottlenecks and overdemand issues
    """
    
    def __init__(self, gtfs_path='gtfs/'):
        """Initialize analyzer with GTFS data"""
        print("="*80)
        print("LOADING GTFS DATA")
        print("="*80)
        self.gtfs_path = gtfs_path
        self.stops = pd.read_csv(f'{gtfs_path}stops.txt')
        self.stop_times = pd.read_csv(f'{gtfs_path}stop_times.txt')
        self.trips = pd.read_csv(f'{gtfs_path}trips.txt')
        self.routes = pd.read_csv(f'{gtfs_path}routes.txt')
        self.network = None
        
        print(f"✓ Loaded {len(self.stops)} stops")
        print(f"✓ Loaded {len(self.stop_times)} stop times")
        print(f"✓ Loaded {len(self.trips)} trips")
        print(f"✓ Loaded {len(self.routes)} routes")
        
        print(f"\nData Preview:")
        print(f"  Unique stops: {self.stops['stop_id'].nunique()}")
        print(f"  Unique trips: {self.stop_times['trip_id'].nunique()}")
        print(f"  Unique routes: {self.routes['route_id'].nunique()}")
        print(f"  Date range: {self.stop_times.shape[0]} scheduled stops")
    
    def build_network(self, default_capacity=100):
        """
        Build directed graph from GTFS data
        
        Args:
            default_capacity: Default bus capacity per route
        
        Returns:
            NetworkX DiGraph with stops as nodes and routes as edges
        """
        print("\nBuilding transit network graph...")
        self.network = nx.DiGraph()
        
        # Add all stops as nodes
        for _, stop in self.stops.iterrows():
            self.network.add_node(
                stop['stop_id'],
                stop_name=stop.get('stop_name', ''),
                lat=stop.get('stop_lat', 0),
                lon=stop.get('stop_lon', 0)
            )
        
        # Group stop_times by trip to get sequential connections
        trips_data = self.stop_times.groupby('trip_id')
        edge_count = 0
        
        for trip_id, trip_stops in trips_data:
            # Sort by stop_sequence to maintain order
            stops_list = trip_stops.sort_values('stop_sequence')
            
            # Connect consecutive stops in the trip
            for i in range(len(stops_list) - 1):
                source = stops_list.iloc[i]['stop_id']
                target = stops_list.iloc[i + 1]['stop_id']
                
                # Add or update edge with capacity
                if self.network.has_edge(source, target):
                    # Multiple trips use same edge -> increase capacity
                    self.network[source][target]['capacity'] += default_capacity
                    self.network[source][target]['trip_count'] += 1
                else:
                    self.network.add_edge(
                        source, 
                        target, 
                        capacity=default_capacity,
                        trip_count=1,
                        weight=1  # Can be updated with distance/time
                    )
                    edge_count += 1
        
        print(f"✓ Built network: {self.network.number_of_nodes()} nodes, {self.network.number_of_edges()} edges")
        
        # Calculate and display additional network info
        degrees = dict(self.network.degree())
        avg_degree = sum(degrees.values()) / len(degrees) if degrees else 0
        max_degree_node = max(degrees.items(), key=lambda x: x[1]) if degrees else (None, 0)
        
        print(f"\nNetwork Build Details:")
        print(f"  Average connections per stop: {avg_degree:.2f}")
        print(f"  Most connected stop: {max_degree_node[0]} ({max_degree_node[1]} connections)")
        print(f"  Total trips processed: {len(trips_data)}")
        
        # Show capacity distribution
        capacities = [d['capacity'] for u, v, d in self.network.edges(data=True)]
        if capacities:
            print(f"  Capacity range: {min(capacities)} - {max(capacities)} passengers")
            print(f"  Total network capacity: {sum(capacities):,} passengers")
        
        return self.network
    
    def analyze_max_flow(self, source_stop, sink_stop):
        """
        Run Ford-Fulkerson maximum flow algorithm
        
        Args:
            source_stop: Starting stop ID
            sink_stop: Ending stop ID
        
        Returns:
            tuple: (max_flow_value, flow_dict)
        """
        if self.network is None:
            raise ValueError("Network not built. Call build_network() first.")
        
        if source_stop not in self.network:
            raise ValueError(f"Source stop '{source_stop}' not found in network")
        if sink_stop not in self.network:
            raise ValueError(f"Sink stop '{sink_stop}' not found in network")
        
        print(f"\nAnalyzing max flow: {source_stop} → {sink_stop}")
        start_time = time.time()
        
        # Run maximum flow algorithm
        flow_value, flow_dict = maximum_flow(
            self.network, 
            source_stop, 
            sink_stop,
            capacity='capacity'
        )
        
        elapsed_time = time.time() - start_time
        
        print(f"✓ Maximum flow: {flow_value} passengers/hour")
        print(f"✓ Computation time: {elapsed_time:.4f} seconds")
        
        # Display flow path information
        active_edges = sum(1 for u in flow_dict for v, f in flow_dict[u].items() if f > 0)
        total_flow = sum(sum(flow_dict[u].values()) for u in flow_dict)
        
        print(f"\nFlow Details:")
        print(f"  Active edges: {active_edges}")
        print(f"  Source: {self.network.nodes[source_stop].get('stop_name', source_stop)}")
        print(f"  Sink: {self.network.nodes[sink_stop].get('stop_name', sink_stop)}")
        
        # Show top flow paths
        flows = [(u, v, f) for u in flow_dict for v, f in flow_dict[u].items() if f > 0]
        flows.sort(key=lambda x: x[2], reverse=True)
        
        if flows:
            print(f"\nTop 5 Flow Paths:")
            for i, (u, v, f) in enumerate(flows[:5], 1):
                u_name = self.network.nodes[u].get('stop_name', u)[:25]
                v_name = self.network.nodes[v].get('stop_name', v)[:25]
                cap = self.network[u][v]['capacity']
                print(f"  {i}. {u_name} → {v_name}: {f}/{cap} ({f/cap*100:.1f}%)")
        
        return flow_value, flow_dict
    
    def find_bottlenecks(self, source_stop, sink_stop, threshold=0.8):
        """
        Identify bottleneck edges where flow is close to capacity
        
        Args:
            source_stop: Starting stop ID
            sink_stop: Ending stop ID
            threshold: Consider bottleneck if flow/capacity >= threshold
        
        Returns:
            list of tuples: [(source, target, flow, capacity, utilization)]
        """
        flow_value, flow_dict = self.analyze_max_flow(source_stop, sink_stop)
        
        bottlenecks = []
        
        for source in flow_dict:
            for target, flow in flow_dict[source].items():
                if flow > 0:  # Only consider edges with flow
                    capacity = self.network[source][target]['capacity']
                    utilization = flow / capacity
                    
                    if utilization >= threshold:
                        bottlenecks.append((
                            source,
                            target,
                            flow,
                            capacity,
                            utilization
                        ))
        
        # Sort by utilization (highest first)
        bottlenecks.sort(key=lambda x: x[4], reverse=True)
        
        print(f"\n🔴 Found {len(bottlenecks)} bottlenecks (>={threshold*100}% capacity)")
        print("\nTop bottleneck edges:")
        print("-" * 80)
        for i, (src, tgt, flow, cap, util) in enumerate(bottlenecks[:10], 1):
            src_name = self.network.nodes[src].get('stop_name', src)[:30]
            tgt_name = self.network.nodes[tgt].get('stop_name', tgt)[:30]
            print(f"{i}. {src_name} → {tgt_name}")
            print(f"   Flow: {flow}/{cap} ({util*100:.1f}% utilization)")
        
        return bottlenecks
    
    def analyze_all_pairs_flow(self, top_stops=10):
        """
        Analyze flow between top stops (by degree)
        
        Args:
            top_stops: Number of top connected stops to analyze
        
        Returns:
            DataFrame with flow analysis results
        """
        if self.network is None:
            raise ValueError("Network not built. Call build_network() first.")
        
        # Find top stops by degree (most connected)
        degrees = dict(self.network.degree())
        top_stop_ids = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:top_stops]
        
        print(f"\nAnalyzing max flow between top {top_stops} connected stops...")
        
        results = []
        total_pairs = len(top_stop_ids) * (len(top_stop_ids) - 1)
        count = 0
        
        for i, (source, _) in enumerate(top_stop_ids):
            for j, (sink, _) in enumerate(top_stop_ids):
                if source != sink:
                    count += 1
                    try:
                        flow_val, _ = self.analyze_max_flow(source, sink)
                        results.append({
                            'source': source,
                            'sink': sink,
                            'max_flow': flow_val,
                            'source_name': self.network.nodes[source].get('stop_name', source),
                            'sink_name': self.network.nodes[sink].get('stop_name', sink)
                        })
                    except:
                        pass  # No path exists
        
        df = pd.DataFrame(results)
        return df.sort_values('max_flow', ascending=False)
    
    def add_passenger_demand(self, demand_data):
        """
        Add passenger demand data to network nodes
        
        Args:
            demand_data: DataFrame with columns ['stop_id', 'passenger_count']
        """
        print("\nAdding passenger demand data...")
        
        # Aggregate demand by stop
        demand_dict = demand_data.groupby('stop_id')['passenger_count'].sum().to_dict()
        
        # Add demand attribute to nodes
        for node in self.network.nodes():
            self.network.nodes[node]['demand'] = demand_dict.get(node, 0)
        
        total_demand = sum(demand_dict.values())
        print(f"✓ Added demand data: {total_demand:,} total passengers")
    
    def find_overdemand_stops(self):
        """
        Find stops where demand exceeds capacity
        
        Returns:
            list of tuples: [(stop_id, demand, capacity, deficit)]
        """
        overdemand_stops = []
        
        for node in self.network.nodes():
            demand = self.network.nodes[node].get('demand', 0)
            
            # Calculate total capacity (sum of incoming edges)
            incoming_capacity = sum(
                self.network[u][node]['capacity'] 
                for u in self.network.predecessors(node)
            )
            
            if demand > incoming_capacity:
                deficit = demand - incoming_capacity
                overdemand_stops.append((
                    node,
                    demand,
                    incoming_capacity,
                    deficit
                ))
        
        overdemand_stops.sort(key=lambda x: x[3], reverse=True)
        
        print(f"\n🔴 Found {len(overdemand_stops)} stops with overdemand")
        print("\nTop overdemand stops:")
        print("-" * 80)
        for i, (stop, demand, cap, deficit) in enumerate(overdemand_stops[:10], 1):
            stop_name = self.network.nodes[stop].get('stop_name', stop)
            print(f"{i}. {stop_name}")
            print(f"   Demand: {demand:,} | Capacity: {cap:,} | Deficit: {deficit:,}")
        
        return overdemand_stops
    
    def get_network_stats(self):
        """Print network statistics"""
        if self.network is None:
            print("Network not built yet.")
            return
        
        print("\n" + "="*80)
        print("NETWORK STATISTICS")
        print("="*80)
        print(f"Nodes (stops): {self.network.number_of_nodes()}")
        print(f"Edges (connections): {self.network.number_of_edges()}")
        print(f"Density: {nx.density(self.network):.4f}")
        print(f"Is strongly connected: {nx.is_strongly_connected(self.network)}")
        print(f"Number of strongly connected components: {nx.number_strongly_connected_components(self.network)}")
        
        # Capacity statistics
        capacities = [d['capacity'] for u, v, d in self.network.edges(data=True)]
        print(f"\nCapacity Statistics:")
        print(f"  Total capacity: {sum(capacities):,}")
        print(f"  Average capacity per edge: {sum(capacities)/len(capacities):.1f}")
        print(f"  Max capacity: {max(capacities):,}")
        print(f"  Min capacity: {min(capacities):,}")
        
        # Node degree analysis
        in_degrees = dict(self.network.in_degree())
        out_degrees = dict(self.network.out_degree())
        
        print(f"\nNode Degree Statistics:")
        print(f"  Average in-degree: {sum(in_degrees.values())/len(in_degrees):.2f}")
        print(f"  Average out-degree: {sum(out_degrees.values())/len(out_degrees):.2f}")
        
        # Find hubs (high degree nodes)
        total_degrees = {k: in_degrees[k] + out_degrees[k] for k in in_degrees}
        top_hubs = sorted(total_degrees.items(), key=lambda x: x[1], reverse=True)[:5]
        
        print(f"\nTop 5 Hub Stops (Most Connected):")
        for i, (stop, degree) in enumerate(top_hubs, 1):
            stop_name = self.network.nodes[stop].get('stop_name', stop)
            print(f"  {i}. {stop_name}: {degree} connections ({in_degrees[stop]} in, {out_degrees[stop]} out)")
        
        # Route analysis
        print(f"\nRoute Information:")
        for i, (idx, route) in enumerate(self.routes.head(10).iterrows(), 1):
            route_name = route.get('route_short_name', route.get('route_long_name', 'N/A'))
            print(f"  {i}. Route {route['route_id']}: {route_name}")


# =============================================================================
# EXAMPLE USAGE
# =============================================================================

if __name__ == "__main__":
    # Initialize analyzer
    analyzer = TransjakartaMaxFlowAnalyzer(gtfs_path='gtfs/')
    
    # Build the network
    analyzer.build_network(default_capacity=50)  # 50 passengers per bus
    
    # Get network statistics
    analyzer.get_network_stats()
    
    # Get list of stops to choose from
    print("\n" + "="*80)
    print("AVAILABLE STOPS (showing top 20 by connections):")
    print("="*80)
    
    # Get stops sorted by degree (most connected)
    degrees = dict(analyzer.network.degree())
    top_stops = sorted(degrees.items(), key=lambda x: x[1], reverse=True)[:20]
    
    for i, (stop_id, degree) in enumerate(top_stops, 1):
        stop_info = analyzer.stops[analyzer.stops['stop_id'] == stop_id]
        if not stop_info.empty:
            stop_name = stop_info.iloc[0].get('stop_name', 'N/A')
            lat = stop_info.iloc[0].get('stop_lat', 'N/A')
            lon = stop_info.iloc[0].get('stop_lon', 'N/A')
            print(f"{i:2d}. ID: {stop_id:20s} | {stop_name:40s} | Connections: {degree:3d} | Coords: ({lat}, {lon})")
    
    # Example: Analyze max flow between most connected stops
    if len(top_stops) >= 2:
        source = top_stops[0][0]  # Most connected stop
        sink = top_stops[1][0]    # Second most connected stop
        
        print("\n" + "="*80)
        print("MAXIMUM FLOW ANALYSIS EXAMPLE")
        print("="*80)
        print(f"Analyzing flow between two highly connected stops:")
        
        try:
            flow_val, flow_dict = analyzer.analyze_max_flow(source, sink)
            
            # Find bottlenecks
            print("\n" + "="*80)
            print("BOTTLENECK ANALYSIS")
            print("="*80)
            bottlenecks = analyzer.find_bottlenecks(source, sink, threshold=0.7)
            
            # Show sample flow data structure
            print("\n" + "="*80)
            print("SAMPLE FLOW DATA STRUCTURE")
            print("="*80)
            print(f"Flow dictionary contains {len(flow_dict)} source nodes")
            sample_count = 0
            for u in flow_dict:
                if sample_count >= 3:
                    break
                for v, f in flow_dict[u].items():
                    if f > 0 and sample_count < 3:
                        u_name = analyzer.network.nodes[u].get('stop_name', u)
                        v_name = analyzer.network.nodes[v].get('stop_name', v)
                        print(f"  {u_name} → {v_name}: flow={f}")
                        sample_count += 1
            
        except Exception as e:
            print(f"Error: {e}")
            print("Tip: Choose stops that are connected in the network")
    
    print("\n" + "="*80)
    print("Analysis complete! Modify the source and sink stops to test different routes.")
    print("="*80)


