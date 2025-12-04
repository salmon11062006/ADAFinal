import pandas as pd
import calendar
from edmondskarp import Graph
from collections import defaultdict

print("="*60)
print("TRANSJAKARTA NETWORK MAX FLOW ANALYSIS")
print("="*60)

# Load passenger data
print("\n1. Loading passenger data...")
df1 = pd.read_excel('data-penumpang-bus-transjakarta-tahun-2021-(1764692849646).xlsx')
df1_BRT = df1[df1['jenis'] == 'BRT'].copy()

# Calculate daily passengers
df1_BRT['year'] = df1_BRT['periode_data'] // 100
df1_BRT['month'] = df1_BRT['periode_data'] % 100
df1_BRT['days_in_month'] = df1_BRT.apply(
    lambda row: calendar.monthrange(row['year'], row['month'])[1], 
    axis=1
)
df1_BRT['jumlah_penumpang_per_day'] = df1_BRT['jumlah_penumpang'] / df1_BRT['days_in_month']

# Get average daily passengers per route
route_capacity = df1_BRT.groupby('kode_trayek')['jumlah_penumpang_per_day'].mean().to_dict()
# Ensure all keys are strings
route_capacity = {str(k): v for k, v in route_capacity.items()}
print(f"    Loaded {len(df1_BRT)} BRT records")
print(f"    Found {len(route_capacity)} unique routes")
print(f"    Capacities: {dict(list(route_capacity.items()))}")

# Load GTFS data
print("\n2. Loading GTFS data...")
stops = pd.read_csv('gtfs/stops.txt')
stop_times = pd.read_csv('gtfs/stop_times.txt')
trips = pd.read_csv('gtfs/trips.txt')
routes = pd.read_csv('gtfs/routes.txt')

print(f"    Loaded {len(stops)} stops")
print(f"    Loaded {len(trips)} trips")
print(f"    Loaded {len(routes)} routes")

# Build stop index mapping
print("\n3. Building network graph...")
unique_stops = stops['stop_id'].unique()
stop_to_idx = {stop_id: idx for idx, stop_id in enumerate(unique_stops)}
idx_to_stop = {idx: stop_id for stop_id, idx in stop_to_idx.items()}

# Create graph
n = len(unique_stops)
g = Graph(n)

# Add vertex names
for idx, stop_id in idx_to_stop.items():
    stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
    g.add_vertex_data(idx, f"{stop_name}")

# Build edges from trips with capacity from passenger data
# FILTER: Only include main corridors (routes 1-13)
import re
main_corridors = [str(i) for i in range(1, 14)]  # '1' to '13'

edge_count = 0
capacity_matrix = defaultdict(lambda: defaultdict(int))
routes_found = set()
routes_not_found = set()

for trip_id, trip_stops in stop_times.groupby('trip_id'):
    # Get route for this trip
    trip_row = trips[trips['trip_id'] == trip_id]
    if trip_row.empty:
        continue
    
    route_id = str(trip_row.iloc[0]['route_id'])  # Ensure string
    
    # Extract base route number (e.g., '10A' -> '10', '11B' -> '11', '1' -> '1')
    base_route = re.sub(r'[A-Z]+$', '', route_id)
    
    # Skip if not in main corridors (1-13)
    if base_route not in main_corridors:
        continue
    
    # Try to match route_id, or use base route for variants
    capacity = route_capacity.get(route_id)
    
    if capacity is None:
        capacity = route_capacity.get(base_route, 100)
        if base_route in route_capacity:
            routes_found.add(f"{route_id} (matched as {base_route})")
        else:
            routes_not_found.add(route_id)
    else:
        routes_found.add(route_id)
    
    # Sort stops by sequence
    stops_list = trip_stops.sort_values('stop_sequence')
    
    # Connect consecutive stops
    for i in range(len(stops_list) - 1):
        source = stops_list.iloc[i]['stop_id']
        target = stops_list.iloc[i + 1]['stop_id']
        
        if source in stop_to_idx and target in stop_to_idx:
            u = stop_to_idx[source]
            v = stop_to_idx[target]
            
            # Accumulate capacity for edges used by multiple routes
            capacity_matrix[u][v] += capacity
            edge_count += 1

# Add edges to graph
for u in capacity_matrix:
    for v in capacity_matrix[u]:
        g.add_edge(u, v, capacity_matrix[u][v])

print(f"   Built graph: {n} nodes, {edge_count} edges")
print(f"   FILTERED: Main corridors only (routes 1-13)")
print(f"   Routes matched with passenger data: {len(routes_found)}")
print(f"   Routes using default capacity: {len(routes_not_found)}")
if routes_not_found:
    print(f"   Unmatched routes (sample): {list(routes_not_found)[:5]}")

# Find most connected stops (hubs)
print("\n4. Finding major transit hubs...")
out_degree = defaultdict(int)
in_degree = defaultdict(int)

for u in capacity_matrix:
    for v in capacity_matrix[u]:
        out_degree[u] += 1
        in_degree[v] += 1

# Total degree (connections)
total_degree = {}
for idx in range(n):
    total_degree[idx] = out_degree[idx] + in_degree[idx]

top_hubs = sorted(total_degree.items(), key=lambda x: x[1], reverse=True)[:10]

print("\n   Top 10 Transit Hubs (Main Corridors 1-13):")
for rank, (stop_idx, degree) in enumerate(top_hubs, 1):
    stop_id = idx_to_stop[stop_idx]
    stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
    print(f"   {rank:2d}. {stop_name}: {degree} connections")

'''
# Run max flow analysis between major hubs
print("\n" + "="*60)
print("MAX FLOW ANALYSIS - MAIN CORRIDORS (1-13)")
print("="*60)

# Example 1: Between top two hubs
if len(top_hubs) >= 2:
    source_idx = top_hubs[0][0]
    sink_idx = top_hubs[1][0]
    
    source_name = stops[stops['stop_id'] == idx_to_stop[source_idx]].iloc[0]['stop_name']
    sink_name = stops[stops['stop_id'] == idx_to_stop[sink_idx]].iloc[0]['stop_name']
    
    print(f"\nAnalysis 1: {source_name} → {sink_name}")
    print("-" * 60)
    
    # Create a copy of the graph for this analysis
    g1 = Graph(n)
    for idx, stop_id in idx_to_stop.items():
        stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
        g1.add_vertex_data(idx, stop_name)
    
    for u in capacity_matrix:
        for v in capacity_matrix[u]:
            g1.add_edge(u, v, capacity_matrix[u][v])
    
    max_flow_1 = g1.edmonds_karp(source_idx, sink_idx)
    print(f"\nMaximum Flow: {max_flow_1:,.0f} passengers/day")

# Example 2: Between 3rd and 4th hubs
if len(top_hubs) >= 4:
    source_idx = top_hubs[2][0]
    sink_idx = top_hubs[3][0]
    
    source_name = stops[stops['stop_id'] == idx_to_stop[source_idx]].iloc[0]['stop_name']
    sink_name = stops[stops['stop_id'] == idx_to_stop[sink_idx]].iloc[0]['stop_name']
    
    print(f"\n\nAnalysis 2: {source_name} → {sink_name}")
    print("-" * 60)
    
    # Create a copy of the graph for this analysis
    g2 = Graph(n)
    for idx, stop_id in idx_to_stop.items():
        stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
        g2.add_vertex_data(idx, stop_name)
    
    for u in capacity_matrix:
        for v in capacity_matrix[u]:
            g2.add_edge(u, v, capacity_matrix[u][v])
    
    max_flow_2 = g2.edmonds_karp(source_idx, sink_idx)
    print(f"\nMaximum Flow: {max_flow_2:,.0f} passengers/day")

print("\n" + "="*60)
print("ANALYSIS COMPLETE")
print("="*60)
'''