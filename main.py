import pandas as pd
import calendar
from edmondskarp import Graph
from collections import defaultdict

print("="*60)
print("TRANSJAKARTA NETWORK MAX FLOW ANALYSIS")
print("="*60)

# ============================================================================
# SECTION 1: Load real passenger data (for comparison later)
# ============================================================================
print("\n1. Loading passenger data (for comparison)...")
df1 = pd.read_excel('data-penumpang-bus-transjakarta-tahun-2021-(1764692849646).xlsx')
df1_BRT = df1[df1['jenis'] == 'BRT'].copy()

# Calculate daily passengers from monthly data
df1_BRT['year'] = df1_BRT['periode_data'] // 100
df1_BRT['month'] = df1_BRT['periode_data'] % 100
df1_BRT['days_in_month'] = df1_BRT.apply(
    lambda row: calendar.monthrange(row['year'], row['month'])[1], 
    axis=1
)
df1_BRT['jumlah_penumpang_per_day'] = df1_BRT['jumlah_penumpang'] / df1_BRT['days_in_month']

# Get average daily passengers per route (for later comparison)
actual_passengers = df1_BRT.groupby('kode_trayek')['jumlah_penumpang_per_day'].mean().to_dict()
actual_passengers = {str(k): v for k, v in actual_passengers.items()}
print(f"    Loaded {len(df1_BRT)} BRT records")
print(f"    Found {len(actual_passengers)} unique routes")
print(f"    (This data will be used for comparison later)")

# ============================================================================
# SECTION 2: Load GTFS data and build network graph
# ============================================================================
print("\n2. Loading GTFS data...")
stops = pd.read_csv('gtfs/stops.txt')
stop_times = pd.read_csv('gtfs/stop_times.txt')
trips = pd.read_csv('gtfs/trips.txt')
routes = pd.read_csv('gtfs/routes.txt')
frequencies = pd.read_csv('gtfs/frequencies.txt')

print(f"    Loaded {len(stops)} stops")
print(f"    Loaded {len(trips)} trips")
print(f"    Loaded {len(routes)} routes")

# ============================================================================
# SECTION 3: Calculate edge capacities based on bus frequency
# Standard bus capacity: 85 passengers
# Edge capacity = (buses per hour) × 85 passengers
# ============================================================================
print("\n3. Calculating edge capacities from GTFS data...")

# CONSTANTS (IMPORTANT!)
BUS_CAPACITY = 85  # Standard TransJakarta bus capacity, this is an assumption
OPERATING_HOURS = 17  # Typical operating hours: 5:00 - 22:00 (17 hours)
PEAK_PERIODS = [('06:00:00', '09:00:00'), ('16:00:00', '20:00:00')]  # For capacity calculation

# Build stop index mapping
unique_stops = stops['stop_id'].unique()
stop_to_idx = {stop_id: idx for idx, stop_id in enumerate(unique_stops)}
idx_to_stop = {idx: stop_id for stop_id, idx in stop_to_idx.items()}

# Create graph
n = len(unique_stops)
print(len(unique_stops), "unique stops found.")
g = Graph(n)

# Add vertex names
for idx, stop_id in idx_to_stop.items():
    stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
    g.add_vertex_data(idx, f"{stop_name}")

# Calculate edge capacity for each route based on bus frequency
# Capacity per edge = (3600 / headway_seconds) × 85 passengers
import re
main_corridors = [str(i) for i in range(1, 14)]  # Filter: Only corridors 1-13

# Step 1: Calculate buses per hour for each route during peak periods
route_edge_capacity = {}

for _, route in routes.iterrows():
    route_id = str(route['route_id'])
    route_short = str(route['route_short_name'])
    
    # Extract base route number (e.g., '10A' -> '10', '1' -> '1')
    base_route = re.sub(r'[A-Z]+$', '', route_short)
    
    # Skip if not in main corridors (1-13)
    if base_route not in main_corridors:
        continue
    
    # Get all trips for this route
    route_trips = trips[trips['route_id'] == route_id]
    
    # Get frequencies for these trips during peak hours
    route_freqs = frequencies[frequencies['trip_id'].isin(route_trips['trip_id'])]
    
    headways = []
    for start, end in PEAK_PERIODS:
        period_data = route_freqs[
            (route_freqs['start_time'] <= end) & 
            (route_freqs['end_time'] >= start)
        ]
        if not period_data.empty:
            headways.extend(period_data['headway_secs'].tolist())
    
    # Calculate edge capacity for this route
    if headways:
        avg_headway = sum(headways) / len(headways)
        buses_per_hour = 3600 / avg_headway
        edge_capacity = int(buses_per_hour * BUS_CAPACITY)
    else:
        # Default: 6 buses/hour (10-minute headway) × 85 passengers
        edge_capacity = 6 * BUS_CAPACITY
    
    route_edge_capacity[route_id] = edge_capacity

# Step 2: Build edges between consecutive stops with calculated capacities
edge_count = 0
capacity_matrix = defaultdict(lambda: defaultdict(int))

for trip_id, trip_stops in stop_times.groupby('trip_id'):
    # Get route for this trip
    trip_row = trips[trips['trip_id'] == trip_id]
    if trip_row.empty:
        continue
    
    route_id = str(trip_row.iloc[0]['route_id'])
    
    # Skip if route not in our calculated capacities (not in corridors 1-13)
    if route_id not in route_edge_capacity:
        continue
    
    # Get edge capacity for this route
    edge_capacity = route_edge_capacity[route_id]
    
    # Sort stops by sequence
    stops_list = trip_stops.sort_values('stop_sequence')
    
    # Connect consecutive stops
    for i in range(len(stops_list) - 1):
        source = stops_list.iloc[i]['stop_id']
        target = stops_list.iloc[i + 1]['stop_id']
        
        if source in stop_to_idx and target in stop_to_idx:
            u = stop_to_idx[source]
            v = stop_to_idx[target]
            
            # Add capacity to edge (multiple routes may use same edge)
            # Edge capacity accumulates when multiple routes share the same segment
            capacity_matrix[u][v] += edge_capacity
            edge_count += 1

# Step 3: Add all calculated edges to the graph
for u in capacity_matrix:
    for v in capacity_matrix[u]:
        g.add_edge(u, v, capacity_matrix[u][v])

print(f"   Built graph: {n} nodes, {edge_count} edges")
print(f"   FILTERED: Main corridors only (routes 1-13)")
print(f"   Edge capacities based on: {BUS_CAPACITY} passengers/bus × frequency")
print(f"   Routes processed: {len(route_edge_capacity)}")

# ============================================================================
# SECTION 4: Identify major transit hubs
# Hubs are stops with the most connections (high degree)
# ============================================================================
print("\n4. Finding major transit hubs...")
out_degree = defaultdict(int)
in_degree = defaultdict(int)

# Count incoming and outgoing connections for each stop
for u in capacity_matrix:
    for v in capacity_matrix[u]:
        out_degree[u] += 1
        in_degree[v] += 1

# Calculate total degree (total connections) for each stop
total_degree = {}
for idx in range(n):
    total_degree[idx] = out_degree[idx] + in_degree[idx]

# Find top 10 most connected stops
top_hubs = sorted(total_degree.items(), key=lambda x: x[1], reverse=True)[:10]

print("\n   Top 10 Transit Hubs (Main Corridors 1-13):")
for rank, (stop_idx, degree) in enumerate(top_hubs, 1):
    stop_id = idx_to_stop[stop_idx]
    stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
    print(f"   {rank:2d}. {stop_name}: {degree} connections")


# ============================================================================
# SECTION 5: Run max flow analysis between major hubs
# Uses Edmonds-Karp algorithm to find maximum passenger throughput
# This reveals network bottlenecks and connectivity between major stops
# ============================================================================
print("\n" + "="*60)
print("MAX FLOW ANALYSIS - NETWORK BOTTLENECKS")
print("="*60)

max_flow_results = []  # Store results for later comparison

# Example 1: Between top two hubs
if len(top_hubs) >= 2:
    source_idx = top_hubs[0][0]
    sink_idx = top_hubs[1][0]
    
    source_name = stops[stops['stop_id'] == idx_to_stop[source_idx]].iloc[0]['stop_name']
    sink_name = stops[stops['stop_id'] == idx_to_stop[sink_idx]].iloc[0]['stop_name']
    
    print(f"\nAnalysis 1: {source_name} → {sink_name}")
    print("-" * 60)
    
    # Create a copy of the graph for this analysis
    # (Edmonds-Karp modifies the graph, so we need a fresh copy)
    g1 = Graph(n)
    for idx, stop_id in idx_to_stop.items():
        stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
        g1.add_vertex_data(idx, stop_name)
    
    for u in capacity_matrix:
        for v in capacity_matrix[u]:
            g1.add_edge(u, v, capacity_matrix[u][v])
    
    # Run Edmonds-Karp max flow algorithm
    max_flow_1 = g1.edmonds_karp(source_idx, sink_idx)
    print(f"\nMaximum Flow Capacity: {max_flow_1:,.0f} passengers/hour (peak)")
    print(f"   (Based on {BUS_CAPACITY} passengers/bus × peak frequency)")
    
    # Convert to daily capacity (operating hours: 5:00 - 22:00 = 17 hours)
    max_flow_daily_1 = max_flow_1 * OPERATING_HOURS
    print(f"\nEstimated Daily Capacity: {max_flow_daily_1:,.0f} passengers/day")
    print(f"   (Assuming {OPERATING_HOURS} operating hours: 5:00 - 22:00)")
    
    max_flow_results.append({
        'source': source_name,
        'sink': sink_name,
        'hourly': max_flow_1,
        'daily': max_flow_daily_1
    })

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
    
    # Run Edmonds-Karp max flow algorithm
    max_flow_2 = g2.edmonds_karp(source_idx, sink_idx)
    print(f"\nMaximum Flow Capacity: {max_flow_2:,.0f} passengers/hour (peak)")
    print(f"   (Based on {BUS_CAPACITY} passengers/bus × peak frequency)")
    
    # Convert to daily capacity
    max_flow_daily_2 = max_flow_2 * OPERATING_HOURS
    print(f"\nEstimated Daily Capacity: {max_flow_daily_2:,.0f} passengers/day")
    print(f"   (Assuming {OPERATING_HOURS} operating hours: 5:00 - 22:00)")
    
    max_flow_results.append({
        'source': source_name,
        'sink': sink_name,
        'hourly': max_flow_2,
        'daily': max_flow_daily_2
    })

# ============================================================================
# SECTION 6: Collect actual passenger data for comparison
# ============================================================================
# Calculate total actual passengers from real data (for network stress analysis)
total_actual_passengers = sum(actual_passengers.values())
total_route_capacity = sum(route_edge_capacity[r] for r in route_edge_capacity) * OPERATING_HOURS

# ============================================================================
# SECTION 7: Max Flow Insights - Network Bottleneck Analysis
# ============================================================================
print("\n" + "="*60)
print("MAX FLOW INSIGHTS - WHAT THE ALGORITHM REVEALS")
print("="*60)

if max_flow_results:
    print("\n1. NETWORK CONNECTIVITY ANALYSIS:")
    print("-" * 60)
    print("Max flow shows the ACTUAL throughput between major hubs,")
    print("accounting for network constraints and bottlenecks.")
    print()
    
    for i, result in enumerate(max_flow_results, 1):
        print(f"Connection {i}: {result['source']} → {result['sink']}")
        print(f"   Max throughput: {result['daily']:,.0f} passengers/day")
        
        # Compare with total system capacity
        network_efficiency = (result['daily'] / total_route_capacity) * 100 if total_route_capacity > 0 else 0
        print(f"   Network efficiency: {network_efficiency:.1f}% of total system capacity")
        
        # Check if this connection is a bottleneck
        if result['daily'] < total_route_capacity * 0.3:
            print(f"   ⚠️  POTENTIAL BOTTLENECK - Low throughput relative to system capacity")
        print()
    
    print("\n2. KEY INSIGHTS FROM MAX FLOW ALGORITHM:")
    print("-" * 60)
    print("✓ Max Flow reveals BOTTLENECKS:")
    print("  - If max flow << sum of route capacities → bottleneck exists")
    print("  - Shows realistic passenger movement between major hubs")
    print("  - Identifies weakest links in the network")
    print()
    print("✓ Network Analysis:")
    print("  - High max flow = Good connectivity between hubs")
    print("  - Low max flow = Network constraints limiting passenger movement")
    print("  - Multiple paths increase resilience and capacity")
    print()
    
    print("\n3. NETWORK CAPACITY vs ACTUAL USAGE:")
    print("-" * 60)
    print(f"Total actual daily passengers:   {total_actual_passengers:,.0f}")
    print(f"Total system capacity (daily):   {total_route_capacity:,.0f}")
    
    if max_flow_results:
        avg_max_flow = sum(r['daily'] for r in max_flow_results) / len(max_flow_results)
        print(f"Average max flow (hub-to-hub):   {avg_max_flow:,.0f}")
        print()
        
        # Calculate network stress
        if avg_max_flow > 0:
            network_stress = (total_actual_passengers / total_route_capacity) * 100
            print(f"Overall system utilization:      {network_stress:.1f}%")
            
            # Network efficiency: How well does max flow compare to theoretical capacity?
            max_flow_efficiency = (avg_max_flow / total_route_capacity) * 100
            print(f"Network efficiency (max flow):   {max_flow_efficiency:.1f}%")
            print()
            
            if network_stress > 70:
                print("⚠️  HIGH SYSTEM UTILIZATION - Recommendations:")
                print("   - System approaching capacity limits")
                print("   - Consider adding routes or increasing frequency")
                print("   - Bottlenecks may be limiting passenger flow")
            elif network_stress < 30:
                print("✓ LOW SYSTEM UTILIZATION - System has spare capacity")
                print("   - Could handle significant demand increases")
                print("   - May indicate inefficient route planning")
            else:
                print("✓ MODERATE UTILIZATION - System operating within range")
            print()
            
            # Bottleneck detection
            if max_flow_efficiency < 50:
                print("⚠️  NETWORK BOTTLENECK DETECTED:")
                print("   - Max flow significantly lower than theoretical capacity")
                print("   - Network structure limiting passenger throughput")
                print("   - Consider adding direct connections between major hubs")
            else:
                print("✓ GOOD NETWORK CONNECTIVITY:")
                print("   - Max flow indicates efficient network structure")
                print("   - Multiple paths available for passenger flow")


