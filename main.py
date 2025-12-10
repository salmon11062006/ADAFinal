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

# Analyze connectivity between ALL top hubs (comprehensive system analysis)
print("\nAnalyzing connectivity between top transit hubs...")
print("This reveals the overall capacity of the corridor network (1-13)")
print()

# Run max flow for all pairs of top hubs to get comprehensive network view
num_hubs_to_analyze = min(5, len(top_hubs))  # Analyze top 5 hubs

for i in range(num_hubs_to_analyze):
    for j in range(i + 1, num_hubs_to_analyze):
        source_idx = top_hubs[i][0]
        sink_idx = top_hubs[j][0]
        
        source_name = stops[stops['stop_id'] == idx_to_stop[source_idx]].iloc[0]['stop_name']
        sink_name = stops[stops['stop_id'] == idx_to_stop[sink_idx]].iloc[0]['stop_name']
        
        # Create a copy of the graph for this analysis
        g_temp = Graph(n)
        for idx, stop_id in idx_to_stop.items():
            stop_name = stops[stops['stop_id'] == stop_id].iloc[0]['stop_name']
            g_temp.add_vertex_data(idx, stop_name)
        
        for u in capacity_matrix:
            for v in capacity_matrix[u]:
                g_temp.add_edge(u, v, capacity_matrix[u][v])
        
        # Run Edmonds-Karp max flow algorithm
        max_flow_hourly = g_temp.edmonds_karp(source_idx, sink_idx)
        max_flow_daily = max_flow_hourly * OPERATING_HOURS
        
        max_flow_results.append({
            'source': source_name,
            'sink': sink_name,
            'hourly': max_flow_hourly,
            'daily': max_flow_daily
        })

# Display summary of key connections
print(f"Analyzed {len(max_flow_results)} hub-to-hub connections")
print("\nTop 5 Highest Capacity Connections:")
print("-" * 80)
print(f"{'From':<25} {'To':<25} {'Daily Capacity':<20}")
print("-" * 80)

sorted_results = sorted(max_flow_results, key=lambda x: x['daily'], reverse=True)[:5]
for result in sorted_results:
    print(f"{result['source']:<25} {result['sink']:<25} {result['daily']:>15,.0f}")

print("\nBottom 5 Lowest Capacity Connections (Potential Bottlenecks):")
print("-" * 80)
print(f"{'From':<25} {'To':<25} {'Daily Capacity':<20}")
print("-" * 80)

bottleneck_results = sorted(max_flow_results, key=lambda x: x['daily'])[:5]
for result in bottleneck_results:
    print(f"{result['source']:<25} {result['sink']:<25} {result['daily']:>15,.0f}")

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
    print("\n1. OVERALL SYSTEM CAPACITY (Corridors 1-13):")
    print("-" * 60)
    print("Max flow analysis across ALL major hub connections")
    print("Shows the comprehensive capacity of the corridor network")
    print()
    
    # Calculate system-wide statistics
    avg_max_flow_daily = sum(r['daily'] for r in max_flow_results) / len(max_flow_results)
    min_max_flow_daily = min(r['daily'] for r in max_flow_results)
    max_max_flow_daily = max(r['daily'] for r in max_flow_results)
    
    print(f"Total hub connections analyzed: {len(max_flow_results)}")
    print(f"Average connection capacity:    {avg_max_flow_daily:,.0f} passengers/day")
    print(f"Minimum connection capacity:    {min_max_flow_daily:,.0f} passengers/day")
    print(f"Maximum connection capacity:    {max_max_flow_daily:,.0f} passengers/day")
    print()
    
    # System capacity variance indicates network balance
    variance = sum((r['daily'] - avg_max_flow_daily)**2 for r in max_flow_results) / len(max_flow_results)
    std_dev = variance ** 0.5
    coefficient_variation = (std_dev / avg_max_flow_daily) * 100 if avg_max_flow_daily > 0 else 0
    
    print(f"Capacity variation (CV):        {coefficient_variation:.1f}%")
    if coefficient_variation > 50:
        print("   ⚠️  HIGH VARIATION - Network has significant imbalances")
    elif coefficient_variation < 25:
        print("   ✓ LOW VARIATION - Network is well-balanced")
    else:
        print("   ✓ MODERATE VARIATION - Acceptable network balance")
    
    print("\n2. NETWORK CONNECTIVITY QUALITY:")
    print("-" * 60)
    
    # Compare with total system capacity
    network_efficiency = (avg_max_flow_daily / total_route_capacity) * 100 if total_route_capacity > 0 else 0
    print(f"Network efficiency:             {network_efficiency:.1f}%")
    print(f"   (Average max flow vs total theoretical capacity)")
    print()
    
    if network_efficiency < 30:
        print("⚠️  LOW NETWORK EFFICIENCY - Significant bottlenecks exist")
        print("   - Network structure severely limits passenger flow")
        print("   - Many hubs poorly connected despite high individual route capacity")
        print("   - Recommend: Add express routes, improve connectivity")
    elif network_efficiency < 60:
        print("⚠️  MODERATE NETWORK EFFICIENCY - Some bottlenecks present")
        print("   - Network could be better connected")
        print("   - Recommend: Identify and strengthen weak connections")
    else:
        print("✓ HIGH NETWORK EFFICIENCY - Good connectivity")
        print("   - Network structure supports good passenger flow")
        print("   - Multiple alternative paths available")
    
    print("\n3. SYSTEM UTILIZATION vs CAPACITY:")
    print("-" * 60)
    print(f"Total actual daily passengers:  {total_actual_passengers:,.0f}")
    print(f"Total system capacity (routes): {total_route_capacity:,.0f}")
    print(f"Average hub-hub max flow:       {avg_max_flow_daily:,.0f}")
    print()
    
    # System utilization
    system_utilization = (total_actual_passengers / total_route_capacity) * 100 if total_route_capacity > 0 else 0
    print(f"Overall system utilization:     {system_utilization:.1f}%")
    
    if system_utilization > 75:
        print("   ⚠️  HIGH UTILIZATION - System near capacity")
    elif system_utilization < 40:
        print("   ✓ LOW UTILIZATION - Significant spare capacity")
    else:
        print("   ✓ MODERATE UTILIZATION - Healthy operating range")
    print()
    
    print("\n4. BOTTLENECK IDENTIFICATION:")
    print("-" * 60)
    
    # Identify critical bottlenecks (connections with very low capacity)
    bottleneck_threshold = avg_max_flow_daily * 0.5  # 50% below average
    critical_bottlenecks = [r for r in max_flow_results if r['daily'] < bottleneck_threshold]
    
    if critical_bottlenecks:
        print(f"Found {len(critical_bottlenecks)} critical bottleneck connections:")
        print(f"   (Capacity < {bottleneck_threshold:,.0f} passengers/day)")
        print()
        for b in critical_bottlenecks[:3]:  # Show top 3 worst
            print(f"   • {b['source']} ↔ {b['sink']}: {b['daily']:,.0f} passengers/day")
        print()
        print("⚠️  RECOMMENDATIONS:")
        print("   - These connections severely limit network capacity")
        print("   - Priority areas for adding routes or increasing frequency")
        print("   - Consider express services between these hubs")
    else:
        print("✓ No critical bottlenecks detected")
        print("   All major connections have reasonable capacity")


