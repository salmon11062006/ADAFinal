import pandas as pd
from collections import defaultdict

# Standard TransJakarta bus capacity (passengers per bus)
BUS_CAPACITY = 85


def get_bus_capacity(route_id=None):
    """
    Get the standard bus capacity (same for all routes).
    
    Args:
        route_id: Route identifier (ignored, kept for compatibility)
    
    Returns:
        int: Passenger capacity of buses (85 passengers)
    """
    return BUS_CAPACITY


def calculate_edge_capacities(gtfs_path='gtfs/', time_period='peak'):
    """
    Calculate edge capacities between consecutive stops for corridors 1-13.
    This represents the maximum passenger flow between each pair of consecutive stops.
    
    Args:
        gtfs_path (str): Path to GTFS data folder
        time_period (str): 'peak' (6-9am, 4-8pm) or 'offpeak'
    
    Returns:
        dict: {route_id: {(stop_from, stop_to): capacity}}
    """
    # Load GTFS data
    routes = pd.read_csv(f'{gtfs_path}routes.txt')
    frequencies = pd.read_csv(f'{gtfs_path}frequencies.txt')
    trips = pd.read_csv(f'{gtfs_path}trips.txt')
    stop_times = pd.read_csv(f'{gtfs_path}stop_times.txt')
    
    # Filter for main corridors 1-13
    main_routes = routes[routes['route_short_name'].astype(str).str.match(r'^[1-9]$|^1[0-3]$')].copy()
    
    # Define time periods
    if time_period == 'peak':
        peak_periods = [('06:00:00', '09:00:00'), ('16:00:00', '20:00:00')]
    else:
        peak_periods = [('09:00:00', '16:00:00')]
    
    edge_capacities = {}
    
    for _, route in main_routes.iterrows():
        route_id = route['route_short_name']
        
        # Get trips for this route
        route_trips = trips[trips['route_id'] == route['route_id']]
        
        # Get frequencies for these trips
        route_freqs = frequencies[frequencies['trip_id'].isin(route_trips['trip_id'])]
        
        # Calculate average buses per hour during time period
        headways = []
        for start, end in peak_periods:
            period_data = route_freqs[
                (route_freqs['start_time'] <= end) & 
                (route_freqs['end_time'] >= start)
            ]
            if not period_data.empty:
                headways.extend(period_data['headway_secs'].tolist())
        
        if not headways:
            # Default: assume 10-minute headway
            buses_per_hour = 6
        else:
            avg_headway = sum(headways) / len(headways)
            buses_per_hour = 3600 / avg_headway
        
        # Edge capacity = buses per hour × bus capacity
        edge_capacity = int(buses_per_hour * BUS_CAPACITY)
        
        # Get stop sequences for this route
        route_edges = {}
        for trip_id in route_trips['trip_id'].unique():
            trip_stops = stop_times[stop_times['trip_id'] == trip_id].sort_values('stop_sequence')
            
            # Create edges between consecutive stops
            stops_list = trip_stops['stop_id'].tolist()
            for i in range(len(stops_list) - 1):
                edge = (stops_list[i], stops_list[i + 1])
                route_edges[edge] = edge_capacity
        
        edge_capacities[str(route_id)] = route_edges
    
    return edge_capacities


def calculate_corridor_capacities(gtfs_path='gtfs/', time_period='peak'):
    """
    Calculate edge capacity for corridors 1-13.
    This is the capacity per edge (segment between consecutive stops).
    
    Args:
        gtfs_path (str): Path to GTFS data folder
        time_period (str): 'peak' (6-9am, 4-8pm) or 'offpeak'
    
    Returns:
        dict: Route ID -> edge capacity mapping
    """
    # Load GTFS data
    routes = pd.read_csv(f'{gtfs_path}routes.txt')
    frequencies = pd.read_csv(f'{gtfs_path}frequencies.txt')
    trips = pd.read_csv(f'{gtfs_path}trips.txt')
    
    # Merge to get route info for each trip
    trip_routes = trips.merge(routes[['route_id', 'route_short_name']], on='route_id')
    freq_with_routes = frequencies.merge(trip_routes[['trip_id', 'route_id', 'route_short_name']], 
                                          on='trip_id')
    
    # Filter for main corridors 1-13
    main_corridors = freq_with_routes[
        freq_with_routes['route_short_name'].astype(str).str.match(r'^[1-9]$|^1[0-3]$')
    ].copy()
    
    # Define peak hours
    if time_period == 'peak':
        # Morning peak: 6-9am, Evening peak: 16-20 (4-8pm)
        peak_periods = [
            ('06:00:00', '09:00:00'),
            ('16:00:00', '20:00:00')
        ]
    else:
        # Off-peak: 9am-4pm
        peak_periods = [('09:00:00', '16:00:00')]
    
    corridor_capacities = {}
    
    # Group by route and calculate capacity per edge
    for route_id in main_corridors['route_short_name'].unique():
        route_data = main_corridors[main_corridors['route_short_name'] == route_id]
        
        # Calculate average headway during specified time period
        headways = []
        for start, end in peak_periods:
            period_data = route_data[
                (route_data['start_time'] <= end) & 
                (route_data['end_time'] >= start)
            ]
            if not period_data.empty:
                headways.extend(period_data['headway_secs'].tolist())
        
        if headways:
            avg_headway = sum(headways) / len(headways)
            buses_per_hour = 3600 / avg_headway
            # Edge capacity = buses/hour * capacity/bus
            edge_capacity = buses_per_hour * BUS_CAPACITY
            corridor_capacities[str(route_id)] = int(edge_capacity)
        else:
            # Default capacity if no frequency data
            corridor_capacities[str(route_id)] = BUS_CAPACITY * 6
    
    return corridor_capacities


def get_corridor_capacity_summary(gtfs_path='gtfs/'):
    """
    Get a summary of corridor capacities for both peak and off-peak periods.
    Edge capacity represents the maximum flow between consecutive stops.
    
    Args:
        gtfs_path (str): Path to GTFS data folder
    
    Returns:
        pd.DataFrame: Summary table with corridor capacities
    """
    peak_cap = calculate_corridor_capacities(gtfs_path, 'peak')
    offpeak_cap = calculate_corridor_capacities(gtfs_path, 'offpeak')
    
    # Create summary dataframe
    corridors = sorted(set(list(peak_cap.keys()) + list(offpeak_cap.keys())), 
                       key=lambda x: int(''.join(filter(str.isdigit, x)) or '0'))
    
    summary = []
    for corridor in corridors:
        if corridor in ['1', '2', '3', '4', '5', '6', '7', '8', '9', '10', '11', '12', '13']:
            summary.append({
                'Corridor': corridor,
                'Bus Capacity': BUS_CAPACITY,
                'Peak Edge Capacity': peak_cap.get(corridor, 0),
                'Off-Peak Edge Capacity': offpeak_cap.get(corridor, 0)
            })
    
    return pd.DataFrame(summary)


if __name__ == '__main__':
    print("="*70)
    print("TRANSJAKARTA CORRIDOR CAPACITY ANALYSIS (Corridors 1-13)")
    print("="*70)
    
    # Get capacity summary
    summary = get_corridor_capacity_summary()
    
    print("\nCapacity Summary (per edge/segment):")
    print(summary.to_string(index=False))
    
    print("\n" + "="*70)
    print("Notes:")
    print("- Peak hours: 6-9am and 4-8pm")
    print("- Off-peak hours: 9am-4pm")
    print("- Edge capacity = (3600/headway) × bus_capacity")
    print("- Each edge represents flow between consecutive stops")
    print(f"- Standard bus capacity: {BUS_CAPACITY} passengers")
    print("- Passengers can board/alight at any stop along the route")
    print("="*70)
