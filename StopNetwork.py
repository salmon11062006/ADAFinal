import pandas as pd
from tqdm import tqdm  # Progress bar HEHEHEHA

class StopNetwork:
    def __init__(self, gtfs_path='gtfs/'):
        self.gtfs_path = gtfs_path
        self.stop_times = None
        self.stops = None
        self.frequencies = None
        self.stop_info = {}
        self._trip_cache = {}  # Cache for expanded trips
        
    def load_data(self, filter_brt_lines=None, save_filtered=False, output_suffix='brt_1-14'):
        """
        Load GTFS data files.
        
        Args:
            filter_brt_lines (list): List of BRT line numbers to filter (1-14)
                                     If None, loads all data.
            save_filtered (bool): If True, saves filtered data to new txt files
            output_suffix (str): Suffix for output filenames (default: 'brt_1-14')
        """
        self.stop_times = pd.read_csv(f'{self.gtfs_path}stop_times.txt')
        self.stops = pd.read_csv(f'{self.gtfs_path}stops.txt')
        self.frequencies = pd.read_csv(f'{self.gtfs_path}frequencies.txt')
        
        # Filter for major BRT lines if specified
        if filter_brt_lines:
            print(f"Filtering for BRT lines: {filter_brt_lines}")
            # Create pattern to match trip_ids starting with line numbers
            # e.g., '1-', '2-', '3-', ... '14-'
            pattern = '|'.join([f'^{line}-' for line in filter_brt_lines])
            
            # Filter frequencies
            self.frequencies = self.frequencies[
                self.frequencies['trip_id'].str.match(pattern, na=False)
            ]
            
            # Get trip_ids from filtered frequencies
            valid_trip_ids = self.frequencies['trip_id'].unique()
            
            # Filter stop_times
            self.stop_times = self.stop_times[
                self.stop_times['trip_id'].isin(valid_trip_ids)
            ]
            
            print(f"Filtered to {len(valid_trip_ids)} trips")
            
            # Save filtered data if requested
            if save_filtered:
                freq_filename = f'{self.gtfs_path}frequencies_{output_suffix}.txt'
                stop_times_filename = f'{self.gtfs_path}stop_times_{output_suffix}.txt'
                
                self.frequencies.to_csv(freq_filename, index=False)
                self.stop_times.to_csv(stop_times_filename, index=False)
                
                print(f"Saved filtered frequencies to: {freq_filename}")
                print(f"Saved filtered stop_times to: {stop_times_filename}")
        
        # Sort stop_times by trip and sequence
        self.stop_times = self.stop_times.sort_values(['trip_id', 'stop_sequence'])
        
        # Create stop info dictionary
        for _, stop in self.stops.iterrows():
            self.stop_info[stop['stop_id']] = {
                'name': stop['stop_name'],
                'lat': stop['stop_lat'],
                'lon': stop['stop_lon']
            }
        
        print(f"Loaded {len(self.stops)} stops")
    
    # This is only for one single trip (a single bus)
    def get_single_trip(self, trip_id):
        """
        Get all stops and timing for a single trip.
        
        Args:
            trip_id (str): Trip ID to analyze
            
        Returns:
            pd.DataFrame: Stop sequence with timing information
        """
        trip_data = self.stop_times[self.stop_times['trip_id'] == trip_id].copy()
        trip_data = trip_data.sort_values('stop_sequence')
        
        # Add stop names
        trip_data['stop_name'] = trip_data['stop_id'].map(
            lambda x: self.stop_info.get(x, {}).get('name', 'Unknown')
        )
        
        return trip_data[['stop_sequence', 'stop_id', 'stop_name', 
                         'arrival_time', 'departure_time']]
    
    def _time_to_seconds(self, time_str):
        """Convert HH:MM:SS to seconds."""
        parts = time_str.split(':')
        return int(parts[0]) * 3600 + int(parts[1]) * 60 + int(parts[2])
    
    def _seconds_to_time(self, seconds):
        """Convert seconds to HH:MM:SS."""
        hours = seconds // 3600
        minutes = (seconds % 3600) // 60
        secs = seconds % 60
        return f"{hours:02d}:{minutes:02d}:{secs:02d}"
    
    def get_trip_with_frequencies(self, trip_id, time_window_start=None, time_window_end=None, use_cache=True):
        """
        Get all departures for a trip based on frequency data.
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time (HH:MM:SS) to filter, optional
            time_window_end (str): End time (HH:MM:SS) to filter, optional
            use_cache (bool): Use cached results if available
            
        Returns:
            list: List of DataFrames, one for each departure
        """
        # Check cache
        cache_key = (trip_id, time_window_start, time_window_end)
        if use_cache and cache_key in self._trip_cache:
            return self._trip_cache[cache_key]
        
        # Get base trip schedule
        base_trip = self.get_single_trip(trip_id)
        
        if base_trip.empty:
            return []
        
        # Get frequency information for this trip
        freq_data = self.frequencies[self.frequencies['trip_id'] == trip_id]
        
        if freq_data.empty:
            result = [base_trip]
            self._trip_cache[cache_key] = result
            return result
        
        all_departures = []
        
        # Process each frequency period
        for _, freq in freq_data.iterrows():
            start_time = freq['start_time']
            end_time = freq['end_time']
            headway_secs = freq['headway_secs']
            
            # Apply time window filter if provided
            if time_window_start:
                start_time = max(start_time, time_window_start)
            if time_window_end:
                end_time = min(end_time, time_window_end)
            
            start_secs = self._time_to_seconds(start_time)
            end_secs = self._time_to_seconds(end_time)
            
            # Get base trip first stop ARRIVAL time (when the trip "starts")
            base_first_arrival = base_trip.iloc[0]['arrival_time']
            base_first_secs = self._time_to_seconds(base_first_arrival)
            
            # Generate departures at each headway interval
            current_departure_secs = start_secs
            
            while current_departure_secs < end_secs:
                # Calculate time offset from base trip
                offset_secs = current_departure_secs - base_first_secs
                
                # Create shifted trip
                shifted_trip = base_trip.copy()
                shifted_trip['arrival_time'] = shifted_trip['arrival_time'].apply(
                    lambda t: self._seconds_to_time(self._time_to_seconds(t) + offset_secs)
                )
                shifted_trip['departure_time'] = shifted_trip['departure_time'].apply(
                    lambda t: self._seconds_to_time(self._time_to_seconds(t) + offset_secs)
                )
                
                all_departures.append(shifted_trip)
                current_departure_secs += headway_secs
        
        # Cache the result
        self._trip_cache[cache_key] = all_departures
        return all_departures
    
    def print_trip_with_frequencies(self, trip_id, time_window_start=None, time_window_end=None):
        """
        Print all departures for a trip based on frequency data.
        
        Args:
            trip_id (str): Trip ID to display
            time_window_start (str): Start time (HH:MM:SS) to filter, optional
            time_window_end (str): End time (HH:MM:SS) to filter, optional
        """
        departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
        
        for i, departure in enumerate(departures, 1):
            first_arrival = departure.iloc[0]['arrival_time']
            print(f"\n{'='*80}")
            print(f"Trip ID: {trip_id} - Arrival #{i} at {first_arrival}")
            print(f"{'='*80}")
            print(departure.to_string(index=True))
    
    # STEP 1: Implementing the nodes and edges for ALL trips in the network
    # NODES   
    def build_nodes(self, trip_id, time_window_start, time_window_end):
        """
        Extract all (stop_id, time) nodes from the frequency-expanded trips.
        Node = (stop_id, arrival_time)
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time window
            time_window_end (str): End time window
            
        Returns:
            list: List of (stop_id, stop_name, time) tuples
        """
        departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
        
        nodes = []
        for departure_df in departures:
            for _, row in departure_df.iterrows():
                node = {
                    'stop_id': row['stop_id'],
                    'stop_name': row['stop_name'],
                    'arrival_time': row['arrival_time']
                }
                nodes.append(node)
        
        return nodes
    
    def visualize_nodes(self, trip_id, time_window_start, time_window_end):
        """
        Visualize all time-expanded nodes.
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time window
            time_window_end (str): End time window
        """
        nodes = self.build_nodes(trip_id, time_window_start, time_window_end)
        
        # Group by stop for better visualization
        from collections import defaultdict
        stops_dict = defaultdict(list)
        
        for node in nodes:
            stops_dict[node['stop_id']].append(node)
        
        for stop_id, stop_nodes in stops_dict.items():
            stop_name = stop_nodes[0]['stop_name']
            print(f"\n{stop_name} ({stop_id}):")
            for node in stop_nodes:
                print(f"  Arrival Time: {node['arrival_time']}")
    
    # EDGES
    def build_edges(self, trip_id, bus_capacity, time_window_start, time_window_end):
        """
        Build edges representing bus travel between consecutive stops.
        Edge = (from_node, to_node, capacity)
        
        Args:
            trip_id (str): Trip ID to analyze
            bus_capacity (int): Bus capacity per edge
            time_window_start (str): Start time window
            time_window_end (str): End time window
            
        Returns:
            list: List of edge dictionaries with capacity
        """
        departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
        
        edges = []
        weight = bus_capacity
        
        # For each bus departure, create edges between consecutive stops
        for departure_df in departures:
            for i in range(len(departure_df) - 1):
                current_stop = departure_df.iloc[i]
                next_stop = departure_df.iloc[i + 1]
                
                edge = {
                    'from_stop': current_stop['stop_id'],
                    'from_name': current_stop['stop_name'],
                    'from_time': current_stop['arrival_time'],
                    'to_stop': next_stop['stop_id'],
                    'to_name': next_stop['stop_name'],
                    'to_time': next_stop['arrival_time'],
                    'capacity': weight,
                    'type': 'bus'
                }
                edges.append(edge)
        
        return edges
    
    def visualize_edges(self, trip_id, bus_capacity, time_window_start, time_window_end):
        """
        Visualize all bus travel edges.
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time window
            time_window_end (str): End time window
        """
        edges = self.build_edges(trip_id, bus_capacity, time_window_start, time_window_end)
        
        for i, edge in enumerate(edges, 1):
            print(f"\nEdge {i}:")
            print(f"  ({edge['from_name']}, {edge['from_time']}) →")
            print(f"  ({edge['to_name']}, {edge['to_time']})")
            print(f"  Capacity: {edge['capacity']} passengers")
    
    def print_network_summary(self, trip_id, bus_capacity, time_window_start, time_window_end):
        """
        Print a summary of the network structure.
        
        Args:
            trip_id (str): Trip ID to analyze
            bus_capacity (int): Bus capacity for edges
            time_window_start (str): Start time window
            time_window_end (str): End time window
        """
        nodes = self.build_nodes(trip_id, time_window_start, time_window_end)
        edges = self.build_edges(trip_id, bus_capacity, time_window_start, time_window_end)
        
        print(f"\n{'='*80}")
        print(f"NETWORK SUMMARY")
        print(f"{'='*80}")
        print(f"Trip ID: {trip_id}")
        print(f"Time Window: {time_window_start} to {time_window_end}")
        print(f"Bus Capacity: {bus_capacity} passengers")
        print(f"\n{'-'*80}")
        print(f"Total Nodes: {len(nodes)}")
        print(f"Node Format: (stop_id, time)")
        print(f"\n{'-'*80}")
        print(f"Total Edges: {len(edges)}")
        print(f"Edge Format: (from_stop, time1) → (to_stop, time2)")
        print(f"Edge Capacity: {bus_capacity} passengers per edge")
        print(f"{'='*80}")

    # IMPLEMENTING FOR ALL TRIPS IN THE NETWORK (BE CAREFUL WITH PERFORMANCE!)
    # NODES
    def build_nodes_all_trips(self, time_window_start, time_window_end):
        """
        Build nodes for ALL trips in the loaded data (e.g., all BRT lines 1-14).
        Node = (stop_id, arrival_time)
        
        Args:
            time_window_start (str): Start time window
            time_window_end (str): End time window
            
        Returns:
            list: List of node dictionaries
        """
        all_nodes = []
        
        # Get all unique trip_ids from frequencies
        trip_ids = self.frequencies['trip_id'].unique()
        print(f"Building nodes for {len(trip_ids)} trips")
        
        # Use tqdm for progress bar
        for trip_id in tqdm(trip_ids, desc="Processing trips", unit="trip"):
            departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
            
            for departure_df in departures:
                for _, row in departure_df.iterrows():
                    node = {
                        'stop_id': row['stop_id'],
                        'stop_name': row['stop_name'],
                        'arrival_time': row['arrival_time'],
                        'trip_id': trip_id
                    }
                    all_nodes.append(node)
        
        print(f"Created {len(all_nodes)} nodes")
        return all_nodes
    
    # EDGES
    def build_edges_all_trips(self, bus_capacity, time_window_start, time_window_end, min_transfer_time=120, max_transfer_time=1800):
        """
        Build edges for ALL trips in the loaded data (e.g., all BRT lines 1-14).
        This includes BOTH:
        1. Bus edges: connections between consecutive stops on the same bus
        2. Transfer edges: waiting at same stop to catch different bus
        
        Args:
            bus_capacity (int): Capacity per bus
            time_window_start (str): Start time window
            time_window_end (str): End time window
            min_transfer_time (int): Minimum transfer time in seconds (default 120s = 2min)
            max_transfer_time (int): Maximum transfer time in seconds (default 1800s = 30min)
            
        Returns:
            list: List of edge dictionaries
        """
        all_edges = []
        all_nodes = []
        
        # Get all unique trip_ids from frequencies
        trip_ids = self.frequencies['trip_id'].unique()
        print(f"Building edges for {len(trip_ids)} trips")
        
        # STEP 1: Build bus edges
        # Use tqdm for progress bar
        for trip_id in tqdm(trip_ids, desc="Building bus edges", unit="trip"):
            departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
            
            # For each bus departure, create edges between consecutive stops
            for departure_df in departures:
                # Store nodes for transfer edge calculation
                for _, row in departure_df.iterrows():
                    all_nodes.append({
                        'stop_id': row['stop_id'],
                        'stop_name': row['stop_name'],
                        'arrival_time': row['arrival_time'],
                        'trip_id': trip_id
                    })
                
                for i in range(len(departure_df) - 1):
                    current_stop = departure_df.iloc[i]
                    next_stop = departure_df.iloc[i + 1]
                    
                    edge = {
                        'from_stop': current_stop['stop_id'],
                        'from_name': current_stop['stop_name'],
                        'from_time': current_stop['arrival_time'],
                        'to_stop': next_stop['stop_id'],
                        'to_name': next_stop['stop_name'],
                        'to_time': next_stop['arrival_time'],
                        'capacity': bus_capacity,
                        'trip_id': trip_id,
                        'type': 'bus'
                    }
                    all_edges.append(edge)
        
        bus_edge_count = len(all_edges)
        
        # STEP 2: Build transfer edges at same stops
        nodes_by_stop = {}
        for node in all_nodes:
            stop_id = node['stop_id']
            if stop_id not in nodes_by_stop:
                nodes_by_stop[stop_id] = []
            nodes_by_stop[stop_id].append(node)
        
        # For each stop, create transfer edges between different trips
        for stop_id, stop_nodes in tqdm(nodes_by_stop.items(), desc="Building transfers"):
            stop_nodes_sorted = sorted(stop_nodes, key=lambda x: self._time_to_seconds(x['arrival_time']))
            
            for i, from_node in enumerate(stop_nodes_sorted):
                from_time_sec = self._time_to_seconds(from_node['arrival_time'])
                
                for j in range(i+1, len(stop_nodes_sorted)):
                    to_node = stop_nodes_sorted[j]
                    to_time_sec = self._time_to_seconds(to_node['arrival_time'])
                    
                    time_diff = to_time_sec - from_time_sec
                    
                    # Create transfer edge if different trips and within time window
                    if (from_node['trip_id'] != to_node['trip_id'] and
                        min_transfer_time <= time_diff <= max_transfer_time):
                        
                        all_edges.append({
                            'from_stop': from_node['stop_id'],
                            'from_name': from_node['stop_name'],
                            'from_time': from_node['arrival_time'],
                            'to_stop': to_node['stop_id'],
                            'to_name': to_node['stop_name'],
                            'to_time': to_node['arrival_time'],
                            'capacity': 999999,  # High capacity - transfers not bus-limited
                            'trip_id': f"{from_node['trip_id']}→{to_node['trip_id']}",
                            'type': 'transfer'
                        })
        
        transfer_edge_count = len(all_edges) - bus_edge_count
        print(f"Created {bus_edge_count} bus edges + {transfer_edge_count} transfer edges = {len(all_edges)} total")
        
        print(f"Created {len(all_edges)} edges")
        return all_edges
    
    # SUMMARY
    def print_network_summary_all_trips(self, bus_capacity, time_window_start, time_window_end, min_transfer_time=120, max_transfer_time=1800):
        """
        Print summary of the full network.
        
        Args:
            bus_capacity (int): Bus capacity for edges
            time_window_start (str): Start time window
            time_window_end (str): End time window
            min_transfer_time (int): Minimum transfer time in seconds (default 120s = 2min)
            max_transfer_time (int): Maximum transfer time in seconds (default 1800s = 30min)
        """
        nodes = self.build_nodes_all_trips(time_window_start, time_window_end)
        edges = self.build_edges_all_trips(bus_capacity, time_window_start, time_window_end, min_transfer_time, max_transfer_time)
        
        trip_count = len(self.frequencies['trip_id'].unique())
        unique_stops = len(set([node['stop_id'] for node in nodes]))
        bus_edges = sum(1 for e in edges if e.get('type') == 'bus')
        transfer_edges = sum(1 for e in edges if e.get('type') == 'transfer')
        
        print(f"\n{'='*80}")
        print(f"FULL NETWORK SUMMARY (All BRT Routes)")
        print(f"{'='*80}")
        print(f"Number of Routes/Trips: {trip_count}")
        print(f"Time Window: {time_window_start} to {time_window_end}")
        print(f"Bus Capacity: {bus_capacity} passengers")
        print(f"\n{'-'*80}")
        print(f"Total Nodes: {len(nodes)}")
        print(f"Unique Stops: {unique_stops}")
        print(f"Node Format: (stop_id, time)")
        print(f"\n{'-'*80}")
        print(f"Total Edges: {len(edges)}")
        print(f"  - Bus edges: {bus_edges} (riding on a bus)")
        print(f"  - Transfer edges: {transfer_edges} (waiting at stop for another bus)")
        print(f"Edge Format: (from_stop, time1) → (to_stop, time2)")
        print(f"{'='*80}")
    
    # STEP 3: Convert to Graph for Edmonds-Karp
    def build_flow_graph(self, source_stop, dest_stop, bus_capacity, time_window_start, time_window_end, min_transfer_time=120, max_transfer_time=1800):
        """
        Build a flow network graph for Edmonds-Karp algorithm.
        
        Args:
            source_stop (str): Source stop ID (e.g., 'P00017' for Blok M)
            dest_stop (str): Destination stop ID (e.g., 'G00060' for Kali Besar)
            bus_capacity (int): Capacity per bus
            time_window_start (str): Start time window
            time_window_end (str): End time window
            min_transfer_time (int): Minimum transfer time in seconds (default 120s = 2min)
            max_transfer_time (int): Maximum transfer time in seconds (default 1800s = 30min)
            
        Returns:
            tuple: (Graph object, node_to_index dict, index_to_node dict, source_idx, sink_idx)
        """
        from edmondskarp import Graph
        
        print(f"\nBuilding flow graph from {source_stop} to {dest_stop}")
        
        # Get all nodes and edges
        nodes = self.build_nodes_all_trips(time_window_start, time_window_end)
        edges = self.build_edges_all_trips(bus_capacity, time_window_start, time_window_end, min_transfer_time, max_transfer_time)
        
        # Create unique node identifiers: (stop_id, time)
        unique_nodes = set()
        for node in nodes:
            unique_nodes.add((node['stop_id'], node['arrival_time']))
        
        # Add super source and super sink
        unique_nodes.add(('SOURCE', '00:00:00'))
        unique_nodes.add(('SINK', '23:59:59'))
        
        unique_nodes_list = list(unique_nodes)
        node_count = len(unique_nodes_list)
        
        # Create mapping: node -> index
        node_to_index = {node: idx for idx, node in enumerate(unique_nodes_list)}
        index_to_node = {idx: node for node, idx in node_to_index.items()}
        
        # Create graph
        graph = Graph(node_count)
        
        # Add vertex labels
        for idx, (stop_id, time) in index_to_node.items():
            stop_name = self.stop_info.get(stop_id, {}).get('name', stop_id)
            graph.add_vertex_data(idx, f"{stop_name}@{time}")
        
        # Add edges from our network
        for edge in edges:
            from_node = (edge['from_stop'], edge['from_time'])
            to_node = (edge['to_stop'], edge['to_time'])
            
            if from_node in node_to_index and to_node in node_to_index:
                from_idx = node_to_index[from_node]
                to_idx = node_to_index[to_node]
                graph.add_edge(from_idx, to_idx, edge['capacity'])
        
        # Connect super source to all nodes at source_stop
        source_idx = node_to_index[('SOURCE', '00:00:00')]
        for node in nodes:
            if node['stop_id'] == source_stop:
                node_key = (node['stop_id'], node['arrival_time'])
                if node_key in node_to_index:
                    target_idx = node_to_index[node_key]
                    graph.add_edge(source_idx, target_idx, float('inf'))
        
        # Connect all nodes at dest_stop to super sink
        sink_idx = node_to_index[('SINK', '23:59:59')]
        for node in nodes:
            if node['stop_id'] == dest_stop:
                node_key = (node['stop_id'], node['arrival_time'])
                if node_key in node_to_index:
                    from_idx = node_to_index[node_key]
                    graph.add_edge(from_idx, sink_idx, float('inf'))
        
        print(f"Graph created with {node_count} nodes and {len(edges)} edges")
        
        return graph, node_to_index, index_to_node, source_idx, sink_idx


if __name__ == "__main__":
    '''THIS PART IS FOR TESTING THE StopNetwork CLASS (Building the graph and visualizing nodes/edges by refering to tranjakarta GTFS data)'''
    # Example usage - just build the network
    network = StopNetwork(gtfs_path='gtfs/')
    
    # Filter for major BRT lines 1-14
    brt_lines = list(range(1, 15))  # 1 to 14
    file_suffix_name = 'BRT_1-14'  # Change this to customize the output filename
    network.load_data(filter_brt_lines=brt_lines, save_filtered=True, output_suffix=file_suffix_name)
    
    # IMPORTANT PARAMETERS!
    # Time window for analysis
    time_start = '06:00:00'  # Morning peak start
    time_end = '07:00:00'    # Morning peak end (reduced to 1 hour for demo)
    # Bus capacity assumption for weights on edges
    bus_capacity = 85
    # Transfer constraints
    min_transfer_time = 120   # Minimum 2 minutes to transfer (120 seconds)
    max_transfer_time = 1800  # Maximum 30 minutes wait (1800 seconds)
    
    # ANALYZING SINGLE TRIP
    print("\n" + "="*80)
    print("OPTION 1: Analyze Single Trip")
    print("="*80)
    trip_id = '1-R07'
    print(f"\nAnalyzing trip: {trip_id}")
    network.print_network_summary(trip_id, bus_capacity, time_start, time_end)

    network.visualize_nodes(trip_id, time_start, time_end)
    network.visualize_edges(trip_id, bus_capacity, time_start, time_end)
    
    # ANALYZING A NETWORK (in this case only for major BRT lines 1-14)
    print("\n\n" + "="*80)
    print("OPTION 2: Analyze FULL BRT Network (Routes 1-14)")
    print("="*80)
    network.print_network_summary_all_trips(bus_capacity, time_start, time_end, min_transfer_time, max_transfer_time)
