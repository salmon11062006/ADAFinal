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
            filter_brt_lines (list): List of BRT line numbers to filter (e.g., [1, 2, 3, ..., 14])
                                     If None, loads all data.
            save_filtered (bool): If True, saves filtered data to new txt files
            output_suffix (str): Suffix for output filenames (default: 'brt_1-14')
        """
        print("Loading GTFS data...")
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
        print(f"Building nodes for {len(trip_ids)} trips...")
        
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
    def build_edges_all_trips(self, bus_capacity, time_window_start, time_window_end):
        """
        Build edges for ALL trips in the loaded data (e.g., all BRT lines 1-14).
        Edge = (from_node, to_node, capacity)
        
        Args:
            bus_capacity (int): Capacity per bus
            time_window_start (str): Start time window
            time_window_end (str): End time window
            
        Returns:
            list: List of edge dictionaries
        """
        all_edges = []
        
        # Get all unique trip_ids from frequencies
        trip_ids = self.frequencies['trip_id'].unique()
        print(f"Building edges for {len(trip_ids)} trips...")
        
        # Use tqdm for progress bar
        for trip_id in tqdm(trip_ids, desc="Processing trips", unit="trip"):
            departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
            
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
                        'capacity': bus_capacity,
                        'trip_id': trip_id,
                        'type': 'bus'
                    }
                    all_edges.append(edge)
        
        print(f"Created {len(all_edges)} edges")
        return all_edges
    
    # SUMMARY
    def print_network_summary_all_trips(self, bus_capacity, time_window_start, time_window_end):
        """
        Print summary for the FULL network (all loaded trips).
        
        Args:
            bus_capacity (int): Bus capacity for edges
            time_window_start (str): Start time window
            time_window_end (str): End time window
        """
        nodes = self.build_nodes_all_trips(time_window_start, time_window_end)
        edges = self.build_edges_all_trips(bus_capacity, time_window_start, time_window_end)
        
        trip_count = len(self.frequencies['trip_id'].unique())
        unique_stops = len(set([node['stop_id'] for node in nodes]))
        
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
        print(f"Edge Format: (from_stop, time1) → (to_stop, time2)")
        print(f"Edge Capacity: {bus_capacity} passengers per edge")
        print(f"{'='*80}")

if __name__ == "__main__":
    # Example usage
    network = StopNetwork(gtfs_path='gtfs/')
    
    # Filter for major BRT lines 1-14
    brt_lines = list(range(1, 15))  # [1, 2, 3, ..., 14]
    file_suffix_name = 'BRT_1-14'  # Change this to customize the output filename
    network.load_data(filter_brt_lines=brt_lines, save_filtered=True, output_suffix=file_suffix_name)
    
    # IMPORTANT PARAMETERS!
    # Time window for analysis
    time_start = '06:00:00'  # Morning peak start
    time_end = '09:00:00'    # Morning peak end
    # Bus capacity assumption for weights on edges
    bus_capacity = 85
    
    # ANALYZING SINGLE TRIP
    print("\n" + "="*80)
    print("OPTION 1: Analyze Single Trip")
    print("="*80)
    trip_id = '1-R07'
    print(f"\nAnalyzing trip: {trip_id}")
    network.print_network_summary(trip_id, bus_capacity, time_start, time_end)
    
    # ANALYZING A NETWORK (in this case only for major BRT lines 1-14)
    print("\n\n" + "="*80)
    print("OPTION 2: Analyze FULL BRT Network (Routes 1-14)")
    print("="*80)
    network.print_network_summary_all_trips(bus_capacity, time_start, time_end)

        
