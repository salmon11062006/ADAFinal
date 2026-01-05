import pandas as pd


class StopNetwork:
    def __init__(self, gtfs_path='gtfs/'):
        self.gtfs_path = gtfs_path
        self.stop_times = None
        self.stops = None
        self.frequencies = None
        self.stop_info = {}
        
    def load_data(self):
        """Load GTFS data files."""
        print("Loading GTFS data...")
        self.stop_times = pd.read_csv(f'{self.gtfs_path}stop_times.txt')
        self.stops = pd.read_csv(f'{self.gtfs_path}stops.txt')
        self.frequencies = pd.read_csv(f'{self.gtfs_path}frequencies.txt')
        
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
    
    def get_trip_with_frequencies(self, trip_id, time_window_start=None, time_window_end=None):
        """
        Get all departures for a trip based on frequency data.
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time (HH:MM:SS) to filter, optional
            time_window_end (str): End time (HH:MM:SS) to filter, optional
            
        Returns:
            list: List of DataFrames, one for each departure
        """
        # Get base trip schedule
        base_trip = self.get_single_trip(trip_id)
        
        if base_trip.empty:
            print(f"No trip found with ID: {trip_id}")
            return []
        
        # Get frequency information for this trip
        freq_data = self.frequencies[self.frequencies['trip_id'] == trip_id]
        
        if freq_data.empty:
            print(f"No frequency data found for trip: {trip_id}")
            return [base_trip]
        
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
    
    def build_time_expanded_nodes(self, trip_id, time_window_start, time_window_end):
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
        nodes = self.build_time_expanded_nodes(trip_id, time_window_start, time_window_end)
        
        print(f"\n{'='*80}")
        print(f"TIME-EXPANDED NODES for Trip {trip_id}")
        print(f"Time Window: {time_window_start} to {time_window_end}")
        print(f"{'='*80}")
        print(f"\nTotal Nodes: {len(nodes)}")
        print(f"\nNode Format: (stop_id, time)")
        print(f"{'-'*80}")
        
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
    
    def build_bus_edges(self, trip_id, time_window_start, time_window_end):
        """
        Build edges representing bus travel between consecutive stops.
        Edge = (from_node, to_node, capacity)
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time window
            time_window_end (str): End time window
            
        Returns:
            list: List of edge dictionaries with capacity
        """
        departures = self.get_trip_with_frequencies(trip_id, time_window_start, time_window_end)
        
        edges = []
        bus_capacity = 85
        
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
                    'type': 'bus'
                }
                edges.append(edge)
        
        return edges
    
    def visualize_edges(self, trip_id, time_window_start, time_window_end):
        """
        Visualize all bus travel edges.
        
        Args:
            trip_id (str): Trip ID to analyze
            time_window_start (str): Start time window
            time_window_end (str): End time window
        """
        edges = self.build_bus_edges(trip_id, time_window_start, time_window_end)
        
        for i, edge in enumerate(edges, 1):
            print(f"\nEdge {i}:")
            print(f"  ({edge['from_name']}, {edge['from_time']}) →")
            print(f"  ({edge['to_name']}, {edge['to_time']})")
            print(f"  Capacity: {edge['capacity']} passengers")

        print(f"\n{'='*80}")
        print(f"BUS TRAVEL EDGES for Trip {trip_id}")
        print(f"Time Window: {time_window_start} to {time_window_end}")
        print(f"\nTotal Edges: {len(edges)}")
        print(f"\nEdge Format: (from_stop, time1) → (to_stop, time2) [capacity]")
        print(f"{'='*80}")

if __name__ == "__main__":
    # Example usage
    network = StopNetwork(gtfs_path='gtfs/')
    network.load_data()
    
    # Example: Show trip with frequencies
    trip_id = '1-R07'
    time_start = '05:00:00'
    time_end = '05:15:00'  
    
    print(f"\nAnalyzing trip: {trip_id} with frequencies")
    network.print_trip_with_frequencies(trip_id, time_start, time_end)
    
    print("\n\n")
    print("="*80)
    print("STEP 1: TIME-EXPANDED NODES")
    print("="*80)
    network.visualize_nodes(trip_id, time_start, time_end)
    
    print("\n\n")
    print("="*80)
    print("STEP 2: BUS TRAVEL EDGES")
    print("="*80)
    network.visualize_edges(trip_id, time_start, time_end)

        
