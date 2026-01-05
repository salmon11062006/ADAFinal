import StopNetwork

network = StopNetwork.StopNetwork(gtfs_path='gtfs/')
network.load_data()

trip_id = '1-R07'
print(f"\nTrip ID: {trip_id}\n{network.get_single_trip(trip_id)}")