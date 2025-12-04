import pandas as pd
import calendar

# Load passenger data
df1 = pd.read_excel('data-penumpang-bus-transjakarta-tahun-2021-(1764692849646).xlsx')
df1_BRT = df1[df1['jenis'] == 'BRT'].copy()

df1_BRT['year'] = df1_BRT['periode_data'] // 100
df1_BRT['month'] = df1_BRT['periode_data'] % 100
df1_BRT['days_in_month'] = df1_BRT.apply(
    lambda row: calendar.monthrange(row['year'], row['month'])[1], 
    axis=1
)
df1_BRT['jumlah_penumpang_per_day'] = df1_BRT['jumlah_penumpang'] / df1_BRT['days_in_month']

route_capacity = df1_BRT.groupby('kode_trayek')['jumlah_penumpang_per_day'].mean().to_dict()

print("Passenger Data Routes:")
for k, v in list(route_capacity.items())[:10]:
    print(f"  '{k}' (type: {type(k).__name__}): {v:.0f} passengers/day")

# Load GTFS data
trips = pd.read_csv('gtfs/trips.txt')
routes = pd.read_csv('gtfs/routes.txt')

print("\nGTFS Routes:")
for route_id in routes['route_id'].head(10):
    print(f"  '{route_id}' (type: {type(route_id).__name__})")

print("\nGTFS Trip route_ids (first 10):")
for route_id in trips['route_id'].head(10):
    print(f"  '{route_id}' (type: {type(route_id).__name__})")
    
print(f"\nDoes '1' exist in route_capacity? {('1' in route_capacity)}")
print(f"Does 1 exist in route_capacity? {(1 in route_capacity)}")

if '1' in route_capacity:
    print(f"Capacity for route '1': {route_capacity['1']:.0f}")
