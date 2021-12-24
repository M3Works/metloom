import geopandas as gpd
from shapely.geometry import Polygon
#-119.8 37.7, -119.8 38.2, -119.2 38.2, -119.2 37.7, -119.8 37.7

df = gpd.GeoDataFrame.from_dict({}, geometry=gpd.GeoSeries(Polygon([[-119.8, 37.7], [-119.2, 37.7], [-119.5, 38.2], [-119.8, 37.7]])), crs=4326)
df.to_file('triangle.shp')
