import geopandas as gpd
from shapely.geometry import Polygon

coords = [[-119.8, 37.7], [-119.2, 37.7], [-119.5, 38.2], [-119.8, 37.7]]
df = gpd.GeoDataFrame.from_dict({},
                                geometry=gpd.GeoSeries(Polygon(coords),
                                                       crs=4326))
df.to_file('triangle.shp')
