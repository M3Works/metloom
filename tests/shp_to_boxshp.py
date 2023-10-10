import geopandas as gpd
import argparse

from metloom.dataframe_utils import shp_to_box


def main():
    parser = argparse.ArgumentParser(
        description="Convert shapefile to a box of the shapefile"
    )
    parser.add_argument(
        "shapefile",
        help="shapefile used for conversion"
    )
    args = parser.parse_args()

    df = gpd.read_file(args.shapefile)
    # Calculate the bounding box of the entire GeoDataFrame
    gdf_bbox = shp_to_box(df)

    # Write the bounding box GeoDataFrame to a new shapefile
    gdf_bbox.to_file("austria_box.shp")


if __name__ == '__main__':
    main()
