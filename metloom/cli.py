"""Console script for metloom."""
import argparse
import sys
import geopandas as gpd

from metloom.pointdata import CDECPointData, SnotelPointData


def main():
    """Console script for metloom."""

    datasouce_map = {
        "cdec": CDECPointData,
        "snotel": SnotelPointData
    }
    parser = argparse.ArgumentParser(
        description="Find measurement locations within a shapefile"
    )
    parser.add_argument(
        "--shapefile", "-sf", dest="shapefile", required=True,
        help="shapefile used to filter measurement locations"
    )
    parser.add_argument(
        "--datasource", "-ds", dest="datasource",
        choices=list(datasouce_map.keys()),
        required=True, help="datasource used to find measurement ids"
    )
    parser.add_argument(
        "--variables", "-v", dest="variables", nargs="+", required=True,
        help="measurement must posses at least one of these variable. "
             "Variables must be the attribute name of the variables"
             "corresponding to the datasource. i.e. SWE"
    )

    parser.add_argument(
        "--snowcourse", "-sc", dest="snowcourse", default=False,
        action="store_true", required=False,
        help="Do you want to find only snowcourses"
    )

    parser.add_argument(
        "--ouput", "-o", dest="output", default="./output.csv",
        required=False, help="output csv file path"
    )

    args = parser.parse_args()
    datasource = datasouce_map.get(args.datasource)
    if datasource is None:
        raise ValueError("Not a valid datasource")
    geometry = gpd.read_file(args.shapefile)

    variables = []
    for variable_string in args.variables:
        variable = getattr(datasource.ALLOWED_VARIABLES, variable_string, None)
        if variable is None:
            raise ValueError(
                f"Could not find variable corresponding to {variable_string}"
            )
        variables.append(variable)

    points = datasource.points_from_geometry(
        geometry, variables, snow_courses=args.snowcourse
    )

    points.to_dataframe().to_csv(args.output)


if __name__ == "__main__":
    sys.exit(main())  # pragma: no cover
