from pathlib import Path
import click
from sqlalchemy import create_engine
import datetime
import geopandas as gpd
import pandas as pd
import tomli


HOME_DIR = Path.cwd()


def create_db_engine(config):
    return create_engine(
        f"postgresql+psycopg://{config['db']['user']}:{config['db']['password']}"
        f"@{config['db']['host']}:{config['db']['port']}/{config['db']['name']}",
        connect_args={'options': f'-csearch_path={config["app"]["name"]},public'},
    )


def convert_dates_to_str(response):
    for col in response.columns:
        if type(response[col][0]) == datetime.date:
            response[col] = response[col].astype(str)

    return response


@click.command()
@click.option("-f", "--filename")
@click.option("-q", "--query")
def main(filename=None, query=None):
    # Make sure a file or a query string is provided
    if not (filename or query):
        print("ERROR: You must provide a query '-q' or a sql file '-f' to sqeegeo.")
        return -1

    if (filename and query):
        print("ERROR: You can provide EITHER a query '-q' or a sql file '-f' to sqeegeo, NOT BOTH.")
        return -1


    # Open config
    #    - TODO add an argument to set config file
    #    - TODO add config pattern to README

    with open(HOME_DIR / "config.toml", "rb") as f:
        config = tomli.load(f)

    # Create db_engine
    db_engine = create_db_engine(config)

    # If file, open file
    if filename is not None:
        q = Path(filename).read_text()
     
    # If query, open query
    else:
        q = query
    
    #    - TODO add a option to select geometry column
    try:
        response = gpd.read_postgis(query, db_engine)

    except ValueError:
        response = gpd.read_postgis(query, db_engine, geom_col="geometry")
        
    except ValueError:
        print("There isn't a findable geometry column on that response.")
        return -1

    #    - TODO add an output filename, else print

    type_corrected = convert_dates_to_str(response)

    print(type_corrected.to_json())
    return 0


if __name__ == "__main__":
    main()
