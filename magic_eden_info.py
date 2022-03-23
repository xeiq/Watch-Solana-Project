from requests import get
from datetime import datetime
from sqlalchemy import create_engine
from pandas import DataFrame, read_sql
from helpers.read_password import read_password
from os import chdir
from os.path import dirname
from sys import argv

chdir(dirname(argv[0]))

username, password = read_password()
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@127.0.0.1/sol_nft", echo=False
)

projects_info = read_sql(
    """SELECT id, symbol FROM sol_nft.projects""", con=engine
).to_dict(orient="records")

_ = []
for project_info in projects_info:
    response = get(
        f"https://api-mainnet.magiceden.dev/v2/collections/{project_info['symbol']}/stats"
    ).json()

    _.append(
        [project_info["id"], datetime.now().strftime("%Y-%m-%d %H:%M")]
        + [
            response_value
            if response_value < 10000
            else round(response_value / 1000000000, 2)
            for response_value in response.values()
            if type(response_value) is not str
        ]
    )

listing_df = DataFrame(
    _,
    columns=[
        "projects_id",
        "date_added",
        "floor_price",
        "listed_count",
        "average_price_24h",
        "volume_all",
    ],
)

listing_df.to_sql("projects_floor_volume", if_exists="append", con=engine, index=False)
