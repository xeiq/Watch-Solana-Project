from solana.rpc.api import Client
from solana.exceptions import SolanaRpcException
from datetime import datetime
from time import sleep
from urllib.error import HTTPError
from pandas import DataFrame, read_sql
from sqlalchemy import create_engine
from helpers.read_password import read_password
from os import chdir
from os.path import dirname
from sys import argv

chdir(dirname(argv[0]))

http_client = Client("https://api.mainnet-beta.solana.com")

username, password = read_password()
engine = create_engine(
    f"mysql+pymysql://{username}:{password}@127.0.0.1/sol_nft", echo=False
)

projects_info = read_sql(
    """SELECT id, pub_key FROM sol_nft.projects""", con=engine
).to_dict(orient="records")

for project_info in projects_info:
    last_quered_signature = read_sql(
        f"""SELECT transaction FROM sol_nft.projects_sales WHERE projects_id = {project_info['id']} and date_added in (SELECT max(date_added) FROM sol_nft.projects_sales WHERE projects_id = {project_info['id']})""",
        con=engine,
    )

    if len(last_quered_signature.transaction.index) > 0:
        signatures = http_client.get_signatures_for_address(
            project_info["pub_key"], until=last_quered_signature.transaction.iloc[0]
        )["result"]
    else:
        signatures = http_client.get_signatures_for_address(project_info["pub_key"])[
            "result"
        ]

    _ = []
    for signature in signatures:
        while True:
            transaction_signature = signature["signature"]
            print(transaction_signature)
            try:
                transaction_details = http_client.get_transaction(
                    transaction_signature
                )["result"]

                price_sell = max(
                    [
                        (preBalance - postBalance) / 1000000000
                        for (preBalance, postBalance) in zip(
                            transaction_details["meta"]["preBalances"],
                            transaction_details["meta"]["postBalances"],
                        )
                    ]
                )

                if price_sell < 0.008:
                    break

                data_sell = datetime.fromtimestamp(
                    transaction_details["blockTime"]
                ).strftime("%Y-%m-%d %H:%M")

                try:
                    token_address = transaction_details["meta"]["postTokenBalances"][0][
                        "mint"
                    ]
                except IndexError:
                    break

                _.append([transaction_signature, token_address, data_sell, price_sell])

            except SolanaRpcException:
                sleep(60)
                continue

            break

    df_sales = DataFrame(_, columns=["transaction", "token", "date_added", "price_sol"])
    df_sales["projects_id"] = project_info["id"]
    df_sales.to_sql("projects_sales", if_exists="append", con=engine, index=False)
