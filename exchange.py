import os
from dotenv import load_dotenv
from mt5_bridge import MT5Connector

def make_mt5():
    load_dotenv()

    login = os.getenv("MT5_LOGIN") or None
    password = os.getenv("MT5_PASSWORD") or None
    server = os.getenv("MT5_SERVER") or None
    path = os.getenv("MT5_PATH") or None

    mt5c = MT5Connector(
        login=int(login) if login else None,
        password=password,
        server=server,
        path=path,
        magic=777001,
        default_deviation=20,
    )
    mt5c.initialize()
    return mt5c
