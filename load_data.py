import sqlite3
import pandas as pd

DB_PATH = "./data/hl_1s_kline.db"

def load_all():
    conn = sqlite3.connect(DB_PATH)
    df = pd.read_sql_query("SELECT * FROM kline_1s ORDER BY ts ASC", conn)
    conn.close()

    df["ts"] = pd.to_datetime(df["ts"])
    print(df.head())
    return df

if __name__ == "__main__":
    load_all()
