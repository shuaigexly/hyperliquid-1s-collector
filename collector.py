import time
import pandas as pd
import requests
import yaml
from db import init_db, insert_kline

def load_config(path: str = "config.yaml"):
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def fetch_trades(base_url: str, coin: str):
    """调用 Hyperliquid 的 recentTrades 接口获取最近成交"""
    payload = {
        "type": "recentTrades",
        "coin": coin,
    }
    resp = requests.post(base_url, json=payload, timeout=5)
    resp.raise_for_status()
    return resp.json()  # 返回的是一个 list

def build_1s_ohlcv(trades):
    """从 tick trades 聚合出 1 秒级 OHLCV"""
    df = pd.DataFrame(trades)
    if df.empty:
        return None

    # 把字符串 px/sz 转成 float
    df["px"] = df["px"].astype(float)
    df["sz"] = df["sz"].astype(float)
    df["time"] = pd.to_datetime(df["time"], unit="ms")

    ohlcv = df.resample("1s", on="time").agg({
        "px": ["first", "max", "min", "last"],
        "sz": "sum",
    })
    ohlcv.columns = ["open", "high", "low", "close", "volume"]
    return ohlcv

def main():
    config = load_config()
    coin = config.get("coin", "BTC")
    base_url = config.get("base_url", "https://api.hyperliquid.xyz/info")
    db_path = config.get("db_path", "./data/hl_1s_kline.db")
    interval = config.get("interval_pull_ms", 300) / 1000.0

    conn = init_db(db_path)

    print(f"🚀 HL 1 秒 K 线采集器启动, 合约: {coin}")
    print(f"📁 数据库文件: {db_path}")
    print("按 Ctrl + C 停止\n")

    last_ts = None

    while True:
        try:
            trades = fetch_trades(base_url, coin)
            ohlcv = build_1s_ohlcv(trades)

            if ohlcv is None or ohlcv.empty:
                time.sleep(interval)
                continue

            latest_ts = ohlcv.index[-1]

            if latest_ts != last_ts:
                last_ts = latest_ts
                row = ohlcv.iloc[-1]

                ts_str = latest_ts.strftime("%Y-%m-%d %H:%M:%S")

                # 写入数据库
                insert_kline(conn, ts_str, row.open, row.high, row.low, row.close, row.volume)

                # 打印一行，方便你观察
                print(
                    f"[{ts_str}] "
                    f"O={row.open:.2f} "
                    f"H={row.high:.2f} "
                    f"L={row.low:.2f} "
                    f"C={row.close:.2f} "
                    f"V={row.volume:.4f}"
                )

            time.sleep(interval)

        except KeyboardInterrupt:
            print("\n⛔ 手动停止采集。")
            break
        except Exception as e:
            print("❌ 错误:", e)
            time.sleep(1)

if __name__ == "__main__":
    main()
