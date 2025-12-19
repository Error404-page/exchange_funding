"""
fetch_binance_funding.py

从 `symbols/` 下的 `binance_bybit_200_usdt.txt` 与 `binance_200_usdc.txt` 读取交易对，
拉取 Binance Futures 的资金费率历史并按 `binance_funding_<symbol>.csv` 保存到输出目录。

用法:
  python fetch_binance_funding.py --output-dir ./output --symbols-dir ./symbols

依赖: `requests`
"""
import argparse
import csv
import datetime
import os
import time
from typing import List, Dict, Any

import requests

BINANCE_FUNDING_ENDPOINT = "https://fapi.binance.com/fapi/v1/fundingRate"


def read_symbols_from_files(folder: str, filenames: List[str]) -> List[str]:
    syms = []
    for fn in filenames:
        path = os.path.join(folder, fn)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                s = s.strip('`\n\r ')
                syms.append(s)
    return sorted(list(dict.fromkeys(syms)))


def read_symbols_for_binance(folder: str) -> List[str]:
    files = ["binance_bybit_200_usdt.txt", "binance_200_usdc.txt"]
    return read_symbols_from_files(folder, files)


def ms_now() -> int:
    return int(time.time() * 1000)


def iso_from_ms(ms: int) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000.0).isoformat() + "Z"


def fetch_binance_funding(symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    results = []
    limit = 1000
    cur_start = start_ms
    while True:
        params = {
            "symbol": symbol,
            "startTime": cur_start,
            "endTime": end_ms,
            "limit": limit,
        }
        try:
            r = requests.get(BINANCE_FUNDING_ENDPOINT, params=params, timeout=20)
            r.raise_for_status()
            data = r.json()
            if not isinstance(data, list) or len(data) == 0:
                break
            for item in data:
                ft = int(item.get("fundingTime"))
                fr = item.get("fundingRate")
                results.append({
                    "exchange": "binance",
                    "symbol": symbol,
                    "funding_time_ms": ft,
                    "funding_time_iso": iso_from_ms(ft),
                    "funding_rate": fr,
                })

            if len(data) < limit:
                break
            last_time = int(data[-1].get("fundingTime"))
            cur_start = last_time + 1
            time.sleep(0.2)
        except Exception as e:
            print(f"Binance 请求失败 for {symbol}: {e}")
            break
    return results


def save_rows_to_csv(rows: List[Dict[str, Any]], path: str):
    os.makedirs(os.path.dirname(path), exist_ok=True)
    keys = ["exchange", "symbol", "funding_time_ms", "funding_time_iso", "funding_rate"]
    with open(path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        for r in rows:
            w.writerow({k: r.get(k, "") for k in keys})
    print(f"已保存 {len(rows)} 行到 {path}")


def sanitize_filename(name: str) -> str:
    return ''.join(c if (c.isalnum() or c in ('-', '_')) else '_' for c in name)


def main():
    p = argparse.ArgumentParser(description="Fetch Binance funding rates")
    p.add_argument("--symbols-dir", default="./symbols")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--output-dir", default="./output")
    args = p.parse_args()

    syms = read_symbols_for_binance(args.symbols_dir)
    if not syms:
        print("未找到 Binance 交易对，请检查 symbols 文件")
        return

    print(f"读取到 Binance {len(syms)} 个交易对，示例前10：{syms[:10]}")

    end_ms = ms_now()
    timeArray = time.strptime("2024-12-16 00:00:00", "%Y-%m-%d %H:%M:%S")
    start_ms = int(time.mktime(timeArray) * 1000)

    all_rows = []
    for symbol in syms:
        print(f"拉取 Binance 资金费率: {symbol} ...")
        rows = fetch_binance_funding(symbol, start_ms, end_ms)
        if rows:
            all_rows.extend(rows)

    os.makedirs(args.output_dir, exist_ok=True)
    # 按 symbol 分文件
    by_sym = {}
    for r in all_rows:
        sym = r.get('symbol')
        by_sym.setdefault(sym, []).append(r)

    for sym, rows in by_sym.items():
        fname = f"binance_funding_{sanitize_filename(sym)}.csv"
        save_rows_to_csv(rows, os.path.join(args.output_dir, fname))


if __name__ == '__main__':
    main()
