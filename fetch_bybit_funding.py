"""
fetch_bybit_funding.py

从 `symbols/` 下的 `binance_bybit_200_usdt.txt` 与 `bybit_200_usdc.txt` 读取交易对，
拉取 Bybit 的资金费率历史并按 `bybit_funding_<symbol>.csv` 保存到输出目录。

用法:
  python fetch_bybit_funding.py --output-dir ./output --symbols-dir ./symbols

依赖: `requests`
"""
import argparse
import csv
import datetime
import os
import time
from typing import List, Dict, Any

import requests


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


def read_symbols_for_bybit(folder: str) -> List[str]:
    files = ["binance_bybit_200_usdt.txt", "bybit_200_usdc.txt"]
    return read_symbols_from_files(folder, files)


def iso_from_ms(ms: int) -> str:
    return datetime.datetime.utcfromtimestamp(ms / 1000.0).isoformat() + "Z"


def fetch_bybit_funding_v5(symbol: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
    ep = "https://api.bybit.com/v5/market/funding/history"
    results: List[Dict[str, Any]] = []
    limit = 200
    cur_end = end_ts

    while True:
        params = {"category": "linear", "symbol": symbol, "startTime": start_ts, "endTime": cur_end, "limit": limit}

        try:
            r = requests.get(ep, params=params, timeout=20)
            r.raise_for_status()
            j = r.json()

            result_obj = j.get("result") if isinstance(j, dict) else None

            if isinstance(result_obj, dict):
                lst = result_obj.get("list") or []
            elif isinstance(result_obj, list):
                lst = result_obj
            else:
                lst = []

            if not lst:
                break

            for item in lst:
                fr = item.get("fundingRate") or item.get("funding_rate") or item.get("funding")
                ft = item.get("fundingRateTimestamp") or item.get("funding_time") or item.get("timestamp") or item.get("time")
                if ft is None:
                    continue
                try:
                    ft_i = int(ft)
                    if ft_i < 1e12:
                        ft_i = ft_i * 1000
                except Exception:
                    try:
                        dt = datetime.datetime.fromisoformat(str(ft).replace("Z", "+00:00"))
                        ft_i = int(dt.timestamp() * 1000)
                    except Exception:
                        continue

                results.append({
                    "exchange": "bybit",
                    "symbol": symbol,
                    "funding_time_ms": ft_i,
                    "funding_time_iso": iso_from_ms(ft_i),
                    "funding_rate": fr,
                })

            if len(lst) < limit:
                break
            last_time = int(lst[-1].get("fundingRateTimestamp"))
            cur_end = last_time - 1
            time.sleep(0.2)
        except Exception as e:
            print(f"Bybit 请求失败 for {symbol}: {e}")
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
    p = argparse.ArgumentParser(description="Fetch Bybit funding rates")
    p.add_argument("--symbols-dir", default="./symbols")
    p.add_argument("--days", type=int, default=365)
    p.add_argument("--output-dir", default="./output")
    args = p.parse_args()

    syms = read_symbols_for_bybit(args.symbols_dir)
    if not syms:
        print("未找到 Bybit 交易对，请检查 symbols 文件")
        return

    print(f"读取到 Bybit {len(syms)} 个交易对，示例前10：{syms[:10]}")

    # 使用固定时间范围以保持与原脚本行为一致
    timeArray = time.strptime("2024-12-16 00:00:00", "%Y-%m-%d %H:%M:%S")
    start_ms = int(time.mktime(timeArray) * 1000)
    end_ms = int(time.time() * 1000)

    all_rows = []
    for symbol in syms:
        print(f"拉取 Bybit 资金费率: {symbol} ...")
        rows = fetch_bybit_funding_v5(symbol, start_ms, end_ms)
        if rows:
            all_rows.extend(rows[::-1])  # 保持升序

    os.makedirs(args.output_dir, exist_ok=True)

    by_sym = {}
    for r in all_rows:
        sym = r.get('symbol')
        by_sym.setdefault(sym, []).append(r)

    for sym, rows in by_sym.items():
        fname = f"bybit_funding_{sanitize_filename(sym)}.csv"
        save_rows_to_csv(rows, os.path.join(args.output_dir, fname))


if __name__ == '__main__':
    main()
