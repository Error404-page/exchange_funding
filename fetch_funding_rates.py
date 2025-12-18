"""
fetch_funding_rates.py

从 `symbols/` 目录下读取两个文件（`bn_200_usdt.txt` 和 `bn_200_usdc.txt`），汇总交易对列表。
对每个交易对：
- 使用 Binance Futures API 获取最近一年的资金费率历史（逐页拉取，保存到 `output/binance_funding.csv`）。
- 尝试从 Bybit 拉取可用的资金费率历史（若能解析），保存到 `output/bybit_funding.csv`。

输出 CSV 字段（通用）:
  exchange,symbol,funding_time_ms,funding_time_iso,funding_rate

注意：
- Binance 的历史资金费率接口为 `https://fapi.binance.com/fapi/v1/fundingRate`（USD-M perpetual）。
- Bybit 的历史接口在不同 API 版本中差异较大，本脚本会尝试多个常见端点并用宽松的解析规则提取 `funding_rate` / `fundingRate` 字段。
  若 Bybit 数据解析失败，会在运行日志中给出原因。

使用：
  python fetch_funding_rates.py --days 365 --output-dir ./output

依赖：`requests`（标准库 csv 用于写 CSV）。
"""
import argparse
import csv
import datetime
import os
import time
from typing import List, Dict, Any

import requests
from collections import defaultdict


BINANCE_FUNDING_ENDPOINT = "https://fapi.binance.com/fapi/v1/fundingRate"


def read_symbols_from_folder(folder: str) -> List[str]:
    # 保留向后兼容：若未指定文件集合，返回所有三个文件的并集
    files = ["binance_bybit_200_usdt.txt", "binance_200_usdc.txt", "bybit_200_usdc.txt"]
    syms = []
    for fn in files:
        path = os.path.join(folder, fn)
        if not os.path.isfile(path):
            continue
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s:
                    continue
                # 某些文件行可能带有反引号或 markdown 块，清理
                s = s.strip('`\n\r ')
                syms.append(s)
    # 去重并返回
    return sorted(list(dict.fromkeys(syms)))


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


def read_symbols_for_bybit(folder: str) -> List[str]:
    files = ["binance_bybit_200_usdt.txt", "bybit_200_usdc.txt"]
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
                # item 包含: symbol, fundingTime (ms), fundingRate
                ft = int(item.get("fundingTime"))
                fr = item.get("fundingRate")
                results.append({
                    "exchange": "binance",
                    "symbol": symbol,
                    "funding_time_ms": ft,
                    "funding_time_iso": iso_from_ms(ft),
                    "funding_rate": fr,
                })

            # Binance 返回按时间升序，若条数 < limit 则完成
            if len(data) < limit:
                break
            # 否则推进 cur_start 到最后一条的时间 +1ms，继续拉取
            last_time = int(data[-1].get("fundingTime"))
            cur_start = last_time + 1
            # 防止被限速略作等待
            time.sleep(0.2)
        except Exception as e:
            print(f"Binance 请求失败 for {symbol}: {e}")
            break
    return results


def try_extract_list_from_json(obj: Any) -> List[dict]:
    """宽松提取 JSON 中可能包含的条目列表，查找任何包含资金费率键的 dict 列表。"""
    candidates = []

    def walk(x):
        if isinstance(x, list):
            # 若列表项为 dict 并含有 fundingRate/funding_rate 等键，视为目标
            if len(x) > 0 and isinstance(x[0], dict):
                keys = set().union(*(set(i.keys()) for i in x if isinstance(i, dict)))
                if any(k.lower().replace('-', '_') in {"fundingrate", "funding_rate"} for k in keys):
                    candidates.append(x)
                    return
            for item in x:
                walk(item)
        elif isinstance(x, dict):
            for v in x.values():
                walk(v)

    walk(obj)
    # 返回第一个匹配到的列表，或空
    return candidates[0] if candidates else []


def fetch_bybit_funding_v5(symbol: str, start_ts: int, end_ts: int) -> List[Dict[str, Any]]:
    """仅使用 Bybit v5 端点 (`/v5/market/funding/history`) 拉取资金费率历史并解析。
    start_ts/end_ts 以毫秒计。
    """
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

            # Bybit v5 通常返回结构 {retCode, retMsg, result: {list: [...], category: "..."}}
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
                # 常见字段名
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

            # Binance 返回按时间升序，若条数 < limit 则完成
            if len(lst) < limit:
                break
            # 否则推进 cur_end 到最后一条的时间 -1ms，继续拉取
            last_time = int(lst[-1].get("fundingRateTimestamp"))
            cur_end = last_time - 1
            # 防止被限速略作等待

            # 轻微等待以避免限速
            time.sleep(0.2)
        except Exception as e:
            print(f"Bybit v5 请求失败 for {symbol}: {e}")
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
    # 保留字母数字、-、_，其它字符替换为下划线，避免文件名问题
    return ''.join(c if (c.isalnum() or c in ('-', '_')) else '_' for c in name)


def main():
    p = argparse.ArgumentParser(description="Fetch funding rates from Binance and Bybit for symbols listed in ./symbols/")
    p.add_argument("--symbols-dir", default="./symbols", help="symbols 目录，包含 bn_200_usdt.txt 与 bn_200_usdc.txt")
    p.add_argument("--days", type=int, default=365, help="向后查询天数，默认 365 天")
    p.add_argument("--output-dir", default="./output", help="输出目录，默认 ./output")
    p.add_argument("--only-binance", action="store_true", help="只获取 Binance 的资金费率")
    args = p.parse_args()

    syms_binance = read_symbols_for_binance(args.symbols_dir)
    syms_bybit = read_symbols_for_bybit(args.symbols_dir)

    if not syms_binance and not syms_bybit:
        print("未找到 symbols 文件或文件为空，请检查 ./symbols 下的 txt 文件")
        return

    print(f"读取到 Binance {len(syms_binance)} 个交易对，示例前10：{syms_binance[:10]}")
    print(f"读取到 Bybit {len(syms_bybit)} 个交易对，示例前10：{syms_bybit[:10]}")

    end_ms = ms_now()
    # start_ms = end_ms - args.days * 24 * 3600 * 1000
    timeArray = time.strptime("2024-12-16 00:00:00", "%Y-%m-%d %H:%M:%S")
    start_ms = int(time.mktime(timeArray) * 1000)

    binance_rows = []
    bybit_rows = []

    # 先为 Binance 拉取（从 binance_bybit_200_usdt.txt 与 binance_200_usdc.txt）
    for symbol in syms_binance:
        print(f"拉取 Binance 资金费率: {symbol} ...")
        b = fetch_binance_funding(symbol, start_ms, end_ms)
        if b:
            binance_rows.extend(b)

    # 再为 Bybit 拉取（从 binance_bybit_200_usdt.txt 与 bybit_200_usdc.txt）
    if not args.only_binance:
        for symbol in syms_bybit:
            print(f"拉取 Bybit 资金费率: {symbol} ...")
            bb = fetch_bybit_funding_v5(symbol, start_ms, end_ms)
            if bb:
                bybit_rows.extend(bb[::-1])  # Bybit v5 返回按时间降序，反转为升序
            else:
                print(f"未找到 Bybit 历史资金数据或解析失败: {symbol}")

    os.makedirs(args.output_dir, exist_ok=True)

    # 按 exchange + symbol 分组，分别保存为单独文件，命名格式: <exchange>_funding_<symbol>.csv
    all_rows = list(binance_rows)
    if not args.only_binance:
        all_rows.extend(bybit_rows)

    grouped = defaultdict(list)
    for r in all_rows:
        exch = r.get("exchange")
        sym = r.get("symbol")
        if exch is None or sym is None:
            continue
        grouped[(exch, sym)].append(r)

    for (exch, sym), rows in grouped.items():
        fname = f"{exch}_funding_{sanitize_filename(sym)}.csv"
        save_rows_to_csv(rows, os.path.join(args.output_dir, fname))


if __name__ == "__main__":
    main()
