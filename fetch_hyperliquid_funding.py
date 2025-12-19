"""
fetch_hyperliquid_funding.py

从 `symbols/` 下的 `hyperliquid_200_usdc.txt` 读取交易对，
尝试从 Hyperliquid 的常见/自定义 HTTP 接口获取这些交易对的资金费率历史，
并把每个交易对的结果保存为 `hyper_funding/hyper_funding_<symbol>.csv`。

说明：Hyperliquid 的公开 API 端点和字段命名可能与此脚本中的猜测不同。
脚本会尝试一组常见的 URL 模板并用宽松的解析规则提取 `funding_rate` 字段。
如果默认端点无法返回，需要手动调整 `HYPER_API_ENDPOINTS`。

用法:
  python fetch_hyperliquid_funding.py --symbols-dir ./symbols --output-dir ./output/hyper_funding

依赖: `requests`
"""
import argparse
import csv
import datetime
import os
import time
from typing import List, Dict, Any

import requests

HYPER_API_ENDPOINT = "https://api.hyperliquid.xyz/info"


def read_symbols_for_hyper(folder: str) -> List[str]:
    fn = "hyperliquid_200_usdc.txt"
    path = os.path.join(folder, fn)
    syms = []
    if not os.path.isfile(path):
        return syms
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            s = s.strip('`\n\r ')
            syms.append(s)
    return sorted(list(dict.fromkeys(syms)))


def iso_from_ms(ms: int) -> str:
    # 直接使用 datetime.UTC
    dt = datetime.datetime.fromtimestamp(ms / 1000.0, datetime.UTC)
    return dt.isoformat().replace("+00:00", "Z")


import re


def extract_coin_from_symbol(symbol: str) -> str:
    """从 symbol 中提取 coin 名称，取开头连续字母序列，例如 ETHUSDC -> ETH"""
    m = re.match(r"^([A-Za-z]+)", symbol)
    return m.group(1) if m else symbol


def parse_item_to_row(item: Dict[str, Any], symbol: str) -> Dict[str, Any] | None:
    # 尝试多种可能字段名
    fr = item.get("fundingRate") if isinstance(item, dict) else None
    if fr is None:
        fr = item.get("funding_rate") or item.get("rate") or item.get("funding")

    # 时间字段
    ft = None
    for k in ("fundingTime", "funding_time", "fundingRateTimestamp", "timestamp", "time", "t"):
        if isinstance(item, dict) and k in item:
            ft = item.get(k)
            break

    if ft is None:
        return None

    try:
        ft_i = int(ft)
        if ft_i < 1e12:
            ft_i = ft_i * 1000
    except Exception:
        try:
            dt = datetime.datetime.fromisoformat(str(ft).replace("Z", "+00:00"))
            ft_i = int(dt.timestamp() * 1000)
        except Exception:
            return None

    return {
        "exchange": "hyperliquid",
        "symbol": symbol,
        "funding_time_ms": ft_i,
        "funding_time_iso": iso_from_ms(ft_i),
        "funding_rate": fr,
    }


def fetch_hyper_funding_for_symbol(symbol: str, start_ms: int, end_ms: int) -> List[Dict[str, Any]]:
    """使用 Hyperliquid 官方 POST /info 接口获取历史资金费率。
    请求体示例: {"type":"fundingHistory","coin":"ETH","startTime":...,"endTime":...}
    返回为列表，每项包含: coin, fundingRate, premium, time
    """
    results: List[Dict[str, Any]] = []
    coin = extract_coin_from_symbol(symbol)
    headers = {"Content-Type": "application/json"}
    url = HYPER_API_ENDPOINT

    # 为兼容大量历史数据，分段拉取（以 30 天为窗口）
    window_ms = 30 * 24 * 3600 * 1000
    cur_start = start_ms
    while cur_start <= end_ms:
        cur_end = min(end_ms, cur_start + window_ms - 1)
        body = {"type": "fundingHistory", "coin": coin, "startTime": cur_start, "endTime": cur_end}
        try:
            r = requests.post(url, json=body, headers=headers, timeout=30)
            r.raise_for_status()
            j = r.json()
            print(f"请求 {coin} 返回: {len(j)} 条 数据")
            if not isinstance(j, list) or len(j) == 0:
                # 未返回数据，推进到下一个窗口
                cur_start = cur_end + 1
                time.sleep(0.5)
                continue

            for item in j:
                # item 结构示例: {"coin":"ETH","fundingRate":"-0.00022196","premium":"...","time":1683849600076}
                ft = item.get("time") or item.get("timestamp") or item.get("t")
                fr = item.get("fundingRate") or item.get("funding_rate") or item.get("rate") or item.get("funding")
                if ft is None:
                    continue
                try:
                    ft_i = int(ft)
                    if ft_i < 1e12:
                        ft_i = ft_i * 1000
                except Exception:
                    continue

                results.append({
                    "exchange": "hyperliquid",
                    "symbol": symbol,
                    "funding_time_ms": ft_i,
                    "funding_time_iso": iso_from_ms(ft_i),
                    "funding_rate": fr,
                })

            # 继续下一窗口
            last_time = int(j[-1].get("time") or j[-1].get("timestamp") or j[-1].get("t"))
            cur_start = last_time + 1
            time.sleep(0.5)
        except Exception as e:
            print(f"Hyperliquid 请求失败({url}) for {symbol}: {e}")
            break

    # 去重并按时间升序返回
    results = sorted({(r['funding_time_ms'], r['funding_rate']): r for r in results}.values(), key=lambda x: x["funding_time_ms"])  # type: ignore
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
    p = argparse.ArgumentParser(description="Fetch Hyperliquid funding rates")
    p.add_argument("--symbols-dir", default="./symbols")
    p.add_argument("--output-dir", default="./output/hyper_funding")
    p.add_argument("--days", type=int, default=365)
    args = p.parse_args()

    syms = read_symbols_for_hyper(args.symbols_dir)
    if not syms:
        print("未找到 hyperliquid_200_usdc.txt 或文件为空，请检查 ./symbols 下的 txt 文件")
        return

    print(f"读取到 Hyperliquid {len(syms)} 个交易对，示例前10：{syms[:10]}")

    end_ms = int(time.time() * 1000)
    # start_ms = end_ms - args.days * 24 * 3600 * 1000
    timeArray = time.strptime("2024-12-16 00:00:00", "%Y-%m-%d %H:%M:%S")
    start_ms = int(time.mktime(timeArray) * 1000)

    all_rows = []
    for symbol in syms:
        print(f"拉取 Hyperliquid 资金费率: {symbol} ...")
        rows = fetch_hyper_funding_for_symbol(symbol, start_ms, end_ms)
        if rows:
            all_rows.extend(rows)
        else:
            print(f"未能从预设 endpoint 提取到 Hyperliquid 数据: {symbol}")

    os.makedirs(args.output_dir, exist_ok=True)

    # 按 symbol 分文件输出到 hyper_funding 目录
    by_sym = {}
    for r in all_rows:
        sym = r.get('symbol')
        by_sym.setdefault(sym, []).append(r)

    for sym, rows in by_sym.items():
        fname = f"hyper_funding_{sanitize_filename(sym)}.csv"
        save_rows_to_csv(rows, os.path.join(args.output_dir, fname))


if __name__ == '__main__':
    main()
