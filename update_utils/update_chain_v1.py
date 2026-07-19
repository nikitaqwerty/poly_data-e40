"""
Polymarket CTF Exchange V1 OrderFilled event reader (HyperSync) — E40.H
fallback for the pre-V2 era when the Goldsky orderbook-subgraph is degraded.

Streams OrderFilled logs from the V1 CTF Exchange + Neg Risk CTF Exchange
contracts on Polygon and writes them to data/orderFilledV1.csv with the same
columns as the patched V2 reader (fee included):

    timestamp, maker, makerAssetId, makerAmountFilled,
    taker, takerAssetId, takerAmountFilled, transactionHash, fee

The V1 event carries makerAssetId/takerAssetId directly (no side enum):

    OrderFilled(bytes32 indexed orderHash, address indexed maker,
                address indexed taker, uint256 makerAssetId,
                uint256 takerAssetId, uint256 makerAmountFilled,
                uint256 takerAmountFilled, uint256 fee)

Cursor is persisted in data/cursor_state_v1.json. Start block defaults to
63,000,000 (~2024-10, before the first Elon weekly tweet ladder) — override
with V1_START_BLOCK. Stop block defaults to the archive safe height (the V1
exchanges go quiet after the 2026-04-28 migration, so the tail streams fast).
"""

import asyncio
import csv
import json
import os
import sys
from datetime import datetime, timezone

import hypersync
from dotenv import load_dotenv
from eth_abi import decode as abi_decode
from eth_utils import keccak
from hypersync import (
    BlockField,
    ClientConfig,
    FieldSelection,
    LogField,
    LogSelection,
    Query,
    StreamConfig,
)

load_dotenv()

CTF_EXCHANGE_V1 = "0x4bfb41d5b3570defd03c39a9a4d8de6bd8b8982e"
NEG_RISK_CTF_EXCHANGE_V1 = "0xc5d563a36ae78145c45a50134d48a1215220f80a"

DEFAULT_START_BLOCK = 63_000_000  # ~2024-10, pre first Elon weekly ladder

ORDERFILLED_V1_TOPIC = "0x" + keccak(
    text=(
        "OrderFilled(bytes32,address,address,uint256,uint256,"
        "uint256,uint256,uint256)"
    )
).hex()
_DATA_TYPES = ["uint256", "uint256", "uint256", "uint256", "uint256"]

OUTPUT_DIR = "data"
OUTPUT_FILE = os.path.join(OUTPUT_DIR, "orderFilledV1.csv")
CURSOR_FILE = os.path.join(OUTPUT_DIR, "cursor_state_v1.json")

CONFIRMATIONS = 20
DEFAULT_URL = "https://polygon.hypersync.xyz"

COLUMNS = [
    "timestamp",
    "maker",
    "makerAssetId",
    "makerAmountFilled",
    "taker",
    "takerAssetId",
    "takerAmountFilled",
    "transactionHash",
    "fee",
]


def _start_block() -> int:
    return int(os.environ.get("V1_START_BLOCK", DEFAULT_START_BLOCK))


def _load_cursor():
    if os.path.isfile(CURSOR_FILE):
        try:
            with open(CURSOR_FILE) as f:
                state = json.load(f)
            last = state.get("last_block")
            if isinstance(last, int) and last >= _start_block():
                cb = state.get("csv_bytes")
                return last, (cb if isinstance(cb, int) else None)
        except Exception:
            pass
    return _start_block(), None


def _save_cursor(next_block: int, csv_bytes: int) -> None:
    tmp = CURSOR_FILE + ".tmp"
    with open(tmp, "w") as f:
        json.dump({"last_block": next_block, "csv_bytes": csv_bytes}, f)
    os.replace(tmp, CURSOR_FILE)


def _now() -> str:
    return datetime.now().strftime("%H:%M:%S")


def _fmt_ts(unix_ts: int) -> str:
    return datetime.fromtimestamp(unix_ts, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


def _as_int(v):
    if isinstance(v, int):
        return v
    if isinstance(v, str):
        return int(v, 16) if v.startswith("0x") else int(v)
    raise TypeError(f"unexpected numeric value: {v!r}")


def _hex_to_bytes(s: str) -> bytes:
    return bytes.fromhex(s[2:] if s.startswith("0x") else s)


def _decode_log(log, ts_by_block: dict) -> list:
    topics = log.topics
    maker = "0x" + topics[2][-40:].lower()
    taker = "0x" + topics[3][-40:].lower()

    maker_aid, taker_aid, maker_amt, taker_amt, fee = abi_decode(
        _DATA_TYPES, _hex_to_bytes(log.data)
    )

    bn = _as_int(log.block_number)
    tx_hash = log.transaction_hash
    if not tx_hash.startswith("0x"):
        tx_hash = "0x" + tx_hash

    return [
        ts_by_block[bn],
        maker,
        str(maker_aid),
        str(maker_amt),
        taker,
        str(taker_aid),
        str(taker_amt),
        tx_hash,
        str(fee),
    ]


def _build_query(from_block: int, to_block: int) -> Query:
    return Query(
        from_block=from_block,
        to_block=to_block + 1,  # exclusive
        logs=[
            LogSelection(
                address=[CTF_EXCHANGE_V1, NEG_RISK_CTF_EXCHANGE_V1],
                topics=[[ORDERFILLED_V1_TOPIC]],
            )
        ],
        field_selection=FieldSelection(
            block=[BlockField.NUMBER, BlockField.TIMESTAMP],
            log=[
                LogField.BLOCK_NUMBER,
                LogField.TRANSACTION_HASH,
                LogField.TOPIC0,
                LogField.TOPIC1,
                LogField.TOPIC2,
                LogField.TOPIC3,
                LogField.DATA,
            ],
        ),
    )


async def _run() -> None:
    if not os.path.isdir(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    url = os.environ.get("POLYGON_HYPERSYNC_URL", DEFAULT_URL)
    token = os.environ.get("HYPERSYNC_API") or None
    if not token:
        raise RuntimeError("HYPERSYNC_API is not set.")
    client = hypersync.HypersyncClient(ClientConfig(url=url, bearer_token=token))

    print(f"[{_now()}] HyperSync V1 reader: {url} (with token)")

    height = await client.get_height()
    safe_height = height - CONFIRMATIONS
    stop_block = int(os.environ.get("V1_STOP_BLOCK", safe_height))
    stop_block = min(stop_block, safe_height)
    start_block, committed_bytes = _load_cursor()

    print(f"[{_now()}] Archive height: {height:,}  scanning {start_block:,} -> {stop_block:,}")

    if start_block > stop_block:
        print(f"[{_now()}] Already up to date.")
        return

    new_file = not os.path.isfile(OUTPUT_FILE)
    if new_file:
        with open(OUTPUT_FILE, "w", newline="") as f:
            csv.writer(f).writerow(COLUMNS)
    elif committed_bytes is not None:
        size = os.path.getsize(OUTPUT_FILE)
        if size > committed_bytes:
            os.truncate(OUTPUT_FILE, committed_bytes)
            print(f"[{_now()}] Discarded {size - committed_bytes:,} bytes from an interrupted batch")

    query = _build_query(start_block, stop_block)
    receiver = await client.stream(query, StreamConfig())

    total = 0
    first_ts = last_ts = None
    out = open(OUTPUT_FILE, "a", newline="")
    writer = csv.writer(out)
    try:
        while True:
            res = await receiver.recv()
            if res is None:
                break

            blocks = res.data.blocks or []
            logs = res.data.logs or []
            ts_by_block = {_as_int(b.number): _as_int(b.timestamp) for b in blocks}

            if logs:
                writer.writerows([_decode_log(log, ts_by_block) for log in logs])
                out.flush()
                total += len(logs)

            committed_bytes = os.fstat(out.fileno()).st_size
            _save_cursor(res.next_block, committed_bytes)

            if ts_by_block:
                if first_ts is None:
                    first_ts = min(ts_by_block.values())
                last_ts = max(ts_by_block.values())
                reached = _fmt_ts(last_ts)
            else:
                reached = "       —             "
            print(
                f"[{_now()}]   reached {reached} UTC  block {res.next_block - 1:>10,}  "
                f"events: {len(logs):>5}  total: {total:,}"
            )
    finally:
        out.close()

    if first_ts is not None:
        print(
            f"[{_now()}] Done. Wrote {total:,} new rows spanning "
            f"{_fmt_ts(first_ts)} → {_fmt_ts(last_ts)} UTC."
        )
    else:
        print(f"[{_now()}] Done. Wrote {total:,} new rows to {OUTPUT_FILE}.")


def update_chain_v1() -> None:
    if hasattr(sys.stdout, "reconfigure"):
        sys.stdout.reconfigure(line_buffering=True)
    asyncio.run(_run())


if __name__ == "__main__":
    update_chain_v1()
