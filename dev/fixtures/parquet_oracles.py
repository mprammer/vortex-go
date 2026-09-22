#!/usr/bin/env python3
# SPDX-License-Identifier: Apache-2.0
"""Write independent Parquet oracles from source formulas, without reading Vortex."""
import argparse
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def write_oracles(output: Path) -> None:
    output.mkdir(parents=True, exist_ok=True)
    rows = range(6149)
    labels = ["", "cedar", "fjord", "東京駅", "naïve", "🪁", "a\x00z",
              "independent strings exceed twelve bytes"]
    schema = pa.schema([
        pa.field("sequence", pa.int64(), nullable=False),
        pa.field("small", pa.int32(), nullable=False),
        pa.field("wide", pa.int64(), nullable=False),
        pa.field("measure", pa.float64(), nullable=False),
        pa.field("enabled", pa.bool_(), nullable=False),
        pa.field("label", pa.string(), nullable=False),
        pa.field("note", pa.string(), nullable=False),
    ])
    columns = [
        [r * 29 - 80000 for r in rows],
        [(r * 37) % 10007 - 5003 for r in rows],
        [9007199254740993 + r * 101 for r in rows],
        [(r % 1801 - 900) / 8 for r in rows],
        [r % 7 < 3 for r in rows],
        [labels[r % 8] for r in rows],
        [f"reading/{r:05}/sector-{r % 23}" for r in rows],
    ]
    scalars = pa.Table.from_arrays(
        [pa.array(values, type=field.type) for field, values in zip(schema, columns)],
        schema=schema,
    )
    pq.write_table(scalars, output / "owned_scalars.parquet", compression="zstd")

    rows = range(2057)
    nullable = pa.table({
        "maybe_int": pa.array([
            None if r % 13 == 4 or 1000 <= r < 1017 else r * 19 - 7000
            for r in rows
        ], type=pa.int32()),
        "maybe_str": pa.array([
            None if r % 17 == 9 else f"item-{r:04}-δ" for r in rows
        ], type=pa.string()),
    })
    pq.write_table(nullable, output / "owned_nullable.parquet", compression="zstd")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    write_oracles(parser.parse_args().output)
