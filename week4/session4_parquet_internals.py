import argparse
from pathlib import Path

import pyarrow.parquet as pq


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Inspect the structure of a Parquet file"
    )
    parser.add_argument(
        "--file",
        required=True,
        help="Path to one Parquet part file"
    )

    args = parser.parse_args()
    file_path = Path(args.file)

    with pq.ParquetFile(file_path) as parquet_file:
        print(f"File: {file_path.name}")
        print(f"Total rows: {parquet_file.metadata.num_rows:,}")
        print(f"Row groups: {parquet_file.metadata.num_row_groups}")

        for group_index in range(parquet_file.metadata.num_row_groups):
            row_group = parquet_file.metadata.row_group(group_index)

            print(f"\nRow group {group_index}:")
            print(f"Rows: {row_group.num_rows:,}")

            for column_index in range(row_group.num_columns):
                column = row_group.column(column_index)

                print(
                    f"Column: {column.path_in_schema}, "
                    f"Compression: {column.compression}"
                )
                if group_index == 0 and column.path_in_schema == "amount":
                    stats = column.statistics

                    if stats is not None and stats.has_min_max:
                        print(f"  Minimum amount: {stats.min}")
                        print(f"  Maximum amount: {stats.max}")
                    else:
                        print("  No minimum/maximum statistics available.")


if __name__ == "__main__":
    main()