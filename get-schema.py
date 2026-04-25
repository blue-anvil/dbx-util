import argparse
import json
import os
import re
import subprocess
import sys
import tempfile

from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField


def parse_size(s):
    m = re.fullmatch(r'(\d+)([kmg]?)', s.lower())
    if not m:
        raise argparse.ArgumentTypeError(f"invalid size: {s}")
    n, u = int(m.group(1)), m.group(2)
    return n * {'k': 1024, 'm': 1024**2, 'g': 1024**3}.get(u, 1)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-f', '--file', required=True)
    parser.add_argument('-t', '--type', choices=['json', 'csv', 'tsv', 'parquet'])
    parser.add_argument('-d', '--delimiter', default=None)
    parser.add_argument('-o', '--output', choices=['stdout'], default=None)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-s', '--size', type=parse_size)
    group.add_argument('-p', '--percent', type=float)
    args = parser.parse_args()

    fmt = args.type
    if fmt is None:
        ext = os.path.splitext(args.file)[1].lower().lstrip('.')
        if ext not in ('json', 'csv', 'tsv', 'parquet'):
            sys.exit(f"error: unrecognized file extension '.{ext}'")
        fmt = ext

    spark = (SparkSession.builder
             .master("local[1]")
             .config("spark.ui.enabled", "false")
             .config("spark.ui.showConsoleProgress", "false")
             .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    tmp = None
    try:
        if fmt == 'parquet':
            df = spark.read.parquet(args.file)
        else:
            file_size = os.path.getsize(args.file)
            n_bytes = int(file_size * args.percent / 100) if args.percent is not None else args.size

            with open(args.file, 'rb') as f:
                data = f.read(n_bytes)

            last_newline = data.rfind(b'\n')
            if last_newline != -1:
                data = data[:last_newline + 1]

            with tempfile.NamedTemporaryFile(delete=False, suffix='.csv') as f:
                tmp = f.name
                f.write(data)

            if fmt == 'json':
                df = spark.read.json(tmp)
            else:
                sep = args.delimiter if args.delimiter is not None else ('\t' if fmt == 'tsv' else ',')
                df = (spark.read
                      .option("header", "true")
                      .option("inferSchema", "true")
                      .option("sep", sep)
                      .csv(tmp))

        schema = df.schema
        schema.add(StructField("_rescued_data", StringType(), True))
        pretty = json.dumps(json.loads(schema.json()), indent=4)
        result = '\n'.join('    ' + line for line in pretty.splitlines())

        if args.output == 'stdout':
            print(result)
        else:
            subprocess.run(['clip'], input=result.encode(), check=True)

    finally:
        if tmp and os.path.exists(tmp):
            os.unlink(tmp)
        spark.stop()


if __name__ == '__main__':
    main()
