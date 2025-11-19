# file: wordcount_df.py

import argparse
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, explode, split, lower

def parse_args():
    parser = argparse.ArgumentParser(
        description="Semplice word count con Spark 4 (DataFrame API)."
    )
    parser.add_argument(
        "--input",
        required=True,
        help="Percorso del file di testo in input (locale o HDFS)."
    )
    parser.add_argument(
        "--output",
        required=False,
        help="Cartella di output per salvare il risultato in CSV (opzionale)."
    )
    parser.add_argument(
        "--topN",
        type=int,
        default=0,
        help="Numero massimo di parole da mostrare/salvare (0 = tutte)."
    )
    return parser.parse_args()

def main():
    args = parse_args()

    spark = (
        SparkSession.builder
        .appName("WordCount-Spark4")
        .getOrCreate()
    )

    # 1) Lettura file di testo (una riga per record)
    lines_df = spark.read.text(args.input)

    # 2) Split in parole, normalizzando a minuscolo
    words_df = (
        lines_df
        .select(explode(split(col("value"), r"\W+")).alias("word"))
        .where(col("word") != "")
        .select(lower(col("word")).alias("word"))
    )

    # 3) Conteggio
    counts_df = (
        words_df
        .groupBy("word")
        .count()
        .orderBy(col("count").desc(), col("word").asc())
    )

    # Eventuale limit
    if args.topN and args.topN > 0:
        counts_df = counts_df.limit(args.topN)

    # 4) Output: o su console, o su CSV
    if args.output:
        (
            counts_df
            .coalesce(1)                 # un solo file di output (comodo per test)
            .write
            .mode("overwrite")
            .option("header", True)
            .csv(args.output)
        )
        print(f"Risultato salvato in cartella: {args.output}")
    else:
        counts_df.show(n=args.topN if args.topN > 0 else 20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()



