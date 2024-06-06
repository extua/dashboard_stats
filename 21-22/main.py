import polars as pl

df = (
    pl.scan_csv(
        "weekly_unique_users_21-22.csv", has_header=False, encoding="utf8", n_rows=42
    )
    .select(
        [
            pl.col("column_1")
            .str.strptime(pl.Date, "%m/%d/%Y")
            .set_sorted()
            .alias("date"),
            pl.col("column_2").cast(pl.UInt32).alias("views"),
        ]
    )
    .group_by_dynamic("date", every="1mo", period="1mo", check_sorted=False)
    .agg(pl.col("views").mean().cast(pl.UInt32).alias("avg_daily_views"))
    # .with_columns([pl.col("date").dt.to_string("%Y-%m")])
    .collect()
)

df.write_csv("weekly_unique_users_21-22_python_agg.csv", separator=",", date_format="%Y-%m")