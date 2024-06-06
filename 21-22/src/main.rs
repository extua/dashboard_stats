// use chrono::Weekday;
use polars::{prelude::*, series::IsSorted};

fn main() {
    // Read the csv into a lazy frame
    let csv_in: LazyFrame = LazyCsvReader::new("weekly_unique_users_21-22.csv")
        .has_header(false)
        .with_encoding(CsvEncoding::Utf8)
        .with_n_rows(Some(42))
        .finish()
        .unwrap();

    let time_options = StrptimeOptions {
        // Parse dates in US format
        format: Some("%m/%d/%Y".into()),
        ..Default::default()
    };

    let mut df: DataFrame = csv_in
        .with_column(
            col("column_1")
                .str()
                .to_date(time_options)
                // Rows are already sorted so set sorted flag
                .set_sorted_flag(IsSorted::Ascending)
                .alias("date"),
        )
        .group_by_dynamic(
            col("date"),
            [],
            DynamicGroupOptions {
                every: Duration::parse("1mo"),
                period: Duration::parse("1mo"),
                // No offset
                offset: Duration::parse("0"),
                // Start by beginning of the window, first day of month
                start_by: StartBy::WindowBound,
                // Don't need to check whether lazyframe is sorted already
                check_sorted: false,
                // wrap up default arguments
                ..Default::default()
            },
        )
        .agg([col("column_2")
            .mean()
            .cast(DataType::UInt32)
            .alias("avg_daily_views")])
        // Convert date column back to string and truncate to YYYY-MM
        .with_column(col("date").cast(DataType::String).str().slice(0, Some(7)))
        .collect()
        .unwrap();

    print!("{}", df);

    // Write out to another csv
    let mut file = std::fs::File::create("weekly_unique_users_21-22_averaged.csv").unwrap();
    CsvWriter::new(&mut file).finish(&mut df).unwrap();

    println!("\nwritten to file");
}
