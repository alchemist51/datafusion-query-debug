mod context;
mod discovery;
mod output;
mod stats;
mod table;

use anyhow::Result;
use clap::Parser;
use log::info;
use std::time::Instant;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Path to the data directory containing shards
    #[arg(short, long, default_value = "/Users/abandeji/Public/work-dump/clickbench_data/datafusion_3shards/data")]
    data_path: String,

    /// SQL query to execute
    #[arg(short, long, default_value = "SELECT COUNT(*) FROM hits")]
    query: String,

    /// Number of target partitions (should match number of shards)
    #[arg(short, long)]
    target_partitions: Option<usize>,

    /// Output format: json, csv, or table
    #[arg(short, long, default_value = "table")]
    output_format: String,

    /// Enable verbose logging
    #[arg(short, long)]
    verbose: bool,

    /// Number of times to run the query
    #[arg(short, long, default_value = "1")]
    num_runs: usize,

    /// Use flat directory structure (parquet files directly in folder)
    #[arg(short = 'f', long)]
    flat: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();

    init_logging(args.verbose);

    let start_time = Instant::now();
    
    info!("Starting DataFusion parquet query utility");
    info!("Data path: {}", args.data_path);
    info!("Query: {}", args.query);

    let shard_dirs = if args.flat {
        vec![args.data_path.clone()]
    } else {
        discovery::discover_shard_directories(&args.data_path).await?
    };
    info!("Found {} shard directories", shard_dirs.len());

    let target_partitions = args.target_partitions.unwrap_or_else(|| {
        let shard_count = shard_dirs.len();
        info!("Setting target_partitions to {}", shard_count);
        shard_count
    });

    let ctx = context::create_datafusion_context(target_partitions).await?;
    table::register_parquet_table_listing(&ctx, &shard_dirs, "hits").await?;

    for run in 1..=args.num_runs {
        println!("\n--- Run {}/{} ---", run, args.num_runs);
        
        let query_start = Instant::now();
        let df = ctx.sql(&args.query).await?;
        let results = df.collect().await?;
        let query_duration = query_start.elapsed();
        
        match args.output_format.as_str() {
            "json" => output::output_json(&results)?,
            "csv" => output::output_csv(&results)?,
            _ => output::output_table(&results)?,
        }
        
        println!("\nQuery execution time: {:?}", query_duration);
        println!("Rows returned: {}", results.iter().map(|batch| batch.num_rows()).sum::<usize>());
        println!("Target partitions: {}", target_partitions);
        stats::print_memory_stats();
    }

    let total_duration = start_time.elapsed();
    println!("\n--- Total Summary ---");
    println!("Total time: {:?}", total_duration);
    println!("Total runs: {}", args.num_runs);

    Ok(())
}

fn init_logging(verbose: bool) {
    if verbose {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Info)
            .init();
    } else {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Warn)
            .init();
    }
}
