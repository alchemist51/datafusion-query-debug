use anyhow::{Context, Result};
use clap::Parser;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::cache::cache_unit::DefaultFileStatisticsCache;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use log::{error, info};
use std::path::Path;
use std::sync::Arc;
use std::time::Instant;
use tokio::fs;

fn print_memory_stats() {
    #[cfg(all(feature = "mimalloc", feature = "mimalloc_extended"))]
    {
        use datafusion::execution::memory_pool::human_readable_size;
        let mut peak_rss = 0;
        let mut peak_commit = 0;
        let mut page_faults = 0;
        unsafe {
            libmimalloc_sys::mi_process_info(
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                std::ptr::null_mut(),
                &mut peak_rss,
                std::ptr::null_mut(),
                &mut peak_commit,
                &mut page_faults,
            );
        }

        println!(
            "Peak RSS: {}, Peak Commit: {}, Page Faults: {}",
            if peak_rss == 0 {
                "N/A".to_string()
            } else {
                human_readable_size(peak_rss)
            },
            if peak_commit == 0 {
                "N/A".to_string()
            } else {
                human_readable_size(peak_commit)
            },
            page_faults
        );
    }
    #[cfg(not(all(feature = "mimalloc", feature = "mimalloc_extended")))]
    {
        println!("Memory stats: N/A (compile with --features mimalloc_extended)");
    }
}

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

    // Initialize logging
    if args.verbose {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Info)
            .init();
    } else {
        env_logger::Builder::from_default_env()
            .filter_level(log::LevelFilter::Warn)
            .init();
    }

    let start_time = Instant::now();
    
    info!("Starting DataFusion parquet query utility");
    info!("Data path: {}", args.data_path);
    info!("Query: {}", args.query);

    // Discover shard directories or use flat path
    let shard_dirs = if args.flat {
        vec![args.data_path.clone()]
    } else {
        discover_shard_directories(&args.data_path).await?
    };
    info!("Found {} shard directories", shard_dirs.len());

    // Determine target partitions
    let target_partitions = args.target_partitions.unwrap_or_else(|| {
        let shard_count = shard_dirs.len();
        info!("Setting target_partitions to {}", shard_count);
        shard_count
    });

    // Create DataFusion context
    let ctx = create_datafusion_context(target_partitions).await?;

    // Register the parquet files as a table using listing table
    register_parquet_table_listing(&ctx, &shard_dirs, "hits").await?;

    // Execute the query multiple times
    for run in 1..=args.num_runs {
        println!("\n--- Run {}/{} ---", run, args.num_runs);
        
        let query_start = Instant::now();
        let df = ctx.sql(&args.query).await?;
        let results = df.collect().await?;
        let query_duration = query_start.elapsed();
        
        // Output results
        match args.output_format.as_str() {
            "json" => output_json(&results)?,
            "csv" => output_csv(&results)?,
            _ => output_table(&results)?,
        }
        
        println!("\nQuery execution time: {:?}", query_duration);
        println!("Rows returned: {}", results.iter().map(|batch| batch.num_rows()).sum::<usize>());
        println!("Target partitions: {}", target_partitions);
        print_memory_stats();
    }

    let total_duration = start_time.elapsed();
    println!("\n--- Total Summary ---");
    println!("Total time: {:?}", total_duration);
    println!("Total runs: {}", args.num_runs);

    Ok(())
}

async fn discover_shard_directories(data_path: &str) -> Result<Vec<String>> {
    let mut shard_dirs = Vec::new();
    let index_entry = Path::new(data_path);

    let mut shards_dir = fs::read_dir(index_entry).await?;
    while let Some(shard_entry) = shards_dir.next_entry().await? {
        if shard_entry.file_type().await?.is_dir() {
            let shard_path = shard_entry.path();
            // Check if this directory contains parquet files
            let mut has_parquet = false;
            let mut shard_dir = fs::read_dir(&shard_path).await?;
            while let Some(file_entry) = shard_dir.next_entry().await? {
                if let Some(name_str) = file_entry.file_name().to_str() {
                    if name_str.ends_with(".parquet") {
                        has_parquet = true;
                        break;
                    }
                }
            }
            if has_parquet {
                shard_dirs.push(shard_path.to_string_lossy().to_string());
                info!("Found shard directory: {}", shard_path.display());
            }
        }
    }

    if shard_dirs.is_empty() {
        return Err(anyhow::anyhow!("No shard directories with parquet files found in: {}", data_path));
    }

    Ok(shard_dirs)
}



async fn create_datafusion_context(target_partitions: usize) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(8192);

    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(CacheManagerConfig::default())
        //.with_memory_limit(2_000_000_000, 1.0) // 2GB memory limit
        .build()?;
    
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));

    info!("Created DataFusion context with {} target partitions", target_partitions);
    Ok(ctx)
}

async fn register_parquet_table_listing(
    ctx: &SessionContext,
    shard_dirs: &[String],
    table_name: &str,
) -> Result<()> {
    use datafusion::datasource::listing::{ListingTable, ListingTableConfig, ListingTableUrl};
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    
    info!("Registering {} shard directories as table '{}' using listing table", shard_dirs.len(), table_name);

    let mut tables = Vec::new();
    
    // Create tables with inferred schema
    for shard_dir in shard_dirs {
        let table_path = ListingTableUrl::parse(&shard_dir)?;
        let file_format = ParquetFormat::new();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");

        let resolved_schema = listing_options
            .infer_schema(&ctx.state(), &table_path)
            .await
            .map_err(|e| anyhow::anyhow!("Failed to infer schema: {}", e))?;

        let table_config = ListingTableConfig::new(table_path.clone())
            .with_listing_options(listing_options)
            .with_schema(resolved_schema);

        let table = ListingTable::try_new(table_config)?;
        tables.push(ctx.read_table(Arc::new(table))?);
    }
    
    // Union all shard tables
    let mut combined_df = tables[0].clone();
    for table in &tables[1..] {
        combined_df = combined_df.union(table.clone())?;
    }
    
    ctx.register_table(table_name, combined_df.into_view())?;
    info!("Successfully registered listing table '{}'", table_name);
    Ok(())
}

fn output_json(results: &[datafusion::arrow::record_batch::RecordBatch]) -> Result<()> {
    // For now, just use table format for JSON output too
    output_table(results)
}

fn output_csv(results: &[datafusion::arrow::record_batch::RecordBatch]) -> Result<()> {
    // For now, just use table format for CSV output too  
    output_table(results)
}

fn output_table(results: &[datafusion::arrow::record_batch::RecordBatch]) -> Result<()> {
    use datafusion::arrow::util::pretty::print_batches;
    print_batches(results)?;
    Ok(())
}