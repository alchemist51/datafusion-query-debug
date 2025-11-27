use anyhow::{Context, Result};
use clap::Parser;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use log::info;
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
    #[arg(short, long, default_value = "/home/ec2-user/benchsetup/OpenSearch/build/distribution/local/opensearch-3.3.0-SNAPSHOT/data")]
    data_path: String,

    /// SQL query to execute
    #[arg(short, long, default_value = "SELECT sum('UserID') FROM clickbench")]
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

    // Discover shards and parquet files
    let parquet_files = discover_parquet_files(&args.data_path).await?;
    info!("Found {} parquet files across shards", parquet_files.len());

    // Determine target partitions
    let target_partitions = args.target_partitions.unwrap_or_else(|| {
        let shard_count = count_shards(&args.data_path).unwrap_or(8);
        info!("Auto-detected {} shards, setting target_partitions to {}", shard_count, shard_count);
        shard_count
    });

    // Create DataFusion context
    let ctx = create_datafusion_context(target_partitions).await?;


    // Register the parquet files as a table
    register_parquet_table(&ctx, &parquet_files, "hits").await?;

    // Execute the query
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

    let total_duration = start_time.elapsed();
    
    println!("\n--- Performance Summary ---");
    println!("Query execution time: {:?}", query_duration);
    println!("Total time: {:?}", total_duration);
    println!("Rows returned: {}", results.iter().map(|batch| batch.num_rows()).sum::<usize>());
    println!("Target partitions: {}", target_partitions);
    print_memory_stats();

    Ok(())
}

async fn discover_parquet_files(data_path: &str) -> Result<Vec<String>> {
    let mut parquet_files = Vec::new();
    let index_entry = Path::new(data_path);

    let mut shards_dir = fs::read_dir(index_entry).await?;
    while let Some(shard_entry) = shards_dir.next_entry().await? {
        if shard_entry.file_type().await?.is_dir() {
            // Look for parquet files in this shard
            let shard_path = shard_entry.path();
            let mut shard_dir = fs::read_dir(&shard_path).await?;
            while let Some(file_entry) = shard_dir.next_entry().await? {
                let file_name = file_entry.file_name();
                if let Some(name_str) = file_name.to_str() {
                    if name_str.ends_with(".parquet") {
                        let full_path = file_entry.path();
                        parquet_files.push(full_path.to_string_lossy().to_string());
                        info!("Found parquet file: {}", full_path.display());
                    }
                }
            }
        }
    }

    if parquet_files.is_empty() {
        return Err(anyhow::anyhow!("No parquet files found in: {}", data_path));
    }

    Ok(parquet_files)
}

fn count_shards(data_path: &str) -> Result<usize> {
    let data_dir = Path::new(data_path);
    let nodes_path = data_dir.join("nodes");
    
    if !nodes_path.exists() {
        return Ok(8); // Default fallback
    }

    let mut shard_count = 0;
    
    // This is a simplified count - in practice you'd want to traverse the full structure
    // For now, we'll use a heuristic based on the typical OpenSearch structure
    if let Ok(entries) = std::fs::read_dir(&nodes_path) {
        for node_entry in entries.flatten() {
            if node_entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                let indices_path = node_entry.path().join("indices");
                if let Ok(index_entries) = std::fs::read_dir(&indices_path) {
                    for index_entry in index_entries.flatten() {
                        if index_entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                            if let Ok(shard_entries) = std::fs::read_dir(&index_entry.path()) {
                                for shard_entry in shard_entries.flatten() {
                                    if shard_entry.file_type().map(|ft| ft.is_dir()).unwrap_or(false) {
                                        shard_count += 1;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    Ok(if shard_count > 0 { shard_count } else { 8 })
}

async fn create_datafusion_context(target_partitions: usize) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(8192);

    let runtime = RuntimeEnvBuilder::new()
        //.with_memory_limit(2_000_000_000, 1.0) // 2GB memory limit
        .build()?;
    
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));

    info!("Created DataFusion context with {} target partitions", target_partitions);
    Ok(ctx)
}

async fn register_parquet_table(
    ctx: &SessionContext,
    parquet_files: &[String],
    table_name: &str,
) -> Result<()> {
    info!("Registering {} parquet files as table '{}'", parquet_files.len(), table_name);

    // Use read_parquet to register all files at once
    let df = ctx.read_parquet(parquet_files[0].clone(), ParquetReadOptions::default()).await?;
    
    // If there are multiple files, union them
    if parquet_files.len() > 1 {
        let mut combined_df = df;
        for file_path in &parquet_files[1..] {
            let next_df = ctx.read_parquet(file_path.clone(), ParquetReadOptions::default()).await?;
            combined_df = combined_df.union(next_df)?;
        }

        ctx.register_table(table_name, combined_df.into_view())?;
    } else {
        ctx.register_table(table_name, df.into_view())?;
    }

    info!("Successfully registered table '{}'", table_name);
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