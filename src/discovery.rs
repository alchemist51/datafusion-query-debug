use anyhow::Result;
use log::info;
use std::path::Path;
use tokio::fs;

pub async fn discover_shard_directories(data_path: &str) -> Result<Vec<String>> {
    let mut shard_dirs = Vec::new();
    let index_entry = Path::new(data_path);

    let mut shards_dir = fs::read_dir(index_entry).await?;
    while let Some(shard_entry) = shards_dir.next_entry().await? {
        if shard_entry.file_type().await?.is_dir() {
            let shard_path = shard_entry.path();
            if has_parquet_files(&shard_path).await? {
                shard_dirs.push(shard_path.to_string_lossy().to_string());
                info!("Found shard directory: {}", shard_path.display());
            }
        }
    }

    if shard_dirs.is_empty() {
        return Err(anyhow::anyhow!("No shard directories with parquet files found in: {}", data_path));
    }

    println!("\nFound shard directories:");
    for (i, dir) in shard_dirs.iter().enumerate() {
        println!("  [{}] {}", i + 1, dir);
    }

    Ok(shard_dirs)
}

async fn has_parquet_files(path: &Path) -> Result<bool> {
    let mut dir = fs::read_dir(path).await?;
    while let Some(entry) = dir.next_entry().await? {
        if let Some(name_str) = entry.file_name().to_str() {
            if name_str.ends_with(".parquet") {
                return Ok(true);
            }
        }
    }
    Ok(false)
}
