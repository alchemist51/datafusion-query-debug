use anyhow::Result;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::execution::context::SessionContext;
use log::info;
use std::sync::Arc;

pub async fn register_parquet_table_listing(
    ctx: &SessionContext,
    shard_dirs: &[String],
    table_name: &str,
) -> Result<()> {
    info!("Registering {} shard directories as table '{}' using listing table", shard_dirs.len(), table_name);

    // For single directory, use register_listing_table directly
    if shard_dirs.len() == 1 {
        let file_format = ParquetFormat::new();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        
        ctx.register_listing_table(
            table_name,
            &shard_dirs[0],
            listing_options,
            None,
            None,
        ).await?;
        
        info!("Successfully registered listing table '{}'", table_name);
        return Ok(());
    }

    // For multiple directories, register each as listing table and union them
    let mut tables = Vec::new();
    
    for (i, shard_dir) in shard_dirs.iter().enumerate() {
        let temp_table_name = format!("{}_{}", table_name, i);
        let file_format = ParquetFormat::new();
        let listing_options = ListingOptions::new(Arc::new(file_format))
            .with_file_extension(".parquet");
        
        ctx.register_listing_table(
            &temp_table_name,
            shard_dir,
            listing_options,
            None,
            None,
        ).await?;
        
        tables.push(ctx.table(&temp_table_name).await?);
    }
    
    let mut combined_df = tables[0].clone();
    for table in &tables[1..] {
        combined_df = combined_df.union(table.clone())?;
    }
    
    ctx.register_table(table_name, combined_df.into_view())?;
    info!("Successfully registered listing table '{}'", table_name);
    Ok(())
}
