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

    let mut tables = Vec::new();
    
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
    
    let mut combined_df = tables[0].clone();
    for table in &tables[1..] {
        combined_df = combined_df.union(table.clone())?;
    }
    
    ctx.register_table(table_name, combined_df.into_view())?;
    info!("Successfully registered listing table '{}'", table_name);
    Ok(())
}
