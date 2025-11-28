use anyhow::Result;
use datafusion::execution::cache::cache_manager::CacheManagerConfig;
use datafusion::execution::context::SessionContext;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use datafusion::prelude::*;
use log::info;
use std::sync::Arc;
use datafusion::execution::cache::cache_unit::{DefaultFileStatisticsCache, DefaultFilesMetadataCache};

pub async fn create_datafusion_context(target_partitions: usize) -> Result<SessionContext> {
    let config = SessionConfig::new()
        .with_target_partitions(target_partitions)
        .with_batch_size(8192);
    let limit = 500*1024*1024;

    let cacheManagerConfig = CacheManagerConfig::default()
        .with_files_statistics_cache(Some(Arc::new(DefaultFileStatisticsCache::default())))
        .with_file_metadata_cache(Some(Arc::new(DefaultFilesMetadataCache::new(limit.clone()))))
        .with_metadata_cache_limit(limit);

    let runtime = RuntimeEnvBuilder::new()
        .with_cache_manager(cacheManagerConfig)
        .build()?;
    
    let ctx = SessionContext::new_with_config_rt(config, Arc::new(runtime));

    info!("Created DataFusion context with {} target partitions", target_partitions);
    Ok(ctx)
}
