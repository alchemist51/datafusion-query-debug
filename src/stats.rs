pub fn print_memory_stats() {
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
