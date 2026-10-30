use super::Store;
use anyhow::Result;

/// A write was rejected before touching storage because it would cross the configured free-space reserve.
#[derive(Debug, thiserror::Error)]
#[error("insufficient free disk space for write")]
pub struct StorageFull;

impl Store {
    /// Ensure a write can proceed while preserving the larger configured store/stream reserve.
    pub fn ensure_write_space(&self, additional: u64) -> Result<()> {
        const METADATA_RESERVE: u64 = 1024 * 1024;
        let reserve_mib = self
            .config
            .storage
            .min_free_mib
            .max(self.config.streaming.min_free_mib as u64);
        let required = reserve_mib
            .saturating_mul(1024 * 1024)
            .saturating_add(METADATA_RESERVE)
            .saturating_add(additional);
        let available = fs2::available_space(&self.root)?;
        if available < required {
            return Err(StorageFull.into());
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{config::Config, engine::PutResult};
    use bytes::Bytes;
    use std::sync::Arc;

    #[test]
    fn rejected_space_check_leaves_store_writable_and_data_unchanged() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().into();
        config.storage.min_free_mib = 0;
        config.streaming.min_free_mib = 0;
        let store = Arc::new(Store::open(&config).unwrap());
        let before = store.status().unwrap();

        let error = store.ensure_write_space(u64::MAX).unwrap_err();
        assert!(error.downcast_ref::<StorageFull>().is_some());
        let after = store.status().unwrap();
        assert_eq!(before.physical_bytes, after.physical_bytes);

        let payload = Bytes::from_static(b"writer remains healthy");
        let first = store.put_many(std::slice::from_ref(&payload)).unwrap();
        let second: Vec<PutResult> = store.put_many(std::slice::from_ref(&payload)).unwrap();
        assert!(!first[0].duplicate);
        assert!(second[0].duplicate);
        assert_eq!(store.get(&first[0].hash).unwrap(), Some(payload.to_vec()));
    }
}
