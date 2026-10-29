//! Metadata-only listing: never opens a retained database generation.
use super::*;
use crate::inspection::{IndexInfo, IndexPage};
use std::collections::BTreeSet;

impl Store {
    pub fn indexes(&self, after: Option<&str>, limit: usize) -> Result<IndexPage> {
        ensure!(
            (1..=1000).contains(&limit),
            "index page limit must be 1..1000"
        );
        if let Some(cursor) = after {
            ensure!(valid_name(cursor), "invalid index cursor");
        }
        let activation = selected_activation(&self.root)?.context("missing selected index")?;
        let mut names = BTreeSet::new();
        for entry in fs::read_dir(self.root.join("index"))? {
            let entry = entry?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if valid_name(&name)
                && entry.file_type()?.is_dir()
                && after.is_none_or(|cursor| name.as_str() > cursor)
            {
                names.insert(name);
                if names.len() > limit + 1 {
                    names.pop_last();
                }
            }
        }
        let more = names.len() > limit;
        let mut keyspaces: Vec<String> = self
            .db
            .list_keyspace_names()
            .iter()
            .map(ToString::to_string)
            .collect();
        keyspaces.sort();
        let indexes: Vec<_> = names
            .into_iter()
            .take(limit)
            .map(|name| {
                let selected = name == activation.generation;
                let complete = self
                    .root
                    .join("index")
                    .join(&name)
                    .join("version")
                    .is_file();
                IndexInfo {
                    name,
                    selected,
                    state: if selected {
                        "selected"
                    } else if complete {
                        "retained"
                    } else {
                        "incomplete"
                    }
                    .into(),
                    format_version: selected.then_some(activation.version),
                    keyspaces: if selected { keyspaces.clone() } else { vec![] },
                }
            })
            .collect();
        let next_cursor = if more {
            indexes.last().map(|index| index.name.clone())
        } else {
            None
        };
        Ok(IndexPage {
            indexes,
            next_cursor,
        })
    }
}
fn valid_name(name: &str) -> bool {
    name.strip_prefix("generation-")
        .is_some_and(|suffix| Uuid::parse_str(suffix).is_ok())
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn index_pages_identify_selection_without_opening_retained_databases() {
        let dir = tempfile::tempdir().unwrap();
        let mut config = Config::default();
        config.storage.root = dir.path().join("store");
        let store = Store::open(&config).unwrap();
        let first = store.indexes(None, 1).unwrap();
        assert!(first.indexes[0].selected);
        assert!(first.indexes[0].keyspaces.contains(&"objects".into()));
        let fake = format!("generation-{}", Uuid::new_v4());
        fs::create_dir(config.storage.root.join("index").join(fake)).unwrap();
        let page = store.indexes(None, 1).unwrap();
        assert!(page.next_cursor.is_some());
        let next = store.indexes(page.next_cursor.as_deref(), 1).unwrap();
        assert_eq!(next.indexes.len(), 1);
        assert!(next.next_cursor.is_none());
        assert!(store.indexes(Some("../../secret"), 1).is_err());
        assert!(store.indexes(None, 0).is_err());
    }
}
