use parking_lot::RwLock;

pub struct Cache {
    inner: RwLock<Vec<CachedData>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            inner: Default::default(),
        }
    }

    pub fn get(&self, start: u64, length: usize) -> Option<Vec<u8>> {
        self.inner
            .read()
            .iter()
            .filter_map(|c| c.try_get(start, length))
            .next()
    }

    pub fn put(&self, start: u64, data: Vec<u8>) {
        self.inner.write().push(CachedData { start, bytes: data });
    }
}

struct CachedData {
    start: u64,
    bytes: Vec<u8>,
}

impl CachedData {
    // whether `test_interval` is inside `a` (start, length).
    fn try_get(&self, start: u64, length: usize) -> Option<Vec<u8>> {
        if start < self.start || start + length as u64 > self.start + self.bytes.len() as u64 {
            return None;
        }

        let offset = (start - self.start) as usize;
        let data = &self.bytes[offset..offset + length];

        Some(data.to_vec())
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::cache::Cache;

    #[test]
    fn test_cache() {
        let cache = Cache::new();

        let mut example_data = vec![0; 128];
        for (idx, b) in example_data.iter_mut().enumerate() {
            *b = idx as u8;
        }

        assert!(cache.get(100, 50).is_none());
        cache.put(50, example_data.clone());

        let r = cache.get(100, 2);
        assert_eq!(r, Some(vec![50, 51]));

        let r = cache.get(50, 128);
        assert_eq!(r, Some(example_data));

        assert!(cache.get(10, 50).is_none());
        assert!(cache.get(100, 1024).is_none());
    }
}
