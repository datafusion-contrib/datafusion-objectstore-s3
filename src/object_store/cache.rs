use bytes::Bytes;
use parking_lot::Mutex;
use tokio::time::Instant;

pub struct Cache {
    branches: Mutex<Vec<CachedData>>,
}

impl Cache {
    pub fn new(max_cache_branches_per_file: usize) -> Self {
        Self {
            branches: Mutex::new(Vec::with_capacity(max_cache_branches_per_file)),
        }
    }

    pub fn get(&self, start: u64, length: usize) -> Option<Bytes> {
        let mut branches = self.branches.lock();

        for c in branches.iter_mut() {
            if let Some(bytes) = c.try_get(start, length) {
                c.last_used = Instant::now();
                return Some(bytes);
            }
        }

        None
    }

    pub fn put(&self, start: u64, bytes: Bytes) {
        let mut branches = self.branches.lock();
        if branches.len() < branches.capacity() {
            branches.push(CachedData::new(start, bytes));
        } else {
            let oldest = branches
                .iter_mut()
                .min_by_key(|c| c.last_used)
                .expect("cache branch list cannot by empty");
            *oldest = CachedData::new(start, bytes);
        }
    }
}

struct CachedData {
    start: u64,
    bytes: Bytes,
    last_used: Instant,
}

impl CachedData {
    fn new(start: u64, bytes: Bytes) -> Self {
        Self {
            start,
            bytes,
            last_used: Instant::now(),
        }
    }
    // whether `test_interval` is inside `a` (start, length).
    fn try_get(&self, start: u64, length: usize) -> Option<Bytes> {
        if start < self.start || start + length as u64 > self.start + self.bytes.len() as u64 {
            return None;
        }

        let offset = (start - self.start) as usize;
        let data = self.bytes.slice(offset..offset + length);

        Some(data)
    }
}

#[cfg(test)]
mod tests {
    use crate::object_store::cache::Cache;
    use bytes::Bytes;

    #[test]
    fn test_cache() {
        let cache = Cache::new(2);

        let mut example_data = vec![0; 128];
        for (idx, b) in example_data.iter_mut().enumerate() {
            *b = idx as u8;
        }
        let example_data = Bytes::from(example_data);

        assert!(cache.get(100, 50).is_none());
        cache.put(50, example_data.clone());

        let r = cache.get(100, 2);
        assert_eq!(r, Some(Bytes::from(vec![50, 51])));

        let r = cache.get(50, 128);
        assert_eq!(r, Some(example_data));

        assert!(cache.get(10, 50).is_none());
        assert!(cache.get(100, 1024).is_none());
    }
}
