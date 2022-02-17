use datafusion::catalog::catalog::CatalogProvider;
use datafusion::catalog::schema::SchemaProvider;
use datafusion::datasource::listing::{ListingTable, ListingTableConfig};
use parking_lot::RwLock;

use crate::object_store::S3FileSystem;

pub struct S3Catalog {
    object_store: Arc<S3FileSystem>,
    schemas: RwLock<HashMap<String, Arc<dyn SchemaProvider>>>,
}

impl S3Catalog {
    /// Instantiates a new S3CatalogProvider with an empty collection of schemas.
    pub fn new(object_store: Arc<S3FileSystem>) -> Self {
        Self {
            object_store,
            schemas: RwLock::new(HashMap::new()),
        }
    }

    /// Adds a new schema to this catalog.
    /// If a schema of the same name existed before, it is replaced in the catalog and returned.
    pub fn register_schema(
        &self,
        name: impl Into<String>,
        schema: Arc<dyn SchemaProvider>,
    ) -> Option<Arc<dyn SchemaProvider>> {
        let mut schemas = self.schemas.write();
        schemas.insert(name.into(), schema)
    }

    /// Create a `SchemaProvider` from a directory uri
    // TODO: Look into partitioned files
    fn uri_to_schema(uri: impl Into<String>, name: &str) -> Arc<dyn SchemaProvider> {
        let files = self.object_store.list_file(dir);
        files.iter.map(|file| {
            let config = ListingTableConfig::new(self.object_store.clone(), file)
                .infer()
                .await?;
            let table = ListingTable::try_new(config)?;
        })
    }
}

impl CatalogProvider for MemoryCatalogProvider {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn schema_names(&self) -> Vec<String> {
        let schemas = self.schemas.read();
        schemas.keys().cloned().collect()
    }

    fn schema(&self, name: &str) -> Option<Arc<dyn SchemaProvider>> {
        let schemas = self.schemas.read();
        schemas.get(name).cloned()
    }
}
