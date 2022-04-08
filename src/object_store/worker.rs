use datafusion_data_access::Result;
use s3::Bucket;
use std::io;
use std::io::ErrorKind;
use std::sync::Arc;
use tokio::sync::mpsc::Receiver;

pub struct BucketWorker {
    bucket: Arc<Bucket>,
    rx: Receiver<GetObjectRange>,
}

impl BucketWorker {
    pub fn new(bucket: Arc<Bucket>, rx: Receiver<GetObjectRange>) -> Self {
        Self { bucket, rx }
    }

    pub async fn wait_for_io(&mut self) {
        while let Some(req) = self.rx.recv().await {
            let end = if req.length > 0 {
                // TODO: co gdy start=0 i length = 1, będzie panic w get_object_range ???
                Some(req.start + req.length as u64 - 1)
            } else {
                None
            };

            let bucket = self.bucket.clone();
            tokio::spawn(async move {
                println!(
                    "id: {} file: {} start: {} len: {}",
                    req.id, req.path, req.start, req.length
                );
                let result = bucket
                    .get_object_range(&req.path, req.start, end)
                    .await
                    .map_err(|e| io::Error::new(ErrorKind::Other, e));

                req.tx
                    .send(result)
                    .expect("send results on bucket worker thread");
            });
        }
        println!("finished bucket worker thread");
    }
}

#[derive(Debug)]
pub struct GetObjectRange {
    pub id: usize,
    pub path: String,
    pub start: u64,
    pub length: usize,
    pub tx: std::sync::mpsc::Sender<Result<(Vec<u8>, u16)>>,
}
