use datafusion_data_access::Result;
use s3::command::Command;
use s3::request::Reqwest;
use s3::request_trait::Request;
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
            let bucket = self.bucket.clone();
            tokio::spawn(async move {
                let result = bucket_read(&bucket, &req.path, req.start, req.length).await;

                req.tx
                    .send(result)
                    .expect("send results on bucket worker thread");
            });
        }
        println!("finished bucket worker thread");
    }
}

pub async fn bucket_read(
    bucket: &Bucket,
    path: &str,
    start: u64,
    length: usize,
) -> Result<Vec<u8>> {
    let end = if length > 0 {
        Some(start + length as u64 - 1)
    } else {
        None
    };

    let command = Command::GetObjectRange { start, end };
    let request = Reqwest::new(bucket, path, command);
    let res = request
        .response()
        .await
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    let status = res.status();
    if !status.is_success() {
        return Err(io::Error::new(
            ErrorKind::Other,
            format!("got status code '{status}'"),
        ));
    }

    let bytes = res
        .bytes()
        .await
        .map_err(|e| io::Error::new(ErrorKind::Other, e))?;

    Ok(bytes.to_vec())
}

#[derive(Debug)]
pub struct GetObjectRange {
    pub id: usize,
    pub path: String,
    pub start: u64,
    pub length: usize,
    pub tx: std::sync::mpsc::Sender<Result<Vec<u8>>>,
}
