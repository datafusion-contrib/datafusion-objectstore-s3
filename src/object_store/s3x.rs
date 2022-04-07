// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#[cfg(test)]
mod tests {

    use s3::bucket::Bucket;
    use s3::command::Command;
    use s3::creds::Credentials;
    use s3::region::Region;
    use s3::request_trait::Request;

    const ACCESS_KEY_ID: &str = "AKIAIOSFODNN7EXAMPLE";
    const SECRET_ACCESS_KEY: &str = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
    const PROVIDER_NAME: &str = "Static";
    const MINIO_ENDPOINT: &str = "http://localhost:9000";
    #[tokio::test]
    async fn test_rust_s3() {
        let region = Region::Custom {
            region: "".to_string(),
            endpoint: MINIO_ENDPOINT.to_string(),
        };
        let credentials = Credentials::new(
            Some(ACCESS_KEY_ID),
            Some(SECRET_ACCESS_KEY),
            None,
            None,
            None,
        )
        .unwrap();
        let mut bucket = Bucket::new("data", region, credentials).unwrap();
        bucket.set_path_style();

        let command = Command::GetObjectRange {
            start: 0,
            end: None,
        };
        let request = s3::request::Reqwest::new(&bucket, "xx", command);
        let x = request.response_data(false).await;
        println!("x: {x:#?}");
    }
}
