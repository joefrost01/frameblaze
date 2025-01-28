use anyhow::{anyhow, bail, Result};
use futures::StreamExt;
use object_store::{
    aws::AmazonS3Builder,
    azure::MicrosoftAzureBuilder,
    gcp::GoogleCloudStorageBuilder,
    local::LocalFileSystem,
    path::Path
    ,
    ObjectStore,
};
use std::fs::{read, File};
use std::io::{Error as IoError, ErrorKind, Write};
use std::path::PathBuf;
use std::sync::Arc;
use tempfile::NamedTempFile;
use bytes::Bytes;
use url::Url;

pub struct Storage {
    is_local: bool,
    store: Option<Arc<dyn ObjectStore>>,
    location: Option<Path>,
    local_path: PathBuf,
    spool: Option<NamedTempFile>,
    downloaded: bool,
    wrote_to_spool: bool,
    wrote_spool_path: Option<PathBuf>,
}

impl Storage {
    pub fn new(path: &str) -> Result<Self> {
        let (store, is_local, loc) = Self::detect_object_store(path)?;
        if is_local {
            Ok(Self {
                is_local: true,
                store: None,
                location: None,
                local_path: PathBuf::from(path),
                spool: None,
                downloaded: false,
                wrote_to_spool: false,
                wrote_spool_path: None,
            })
        } else {
            Ok(Self {
                is_local: false,
                store: Some(store),
                location: Some(loc),
                local_path: PathBuf::new(),
                spool: None,
                downloaded: false,
                wrote_to_spool: false,
                wrote_spool_path: None,
            })
        }
    }

    pub fn get_target_file(&mut self) -> Result<File> {
        if self.is_local {
            let f = File::create(&self.local_path)?;
            Ok(f)
        } else {
            let spool = NamedTempFile::new()?;
            self.spool = Some(spool);
            let spool_path = self.spool.as_ref().unwrap().path().to_path_buf();
            let spool_file = self.spool.as_ref().unwrap().reopen()?;
            self.wrote_to_spool = true;
            self.wrote_spool_path = Some(spool_path);
            Ok(spool_file)
        }
    }

    /// Call this once Polars has finished writing. If remote, we do a single
    /// `store.put(...)` of the spool file contents.
    pub fn finish_write(&mut self) -> Result<()> {
        if self.is_local {
            return Ok(());
        }
        if !self.wrote_to_spool {
            // no spool was written
            return Ok(());
        }
        let spool_path = self
            .wrote_spool_path
            .as_ref()
            .ok_or_else(|| anyhow!("No spool path found"))?;

        let store = self
            .store
            .as_ref()
            .ok_or_else(|| anyhow!("Missing store for remote"))?;
        let location = self
            .location
            .as_ref()
            .ok_or_else(|| anyhow!("Missing location for remote"))?;

        // read spool file fully into memory
        let buf = read(spool_path)?;
        let payload = object_store::PutPayload::from_bytes(Bytes::from(buf));

        // build a runtime
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;
        // do a single put
        rt.block_on(async {
            store
                .put(location, payload)
                .await
                .map_err(|e| IoError::new(ErrorKind::Other, format!("Error uploading spool: {e}")))
        })?;

        Ok(())
    }


    /// Return a local File handle for reading.
    /// - If local, we open local_path.
    /// - If remote, spool once, then open spool file.
    pub fn get_source_file(&mut self) -> Result<File> {
        if self.is_local {
            // just open local_path
            let f = File::open(&self.local_path)?;
            return Ok(f);
        }

        // If remote => spool if not already
        if !self.downloaded {
            self.download_remote()?;
            self.downloaded = true;
        }

        // Now open spool
        let spool_path = self
            .spool
            .as_ref()
            .ok_or_else(|| anyhow!("No spool file found"))?
            .path()
            .to_path_buf();

        let f = File::open(spool_path)?;
        Ok(f)
    }

    fn download_remote(&mut self) -> Result<()> {
        // Must have store+location
        let store = self
            .store
            .as_ref()
            .ok_or_else(|| anyhow!("No store found (remote)"))?;
        let location = self
            .location
            .as_ref()
            .ok_or_else(|| anyhow!("No location found (remote)"))?;

        let spool = NamedTempFile::new()?;
        let mut sink = spool.as_file();

        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()?;

        rt.block_on(async {
            match store.get(location).await {
                Ok(getres) => {
                    // `getres` is the entire GetResult
                    let mut stream = getres.into_stream();
                    while let Some(chunk_res) = stream.next().await {
                        let chunk = chunk_res.map_err(|e| {
                            std::io::Error::new(ErrorKind::Other, format!("Download error: {e}"))
                        })?;
                        sink.write_all(&chunk)?;
                    }
                    sink.flush()?;
                    Ok(())
                }
                Err(e) => Err(std::io::Error::new(
                    ErrorKind::NotFound,
                    format!("Error fetching remote object: {e}"),
                )),
            }
        })?;

        self.spool = Some(spool);
        Ok(())
    }


    /// Decide local vs. remote from path. Returns (store, is_local, location).
    fn detect_object_store(path: &str) -> Result<(Arc<dyn ObjectStore>, bool, Path)> {
        if let Ok(url) = Url::parse(path) {
            match url.scheme() {
                "s3" => {
                    let bucket = url.host_str().ok_or_else(|| anyhow!("Invalid S3 URL: {path}"))?;
                    let store = AmazonS3Builder::from_env()
                        .with_bucket_name(bucket)
                        .build()?;
                    let key = url.path().trim_start_matches('/');
                    Ok((Arc::new(store), false, Path::from(key)))
                }
                "gs" => {
                    let bucket = url.host_str().ok_or_else(|| anyhow!("Bad gs URL: {path}"))?;
                    let store = GoogleCloudStorageBuilder::from_env()
                        .with_bucket_name(bucket)
                        .build()?;
                    let key = url.path().trim_start_matches('/');
                    Ok((Arc::new(store), false, Path::from(key)))
                }
                "azure" => {
                    let container = url
                        .host_str()
                        .ok_or_else(|| anyhow!("Bad azure URL: {path}"))?;
                    let store = MicrosoftAzureBuilder::from_env()
                        .with_container_name(container)
                        .build()?;
                    let key = url.path().trim_start_matches('/');
                    Ok((Arc::new(store), false, Path::from(key)))
                }
                "file" => {
                    let local = LocalFileSystem::new();
                    let key = url.path().trim_start_matches('/');
                    Ok((Arc::new(local), true, Path::from(key)))
                }
                other => bail!("Unsupported scheme '{other}' in {path}"),
            }
        } else {
            // local
            let local = LocalFileSystem::new();
            Ok((Arc::new(local), true, Path::from(path)))
        }
    }
}

impl Drop for Storage {
    fn drop(&mut self) {
        // spool is auto-removed by NamedTempFile
    }
}
