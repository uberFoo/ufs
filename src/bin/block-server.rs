//! RESTful Data Block Server
//!
//! GET -- retrieve the data for a particular block
//!
//! POST -- write some data to a particular block
//!
use std::{
    collections::HashMap,
    env,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use {
    dotenv::dotenv,
    futures::future,
    hyper::{
        header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE},
        rt::{Future, Stream},
        service::service_fn,
        Body, Method, Request, Response, Server, StatusCode,
    },
    // Note to self: error > warn > info > debug > trace
    log::{debug, error, info, trace},
    pretty_env_logger,
};

use ufs::{make_fs_key, BlockNumber, BlockReader, BlockWriter, FileStore, UfsUuid};

// Just a simple type alias
type BoxFut = Box<dyn Future<Item = Response<Body>, Error = hyper::Error> + Send>;

struct BlockStores {
    inner: HashMap<String, FileStore>,
    bundle_root: PathBuf,
}

impl BlockStores {
    pub fn new(bundle_root: PathBuf) -> Self {
        BlockStores {
            inner: HashMap::new(),
            bundle_root,
        }
    }

    fn open_store(bundle_path: PathBuf) -> Option<FileStore> {
        let fs_name = bundle_path.file_name().unwrap().to_str().unwrap();

        // FIXME: This doesn't allow for running the server as a daemon, i.e., it requires a
        // TTY, and someone to type the password.
        let password =
            rpassword::read_password_from_tty(Some(&format!("master password for {}: ", fs_name)))
                .unwrap();

        let key = make_fs_key(&password, &UfsUuid::new_root_fs(fs_name));

        match FileStore::load(key, bundle_path.clone()) {
            Ok(bs) => {
                debug!("loaded file store {:?}", bundle_path);
                Some(bs)
            }
            Err(_) => {
                error!(
                    "Unable to open File Store {}. Possibly invalid password.",
                    bundle_path.to_str().unwrap(),
                );
                None
            }
        }
    }

    pub fn get_store(&mut self, uri_path: &str) -> Option<(String, FileStore)> {
        let path = Path::new(uri_path).strip_prefix("/").unwrap();

        if path.iter().count() != 1 {
            // Don't allow arbitrary paths within the host file system -- this is just an ID.
            error!("Bundle ID is malformed {:?}", path);
            None
        } else {
            let bundle = path.to_str().expect("Bundle ID wasn't parsable.");
            let bundle_path = self.bundle_root.join(bundle);

            match self.inner.get(bundle) {
                Some(store) => Some((bundle.to_string(), store.clone())),
                None => match BlockStores::open_store(bundle_path) {
                    Some(store) => {
                        let store = self.inner.entry(bundle.to_owned()).or_insert(store);
                        Some((bundle.to_string(), store.clone()))
                    }
                    None => None,
                },
            }
        }
    }
}

fn block_manager(req: Request<Body>, store_map: &Arc<RwLock<BlockStores>>) -> BoxFut {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_FOUND;

    trace!("Received a request: {:?}", req);

    match (req.method(), req.uri().path(), req.uri().query()) {
        // Read a block
        //
        // The path component specifies the file system UUID, and the sole query component the
        // block number.
        (&Method::GET, path, Some(query)) => {
            if let Some((bundle, store)) = store_map.write().unwrap().get_store(path) {
                // FIXME:
                // * Allow a comma separated list of blocks, e.g., 0,5,4,10,1
                // * Allow a range of blocks, e.g., 5-9
                if let Ok(block) = query.parse::<BlockNumber>() {
                    debug!("Request to read {}:0x{:x?}", bundle, block);
                    if let Ok(data) = store.read_block(block) {
                        trace!("Read {} bytes", data.len());

                        response.headers_mut().insert(
                            CONTENT_TYPE,
                            HeaderValue::from_static("application/octet-stream"),
                        );
                        // response
                        //     .headers_mut()
                        //     .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
                        *response.body_mut() = Body::from(data);
                        *response.status_mut() = StatusCode::OK;
                    } else {
                        error!("Problem reading block {}:0x{:x?}", bundle, block);
                    }
                } else {
                    error!("Invalid block number: '{}'", query);
                    *response.status_mut() = StatusCode::BAD_REQUEST;
                }
            }
        }

        // Write a block
        // The path component specifies the file system UUID, and the sole query component the
        // block number.
        (&Method::POST, path, Some(query)) => {
            if let Some((bundle, mut store)) = store_map.write().unwrap().get_store(path) {
                if let Ok(block) = query.parse::<BlockNumber>() {
                    debug!("Request to write {}:0x{:x?}", bundle, block);

                    let bytes_written = req
                        .into_body()
                        // A future of when we finally have the full body...
                        .concat2()
                        // `move` the `Response` into this future...
                        .map(move |chunk| {
                            let body = chunk.iter().cloned().collect::<Vec<u8>>();

                            if let Ok(bytes_written) = store.write_block(block, body) {
                                trace!("Wrote {} bytes", bytes_written);
                                *response.body_mut() = Body::from(bytes_written.to_string());
                                *response.status_mut() = StatusCode::OK;
                                response
                            } else {
                                error!("Problem writing block {}:0x{:x?}", bundle, block);
                                response
                            }
                        });

                    // We can't just return the `Response` from this match arm,
                    // because we can't set the body until the `concat` future
                    // completed...
                    //
                    // However, `reversed` is actually a `Future` that will return
                    // a `Response`! So, let's return it immediately instead of
                    // falling through to the default return of this function.
                    return Box::new(bytes_written);
                } else {
                    error!("Invalid block number: '{}'", query);
                    *response.status_mut() = StatusCode::BAD_REQUEST;
                }
            }
        }

        // Create a new file system
        //
        // The query string contains the particulars:
        // * uuid=<uuid> is the new file system's uuid
        // * size=<size> is the block size
        // * blocks=<block count> is the number of blocks to allocate
        (&Method::POST, "/create", None) => {
            trace!("body {:#?}", req.body());
            debug!("Created new file system");
        }

        _ => {
            error!("Invalid method and/or URI: {}: {}", req.method(), req.uri());
        }
    };

    Box::new(future::ok(response))
}

fn main() -> Result<(), failure::Error> {
    pretty_env_logger::init();

    dotenv().ok();
    let port = env::var("BS_PORT")
        .expect("BS_PORT must specify the incoming connection port.")
        .parse::<u16>()
        .expect("BS_PORT must be a number.");
    let addr = ([0, 0, 0, 0], port).into();

    let bundle_root =
        env::var("BUNDLE_DIR").expect("BUNDLE_DIR must point to the file system bundle directory");
    if !Path::new(&bundle_root).is_dir() {
        panic!(
            "BUNDLE_DIR, {}, is not a directory, or does not exist.",
            bundle_root
        );
    }

    let block_stores = Arc::new(RwLock::new(BlockStores::new(PathBuf::from(bundle_root))));

    let new_service = move || {
        debug!("Starting a new service");
        let block_stores = block_stores.clone();
        service_fn(move |req| block_manager(req, &block_stores))
    };

    let server = Server::bind(&addr)
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    info!("Block manager listening on {}", addr);
    hyper::rt::run(server);

    Ok(())
}
