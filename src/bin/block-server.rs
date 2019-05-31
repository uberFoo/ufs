//! RESTful Data Block Server
//!
//! GET -- retrieve the data for a particular block
//!
//! POST -- write some data to a particular block
//!
use std::{collections::HashMap, env, path::Path};

use dotenv::dotenv;
use futures::future;
use hyper::{
    header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE},
    rt::{Future, Stream},
    service::service_fn,
    Body, Method, Request, Response, Server, StatusCode,
};
// Note to self: error > warn > info > debug > trace
use log::{debug, error, info, trace, warn};
use pretty_env_logger;

use ufs::{BlockNumber, BlockReader, BlockWriter, FileStore};

// Just a simple type alias
type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

fn get_store(
    uri_path: &str,
    bundle_root: &Path,
    store_map: &mut HashMap<String, Option<FileStore>>,
) -> Option<(String, FileStore)> {
    trace!("store map {:#?}", store_map);
    let path = Path::new(uri_path).strip_prefix("/").unwrap();
    if path.iter().count() != 1 {
        // Don't allow arbitrary paths within the host file system -- this is just an ID.
        error!("Bundle ID is malformed {:?}", path);
        None
    } else {
        let bundle = path.to_str().expect("Bundle ID wasn't parsable.");
        let bundle_path = bundle_root.join(bundle);

        let store = store_map.entry(bundle.to_string()).or_insert_with(|| {
            match FileStore::load(bundle_path.clone()) {
                Ok(bs) => Some(bs),
                Err(e) => {
                    error!(
                        "Unable to open File Store {}: {}",
                        bundle_path.to_str().unwrap(),
                        e
                    );
                    None
                }
            }
        });

        match store {
            Some(store) => Some((bundle.to_string(), store.clone())),
            None => None,
        }
    }
}

fn block_manager(
    req: Request<Body>,
    bundle_root: &Path,
    store_map: &mut HashMap<String, Option<FileStore>>,
) -> BoxFut {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_FOUND;

    trace!("Received a request: {:?}", req);

    match (req.method(), req.uri().path(), req.uri().query()) {
        // Read a block
        //
        // The path component specifies the file system UUID, and the sole query component the
        // block number.
        (&Method::GET, path, Some(query)) => {
            if let Some((bundle, store)) = get_store(path, bundle_root, store_map) {
                // FIXME:
                // * Allow a comma separated list of blocks, e.g., 0,5,4,10,1
                // * Allow a range of blocks, e.g., 5-9
                if let Ok(block) = query.parse::<BlockNumber>() {
                    debug!("Request to read {}: {}", bundle, block);
                    if let Ok(data) = store.read_block(block) {
                        trace!("Read {} bytes", data.len());

                        response.headers_mut().insert(
                            CONTENT_TYPE,
                            HeaderValue::from_static("application/octet-stream"),
                        );
                        response
                            .headers_mut()
                            .insert(ACCESS_CONTROL_ALLOW_ORIGIN, HeaderValue::from_static("*"));
                        *response.body_mut() = Body::from(data);
                        *response.status_mut() = StatusCode::OK;
                    } else {
                        error!("Problem reading block {}", block);
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
            if let Some((bundle, mut store)) = get_store(path, bundle_root, store_map) {
                if let Ok(block) = query.parse::<BlockNumber>() {
                    debug!("Request to write {}:{}", bundle, block);

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
                                error!("Problem writing block {}", block);
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
    let addr = ([127, 0, 0, 1], port).into();

    let bundle_root =
        env::var("BUNDLE_DIR").expect("BUNDLE_DIR must point to the file system bundle directory");
    if !Path::new(&bundle_root).is_dir() {
        panic!(
            "BUNDLE_DIR, {}, is not a directory, or does not exist.",
            bundle_root
        );
    }

    // FIXME: This does _not_ work as expected.
    // It looks like new_service is _only_ called when I'm not using client::reqwest.  In other
    // words, creating a reqwest::Client, and then using get on it caches the FileStore as expected.
    // So there is some session stuff happening?
    // Also, even when it appears to "work", it isn't really.  I really want the caching to happen
    // globally.
    let store: HashMap<String, Option<FileStore>> = HashMap::new();

    let new_service = move || {
        debug!("Starting a new service");
        let bundle_root = bundle_root.clone();
        let mut store = store.clone();

        service_fn(move |req| block_manager(req, &Path::new(&bundle_root), &mut store))
    };

    let server = Server::bind(&addr)
        // .serve(|| service_fn(block_manager))
        .serve(new_service)
        .map_err(|e| eprintln!("server error: {}", e));

    info!("Block manager listening on {}", addr);
    hyper::rt::run(server);

    Ok(())
}
