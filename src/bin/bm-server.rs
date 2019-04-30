//! RESTful Data Block Server
//!
//! GET -- retrieve the data for a particular block
//!
//! POST -- write some data to a particular block
//!
use std::{collections::HashMap, env, path::Path};

use dotenv::dotenv;
use failure::Error;
use futures::future;
use hyper::{
    header::{HeaderValue, ACCESS_CONTROL_ALLOW_ORIGIN, CONTENT_TYPE},
    rt::Future,
    service::service_fn,
    Body, Method, Request, Response, Server, StatusCode,
};
// Note to self: error > warn > info > debug > trace
use log::{debug, error, info, trace, warn};
use pretty_env_logger;

use ufs::{BlockNumber, BlockStorage, FileStore};

// Just a simple type alias
type BoxFut = Box<Future<Item = Response<Body>, Error = hyper::Error> + Send>;

fn block_manager(
    req: Request<Body>,
    bundle_root: &Path,
    store_map: &mut HashMap<String, Option<FileStore>>,
) -> BoxFut {
    let mut response = Response::new(Body::empty());
    *response.status_mut() = StatusCode::NOT_FOUND;

    trace!("Received a request {:?}", req);

    match (req.method(), req.uri().path()) {
        (&Method::GET, path) => {
            let path = Path::new(path).strip_prefix("/").unwrap();
            if path.iter().count() != 1 {
                // Don't allow arbitrary paths within the file system.  This is just an ID.
                error!("Bundle ID is malformed {:?}", path);
                *response.status_mut() = StatusCode::BAD_REQUEST;
            } else {
                let bundle = path.to_str().expect("Bundle ID wasn't parsable.");
                let bundle_path = bundle_root.join(bundle);

                println!("store {:?}", store_map);

                let store = store_map.entry(bundle.to_string()).or_insert_with(|| {
                    match FileStore::load(bundle_path.clone()) {
                        // load_file_store(bundle_path.clone()) {
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

                if let Some(store) = store {
                    if let Some(query) = req.uri().query() {
                        // FIXME:
                        // * Allow a comma separated list of blocks, e.g., 0,5,4,10,1
                        // * Allow a range of blocks, e.g., 5-9
                        if let Ok(block) = query.parse::<BlockNumber>() {
                            debug!("Request for {}:{}", bundle, block);
                            if let Ok(data) = store.read_block(block) {
                                trace!("read data {:?}", data);

                                response.headers_mut().insert(
                                    CONTENT_TYPE,
                                    HeaderValue::from_static("application/octet-stream"),
                                );
                                response.headers_mut().insert(
                                    ACCESS_CONTROL_ALLOW_ORIGIN,
                                    HeaderValue::from_static("*"),
                                );
                                *response.body_mut() = Body::from(data);
                                *response.status_mut() = StatusCode::OK;
                            } else {
                                error!("Problem reading block '{}'", block);
                            }
                        } else {
                            error!("Invalid block number: '{}'", query);
                            *response.status_mut() = StatusCode::BAD_REQUEST;
                        }
                    } else {
                        warn!("Missing block number.");
                        *response.status_mut() = StatusCode::BAD_REQUEST;
                    }
                }
            }
        }

        (&Method::POST, "/") => {
            info!("Got a POST");
            *response.status_mut() = StatusCode::OK;
        }

        _ => {
            error!("Invalid method and/or URI: {}: {}", req.method(), req.uri());
        }
    };

    Box::new(future::ok(response))
}

fn main() -> Result<(), Error> {
    pretty_env_logger::init();

    dotenv().ok();
    let port = env::var("BS_PORT")
        .expect("BS_PORT must specify the incoming connection port.")
        .parse::<u16>()
        .expect("BS_PORT must be a number.");
    let addr = ([127, 0, 0, 1], port).into();

    let bundle_root =
        env::var("BUNDLE_DIR").expect("BUNDLE_DIR must point to theh file system bundle directory");
    if !Path::new(&bundle_root).is_dir() {
        panic!(
            "BUNDLE_DIR, {}, is not a directory, or does not exist.",
            bundle_root
        );
    }

    let new_service = move || {
        let bundle_root = bundle_root.clone();
        let mut store: HashMap<String, Option<FileStore>> = HashMap::new();

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
