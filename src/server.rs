//! Embedded UFS Block Server
//!
//! A mounted UFS may also act as a block server for remote connections. That is implemented herein.
//!
use {
    crate::{
        metadata::{DirectoryEntry, File, FileHandle},
        BlockNumber, BlockStorage, OpenFileMode, UberFileSystem,
    },
    failure::format_err,
    handlebars::Handlebars,
    log::info,
    serde::{Deserialize, Serialize},
    serde_json::json,
    std::{
        collections::HashMap,
        error::Error,
        io::prelude::*,
        net::{IpAddr, Ipv4Addr, SocketAddr, TcpListener},
        path::{Path, PathBuf},
        sync::{Arc, Mutex},
        thread::{spawn, JoinHandle},
    },
    warp::{path, Filter},
};

pub(crate) struct UfsRemoteServer<B: BlockStorage + 'static> {
    iofs: Arc<Mutex<UberFileSystem<B>>>,
    port: u16,
}

impl<B: BlockStorage> UfsRemoteServer<B> {
    pub(crate) fn new(
        iofs: Arc<Mutex<UberFileSystem<B>>>,
        port: u16,
    ) -> Result<(Self), failure::Error> {
        Ok(UfsRemoteServer { iofs, port })
    }

    pub(crate) fn start(server: UfsRemoteServer<B>) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            let template = include_str!("./static/index.html");

            let mut hb = Handlebars::new();
            hb.register_template_string("index.html", template)
                .expect("unable to register handlebars template");
            let hb = Arc::new(hb);

            let foo = hb.clone();
            let handlebars = move |with_template| render(with_template, foo.clone());
            let foo = hb.clone();
            let handlebars1 = move |with_template| render(with_template, foo.clone());

            let iofs = server.iofs.clone();
            let index_values = move || get_index_values(iofs.clone());

            let iofs = server.iofs.clone();
            let block_values = move |number| get_block_values(number, iofs.clone());

            let index = warp::get2()
                .and(warp::path::end())
                .map(index_values)
                .map(|a| WithTemplate {
                    name: "index.html",
                    value: a,
                })
                .map(handlebars);

            let block = path!("block" / BlockNumber)
                .map(block_values)
                .map(|a| WithTemplate {
                    name: "index.html",
                    value: a,
                })
                .map(handlebars1);

            let routes = warp::get2().and(index);

            warp::serve(routes).run(([0, 0, 0, 0], server.port));
            Ok(())
        })
    }
}

struct WithTemplate<T: Serialize> {
    name: &'static str,
    value: T,
}

fn render<T>(template: WithTemplate<T>, hbs: Arc<Handlebars>) -> impl warp::Reply
where
    T: Serialize,
{
    let rendered = hbs
        .render(template.name, &template.value)
        .unwrap_or_else(|err| err.description().to_owned());

    warp::reply::html(rendered)
}

fn get_index_values<B>(iofs: Arc<Mutex<UberFileSystem<B>>>) -> serde_json::value::Value
where
    B: BlockStorage,
{
    let guard = iofs.lock().expect("poisoned iofs lock");
    let manager = guard.block_manager();

    let fs_id = format!("{}", manager.id());
    let block_size = format!("{}", manager.block_size());

    json!({
        "iofs_id": fs_id,
        "block_size": block_size,
        "block_count": manager.block_count(),
        "free_blocks": manager.free_block_count(),
        "root_block": manager.root_block()
    })
}

fn get_block_values<B>(
    block: BlockNumber,
    iofs: Arc<Mutex<UberFileSystem<B>>>,
) -> serde_json::value::Value
where
    B: BlockStorage,
{
    json!({})
}
