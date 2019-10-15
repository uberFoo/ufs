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
            let index_tmpl = include_str!("./static/index.html");
            let block_tmpl = include_str!("./static/block.html");

            let mut hb = Handlebars::new();
            hb.register_template_string("index.html", index_tmpl)
                .expect("unable to register handlebars template");
            hb.register_template_string("block.html", block_tmpl)
                .expect("unable to register handlebars template");

            let hb = Arc::new(hb);
            let hb_clone = hb.clone();
            let handlebars = move |with_template| render(with_template, hb_clone.clone());
            let hb_clone = hb.clone();
            let handlebars1 = move |with_template| render(with_template, hb_clone.clone());

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
                    name: "block.html",
                    value: a,
                })
                .map(handlebars1);

            let routes = warp::get2().and(index.or(block));

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

    json!({
        "iofs_id": format!("{}", manager.id()),
        "block_size": format!("{}", manager.block_size()),
        "block_count": manager.block_count(),
        "free_blocks": manager.free_block_count(),
        "root_block": manager.root_block(),
    })
}

fn get_block_values<B>(
    block: BlockNumber,
    iofs: Arc<Mutex<UberFileSystem<B>>>,
) -> serde_json::value::Value
where
    B: BlockStorage,
{
    let guard = iofs.lock().expect("poisoned iofs lock");
    match guard.block_manager().get_block(block) {
        Some(block) => json!({
            "block_number": block.number(),
            "block_type": block.block_type(),
            "block_hash": format!("{:?}", block.hash()),
            "block_size": block.size(),
        }),
        None => json!({}),
    }
}
