//! Embedded UFS Block Server
//!
//! A mounted UFS may also act as a block server for remote connections. That is implemented herein.
//!
use {
    crate::{
        metadata::{DirectoryEntry, File, FileHandle},
        uuid::UfsUuid,
        wasm::{IofsMessage, IofsNetworkMessage, IofsNetworkValue, RuntimeManagerMsg},
        BlockNumber, BlockStorage, OpenFileMode, UberFileSystem,
    },
    crossbeam::crossbeam_channel,
    failure::format_err,
    futures::sync::oneshot,
    handlebars::{Context, Handlebars, Helper, JsonRender, Output, RenderContext, RenderError},
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
    wasm_channel: crossbeam_channel::Sender<RuntimeManagerMsg>,
    port: u16,
}

impl<B: BlockStorage> UfsRemoteServer<B> {
    pub(crate) fn new(
        iofs: Arc<Mutex<UberFileSystem<B>>>,
        wasm_channel: crossbeam_channel::Sender<RuntimeManagerMsg>,
        port: u16,
    ) -> Self {
        UfsRemoteServer {
            iofs,
            wasm_channel,
            port,
        }
    }

    pub(crate) fn start(
        server: UfsRemoteServer<B>,
        stop_signal: oneshot::Receiver<()>,
    ) -> JoinHandle<Result<(), failure::Error>> {
        spawn(move || {
            let index_tmpl = include_str!("./static/index.html");
            let dir_tmpl = include_str!("./static/dir.html");
            let file_tmpl = include_str!("./static/file.html");
            let block_tmpl = include_str!("./static/block.html");

            let mut hb = Handlebars::new();
            hb.register_template_string("index.html", index_tmpl)
                .expect("unable to register handlebars template");
            hb.register_template_string("dir.html", dir_tmpl)
                .expect("unable to register handlebars template");
            hb.register_template_string("file.html", file_tmpl)
                .expect("unable to register handlebars template");
            hb.register_template_string("block.html", block_tmpl)
                .expect("unable to register handlebars template");
            hb.register_helper("dir_entry_format", Box::new(dir_entry_format));
            hb.register_helper("block_format", Box::new(block_format));

            let hb = Arc::new(hb);
            let hb_clone = hb.clone();
            let handlebars_index = move |with_template| render(with_template, hb_clone.clone());
            let hb_clone = hb.clone();
            let handlebars_block = move |with_template| render(with_template, hb_clone.clone());
            let hb_clone = hb.clone();
            let handlebars_dir = move |with_template| render(with_template, hb_clone.clone());
            let hb_clone = hb.clone();
            let handlebars_file = move |with_template| render(with_template, hb_clone.clone());

            let iofs = server.iofs.clone();
            let index_values = move || get_index_values(iofs.clone());

            let iofs = server.iofs.clone();
            let dir_values = move |path| get_dir_values(path, iofs.clone());

            let iofs = server.iofs.clone();
            let file_values = move |path, name| get_file_values(path, name, iofs.clone());

            let iofs = server.iofs.clone();
            let block_values = move |number| get_block_values(number, iofs.clone());

            let channel = server.wasm_channel.clone();
            let to_wasm = move |receiver, json| send_json_to_wasm(receiver, json, channel.clone());

            let index = warp::get2()
                .and(warp::path::end())
                .map(index_values)
                .map(|a| WithTemplate {
                    name: "index.html",
                    value: a,
                })
                .map(handlebars_index);

            let block = path!("block" / BlockNumber)
                .map(block_values)
                .map(|a| WithTemplate {
                    name: "block.html",
                    value: a,
                })
                .map(handlebars_block);

            let dir = path!("dir" / String)
                .map(dir_values)
                .map(|a| WithTemplate {
                    name: "dir.html",
                    value: a,
                })
                .map(handlebars_dir);

            let file = path!("file" / String / String)
                .map(file_values)
                .map(|a| WithTemplate {
                    name: "file.html",
                    value: a,
                })
                .map(handlebars_file);

            let wasm_post = warp::post2()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .and(warp::body::content_length_limit(1024 * 16))
                .and(warp::body::json())
                .map(to_wasm);

            let routes = index.or(block).or(dir).or(file).or(wasm_post);

            let (addr, warp) = warp::serve(routes)
                // .tls("src/certs/cert.pem", "src/certs/cern.rsa")
                .bind_with_graceful_shutdown(([0, 0, 0, 0], server.port), stop_signal);

            hyper::rt::run(warp);

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

fn dir_entry_format(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> Result<(), RenderError> {
    let entry = h.param(0).ok_or(RenderError::new("param 0 is required"))?;
    let json = entry.value();
    let rendered = if json["type"] == "dir" {
        format!(
            "<li><a href=\"/dir/{}\">{}</a></li>",
            json["id"].render(),
            json["name"].render()
        )
    } else {
        format!(
            "<li><a href=\"/file/{}/{}\">{}</a></li>",
            json["id"].render(),
            json["name"].render(),
            json["name"].render()
        )
    };
    out.write(rendered.as_ref())?;
    Ok(())
}

fn block_format(
    h: &Helper,
    _: &Handlebars,
    _: &Context,
    _: &mut RenderContext,
    out: &mut dyn Output,
) -> Result<(), RenderError> {
    let block = h.param(0).ok_or(RenderError::new("param 0 is required"))?;
    let json = block.value();
    let rendered = format!(
        "<a href=\"/block/{}\">{}</a>,",
        json.render(),
        json.render()
    );
    out.write(rendered.as_ref())?;
    Ok(())
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
        "root_dir_id": manager.metadata().root_directory().id().to_string(),
        // "block_map": format!("{:?}", manager.map()),
        "metadata": format!("{:#?}", manager.metadata()),
    })
}

fn get_dir_values<B>(
    dir_id: String,
    iofs: Arc<Mutex<UberFileSystem<B>>>,
) -> serde_json::value::Value
where
    B: BlockStorage,
{
    use std::cmp::Ordering;

    let guard = iofs.lock().expect("poisoned iofs lock");
    let metadata = guard.block_manager().metadata();

    let mut dir_ufsid: UfsUuid = dir_id.clone().into();
    if let Ok(dir) = metadata.get_directory(dir_ufsid) {
        let mut tree = vec![];
        // Add files and directories under this one for display.
        for (name, entry) in dir.entries() {
            tree.push(json!({
                "type": if entry.is_dir(){ "dir" } else { "file"},
                "name": name,
                "id": entry.id().to_string(),
                "owner": entry.owner().to_string(),
            }));
        }

        // Sort lexicographically, with directories first.
        tree.sort_unstable_by(|a, b| {
            if a["type"].as_str() == Some("dir") {
                if b["type"].as_str() == Some("dir") {
                    if a["name"].as_str() < b["name"].as_str() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                } else {
                    Ordering::Less
                }
            } else {
                if b["type"].as_str() == Some("dir") {
                    Ordering::Greater
                } else {
                    if a["name"].as_str() < b["name"].as_str() {
                        Ordering::Less
                    } else {
                        Ordering::Greater
                    }
                }
            }
        });

        // Build a path to this directory for display
        let mut dir_path_components = vec![];
        let mut parent_id_option = dir.parent_id();
        while let Some(parent_id) = parent_id_option {
            if let Ok(parent_dir) = metadata.get_directory(parent_id) {
                for (name, entry) in parent_dir.entries() {
                    if entry.id() == dir_ufsid {
                        dir_path_components.push(name.to_string());
                        break;
                    }
                }
                dir_ufsid = parent_dir.id();
                parent_id_option = parent_dir.parent_id();
            }
        }
        dir_path_components.push("/".to_string());

        let dir_path: PathBuf = dir_path_components.iter().rev().collect();

        json!({
            "name": dir_path.to_str(),
            "id": dir_id,
            "files": tree,
        })
    } else {
        json!({
            "name": "invalid directory id"
        })
    }
}

fn get_file_values<B>(
    file_id: String,
    file_name: String,
    iofs: Arc<Mutex<UberFileSystem<B>>>,
) -> serde_json::value::Value
where
    B: BlockStorage,
{
    let guard = iofs.lock().expect("poisoned iofs lock");
    let metadata = guard.block_manager().metadata();

    let file_ufsid: UfsUuid = file_id.clone().into();
    if let Ok(file) = metadata.get_file_metadata(file_ufsid) {
        let latest = file.get_latest();

        json!({
            "name": file_name,
            "id": file_id,
            "size": latest.size(),
            "blocks": latest.blocks()
        })
    } else {
        json!({
            "name": "invalid file id"
        })
    }
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

fn send_json_to_wasm(
    receiver: String,
    json: serde_json::Value,
    channel: crossbeam_channel::Sender<RuntimeManagerMsg>,
) -> impl warp::Reply {
    channel.send(RuntimeManagerMsg::IofsMessage(IofsMessage::NetworkMessage(
        IofsNetworkMessage::Post(IofsNetworkValue::new(receiver, json)),
    )));
    warp::reply::reply()
}
