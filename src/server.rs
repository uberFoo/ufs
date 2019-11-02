//! Embedded UFS Block Server
//!
//! A mounted UFS may also act as a block server for remote connections. That is implemented herein.
//!
use {
    crate::{uuid::UfsUuid, BlockNumber, BlockStorage, UberFileSystem},
    crossbeam::crossbeam_channel,
    futures::{future::Future, sync::oneshot},
    handlebars::{Context, Handlebars, Helper, JsonRender, Output, RenderContext, RenderError},
    serde::Serialize,
    serde_json::json,
    std::{
        error::Error,
        path::PathBuf,
        sync::{Arc, Mutex},
        thread::{spawn, JoinHandle},
    },
    warp::{path, Filter},
};

const CONTENT_LENGTH: u64 = 1024 * 16;

#[derive(Debug)]
pub(crate) enum IofsNetworkMessage {
    Get(IofsNetworkGetValue),
    Post(IofsNetworkJsonValue),
    Put(IofsNetworkJsonValue),
    Patch(IofsNetworkJsonValue),
    Delete(IofsNetworkJsonValue),
}

impl IofsNetworkMessage {
    pub(crate) fn route(&self) -> &str {
        match self {
            IofsNetworkMessage::Get(m) => &m.route,
            IofsNetworkMessage::Post(m) => &m.route,
            IofsNetworkMessage::Put(m) => &m.route,
            IofsNetworkMessage::Patch(m) => &m.route,
            IofsNetworkMessage::Delete(m) => &m.route,
        }
    }
}

#[derive(Debug)]
pub(crate) struct IofsNetworkGetValue {
    route: String,
    response_channel: Option<oneshot::Sender<String>>,
}

impl IofsNetworkGetValue {
    pub(crate) fn new(route: String, response_channel: oneshot::Sender<String>) -> Self {
        IofsNetworkGetValue {
            route,
            response_channel: Some(response_channel),
        }
    }

    pub(crate) fn respond(&mut self, value: String) {
        if let Some(channel) = self.response_channel.take() {
            channel.send(value);
        }
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }
}

#[derive(Debug)]
pub(crate) struct IofsNetworkJsonValue {
    route: String,
    body: serde_json::Value,
    response_channel: Option<oneshot::Sender<String>>,
}

impl IofsNetworkJsonValue {
    pub(crate) fn new(
        route: String,
        body: serde_json::Value,
        response_channel: oneshot::Sender<String>,
    ) -> Self {
        IofsNetworkJsonValue {
            route,
            body,
            response_channel: Some(response_channel),
        }
    }

    pub(crate) fn route(&self) -> &str {
        &self.route
    }

    pub(crate) fn json(&self) -> &serde_json::Value {
        &self.body
    }

    pub(crate) fn respond(&mut self, value: String) {
        if let Some(channel) = self.response_channel.take() {
            channel.send(value);
        }
    }
}

pub(crate) struct UfsRemoteServer<B: BlockStorage + 'static> {
    iofs: Arc<Mutex<UberFileSystem<B>>>,
    http_sender: crossbeam_channel::Sender<IofsNetworkMessage>,
    http_receiver: crossbeam_channel::Receiver<IofsNetworkMessage>,
    port: u16,
}

impl<B: BlockStorage> UfsRemoteServer<B> {
    pub(crate) fn new(iofs: Arc<Mutex<UberFileSystem<B>>>, port: u16) -> Self {
        let (http_sender, http_receiver) = crossbeam_channel::unbounded::<IofsNetworkMessage>();
        UfsRemoteServer {
            iofs,
            http_sender,
            http_receiver,
            port,
        }
    }

    pub(crate) fn get_http_receiver(&self) -> crossbeam_channel::Receiver<IofsNetworkMessage> {
        self.http_receiver.clone()
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

            let channel = server.http_sender.clone();
            let to_wasm_get = move |receiver| send_get_filter(receiver, channel.clone());

            let channel = server.http_sender.clone();
            let to_wasm_post =
                move |receiver, json| send_post_to_wasm(receiver, json, channel.clone());

            let channel = server.http_sender.clone();
            let to_wasm_put =
                move |receiver, json| send_put_to_wasm(receiver, json, channel.clone());

            let channel = server.http_sender.clone();
            let to_wasm_patch =
                move |receiver, json| send_patch_to_wasm(receiver, json, channel.clone());

            let channel = server.http_sender.clone();
            let to_wasm_delete =
                move |receiver, json| send_delete_to_wasm(receiver, json, channel.clone());

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

            let wasm_get = warp::get2()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .map(to_wasm_get);

            let wasm_post = warp::post2()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .and(warp::body::content_length_limit(CONTENT_LENGTH))
                .and(warp::body::json())
                .map(to_wasm_post);

            let wasm_put = warp::put2()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .and(warp::body::content_length_limit(CONTENT_LENGTH))
                .and(warp::body::json())
                .map(to_wasm_put);

            let wasm_patch = warp::patch()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .and(warp::body::content_length_limit(CONTENT_LENGTH))
                .and(warp::body::json())
                .map(to_wasm_patch);

            let wasm_delete = warp::delete2()
                .and(warp::path("wasm"))
                .and(warp::path::param())
                .and(warp::body::content_length_limit(CONTENT_LENGTH))
                .and(warp::body::json())
                .map(to_wasm_delete);

            let routes = index
                .or(block)
                .or(dir)
                .or(file)
                .or(wasm_get)
                .or(wasm_post)
                .or(wasm_put)
                .or(wasm_patch)
                .or(wasm_delete);

            let (addr, warp) = warp::serve(routes)
                .tls("src/certs/cert.pem", "src/certs/key.rsa")
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

fn send_get_filter(
    receiver: String,
    channel: crossbeam_channel::Sender<IofsNetworkMessage>,
) -> impl warp::Reply {
    let (tx, rx) = oneshot::channel::<String>();
    channel.send(IofsNetworkMessage::Get(IofsNetworkGetValue::new(
        receiver, tx,
    )));

    // let bar = rx.map(|result| warp::reply::html(result));
    // let result = rx.wait().unwrap();
    // let baz = warp::reply::html(result);
    // warp::reply::reply()

    rx.map(|result| warp::reply::html(result)).wait().unwrap()
    // rx.map(|result| warp::reply::html(result))
}

fn send_post_to_wasm(
    receiver: String,
    json: serde_json::Value,
    channel: crossbeam_channel::Sender<IofsNetworkMessage>,
) -> impl warp::Reply {
    let (tx, rx) = oneshot::channel::<String>();
    channel.send(IofsNetworkMessage::Post(IofsNetworkJsonValue::new(
        receiver, json, tx,
    )));
    rx.map(|result| warp::reply::html(result)).wait().unwrap()
}

fn send_put_to_wasm(
    receiver: String,
    json: serde_json::Value,
    channel: crossbeam_channel::Sender<IofsNetworkMessage>,
) -> impl warp::Reply {
    let (tx, rx) = oneshot::channel::<String>();
    channel.send(IofsNetworkMessage::Put(IofsNetworkJsonValue::new(
        receiver, json, tx,
    )));
    rx.map(|result| warp::reply::html(result)).wait().unwrap()
}

fn send_patch_to_wasm(
    receiver: String,
    json: serde_json::Value,
    channel: crossbeam_channel::Sender<IofsNetworkMessage>,
) -> impl warp::Reply {
    let (tx, rx) = oneshot::channel::<String>();
    channel.send(IofsNetworkMessage::Patch(IofsNetworkJsonValue::new(
        receiver, json, tx,
    )));
    rx.map(|result| warp::reply::html(result)).wait().unwrap()
}

fn send_delete_to_wasm(
    receiver: String,
    json: serde_json::Value,
    channel: crossbeam_channel::Sender<IofsNetworkMessage>,
) -> impl warp::Reply {
    let (tx, rx) = oneshot::channel::<String>();
    channel.send(IofsNetworkMessage::Delete(IofsNetworkJsonValue::new(
        receiver, json, tx,
    )));
    rx.map(|result| warp::reply::html(result)).wait().unwrap()
}
