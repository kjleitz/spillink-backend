// Adapted in EXTREMELY LARGE PART from Warp's `websockets_chat` example. Latest
// (probably) lives here:
// https://github.com/seanmonstar/warp/blob/master/examples/websockets_chat.rs
// Snapshot of what it looked like when I copied it:
// https://github.com/seanmonstar/warp/blob/25eedf650a7dd11b210b27d3cb7ed2d262af0781/examples/websockets_chat.rs

// #![deny(warnings)]
use std::collections::HashMap;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

use futures_util::{SinkExt, StreamExt, TryFutureExt};
use tokio::sync::{mpsc, RwLock};
use tokio_stream::wrappers::UnboundedReceiverStream;
use warp::ws::{Message, WebSocket};
use warp::Filter;

use redis::Commands;
use serde::{Deserialize, Serialize};

/// Our global unique user id counter.
// static NEXT_USER_ID: AtomicUsize = AtomicUsize::new(1);

// struct UserSocket {
//     username: String,
//     sender_connection: mpsc::UnboundedSender<Message>,
// }
/// Our state of currently connected user_sockets.
///
/// - Key is their uuid
/// - Value is a sender of `warp::ws::Message`
type UserSockets = Arc<RwLock<HashMap<String, mpsc::UnboundedSender<Message>>>>;
// type UserSockets = Arc<RwLock<HashMap<String, UserSocket>>>;

#[derive(Serialize, Deserialize)]
struct UpdateUserPayload {
    username: String,
}

#[derive(Serialize, Deserialize)]
struct WebsocketQueryParams {
    uuid: String,
}

#[tokio::main]
async fn main() {
    // Keep track of all connected user_sockets, key is String, value
    // is a UserDetails struct containing the websocket sender.
    let user_sockets = UserSockets::default();
    // Turn our "state" into a new Filter...
    let user_sockets = warp::any().map(move || user_sockets.clone());

    // GET /ws?uuid=:uuid -> websocket upgrade
    let ws_route = warp::path("ws")
        .and(warp::query::<WebsocketQueryParams>())
        .and(warp::ws()) // The `ws()` filter will prepare Websocket handshake...
        .and(user_sockets)
        .map(
            |query: WebsocketQueryParams, ws: warp::ws::Ws, user_sockets| {
                // This will call our function if the handshake succeeds.
                let uuid = query.uuid.clone();
                ws.on_upgrade(move |socket| on_user_connected(socket, uuid, user_sockets))
            },
        );

    // GET / -> index html
    let index_route = warp::path::end().map(|| warp::reply::html(INDEX_HTML));

    // PATCH /users/:uuid -> reflects the payload on success
    let update_user_route = warp::patch()
        .and(warp::path("users"))
        .and(warp::path::param())
        .and(warp::body::content_length_limit(1024 * 16))
        .and(warp::body::json())
        .map(|uuid: String, payload: UpdateUserPayload| {
            let mut redis_conn = get_redis_connection();
            set_user_field(&mut redis_conn, &uuid, "username", &payload.username);
            warp::reply::json(&payload)
        });

    let routes = index_route.or(update_user_route).or(ws_route);

    warp::serve(routes).run(([127, 0, 0, 1], 3030)).await;
}

fn get_redis_connection() -> redis::Connection {
    redis::Client::open("redis://127.0.0.1:6379")
        .expect("Cannot open Redis at redis://127.0.0.1:6379")
        .get_connection()
        .expect("Failed to connect to Redis")
}

fn user_key(my_uuid: &str) -> String {
    format!("user:{}", my_uuid)
}

fn set_user_field<V: redis::ToRedisArgs>(
    conn: &mut redis::Connection,
    my_uuid: &str,
    field: &str,
    value: V,
) {
    let key = user_key(my_uuid);
    conn.hset(key, field, value).unwrap()
}

fn set_user_field_if_not_set<V: redis::ToRedisArgs>(
    conn: &mut redis::Connection,
    my_uuid: &str,
    field: &str,
    value: V,
) {
    let key = user_key(my_uuid);
    conn.hset_nx(key, field, value).unwrap()
}

fn get_user_field<V: redis::FromRedisValue>(
    conn: &mut redis::Connection,
    my_uuid: &str,
    field: &str,
) -> Option<V> {
    let key = user_key(my_uuid);
    conn.hget(key, field).ok()
}

async fn on_user_connected(ws: WebSocket, my_uuid: String, user_sockets: UserSockets) {
    // Use a counter to assign a new unique ID for this user.
    // let my_uuid = NEXT_USER_ID.fetch_add(1, Ordering::Relaxed);

    println!("new chat user {}", my_uuid);

    let mut redis_conn = get_redis_connection();
    let username = user_key(&my_uuid);

    set_user_field_if_not_set(&mut redis_conn, &my_uuid, "username", &username);
    let username = get_user_field(&mut redis_conn, &my_uuid, "username").unwrap_or(username);

    // Split the socket into a sender and receiver of messages.
    let (mut user_ws_tx, mut user_ws_rx) = ws.split();

    // Use an unbounded channel to handle buffering and flushing of messages
    // to the websocket...
    let (tx, rx) = mpsc::unbounded_channel();
    let mut rx = UnboundedReceiverStream::new(rx);

    tokio::task::spawn(async move {
        while let Some(message) = rx.next().await {
            user_ws_tx
                .send(message)
                .unwrap_or_else(|e| {
                    eprintln!("websocket send error: {}", e);
                })
                .await;
        }
    });

    // Save the sender in our list of connected user_sockets.
    // let user_socket = UserSocket {
    //     username: username.clone(),
    //     sender_connection: tx,
    // };
    user_sockets.write().await.insert(my_uuid.clone(), tx);

    // Return a `Future` that is basically a state machine managing
    // this specific user's connection.

    // Every time the user sends a message, broadcast it to
    // all other user_sockets...
    while let Some(result) = user_ws_rx.next().await {
        let msg = match result {
            Ok(msg) => msg,
            Err(e) => {
                eprintln!("websocket error (uid={}): {}", my_uuid, e);
                break;
            }
        };

        on_user_message(&my_uuid, &username, msg, &user_sockets, &mut redis_conn).await;
    }

    // user_ws_rx stream will keep processing as long as the user stays
    // connected. Once they disconnect, then...
    on_user_disconnected(&my_uuid, &user_sockets).await;
}

#[derive(Deserialize, Serialize)]
struct SocketMessageReceived {
    message_type: String,
    data: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
struct SocketMessageToSend {
    message_type: String,
    from_username: String,
    data: serde_json::Value,
}

#[derive(Deserialize, Serialize)]
struct TextMessageData {
    text: String,
}

#[derive(Deserialize, Serialize)]
struct SetUsernameData {
    username: String,
}

async fn on_user_message(
    my_uuid: &str,
    username: &str,
    message: Message,
    user_sockets: &UserSockets,
    conn: &mut redis::Connection,
) {
    // Skip any non-Text messages...
    let message: SocketMessageReceived = if let Ok(message_string) = message.to_str() {
        serde_json::from_str(message_string).unwrap()
    } else {
        return;
    };

    match message.message_type.as_str() {
        "text" => {
            let msg_data: TextMessageData = serde_json::from_value(message.data).unwrap();
            on_message_type_text(msg_data, my_uuid, username, user_sockets).await;
        }
        "set:username" => {
            let msg_data: SetUsernameData = serde_json::from_value(message.data).unwrap();
            on_message_type_set_username(msg_data, my_uuid, user_sockets, conn).await;
        }
        _ => println!("Unknown message type: {}", message.message_type),
    };
}

async fn send_to_all_other_users(
    msg_object: SocketMessageToSend,
    my_uuid: &str,
    user_sockets: &UserSockets,
) {
    let msg = serde_json::to_string(&msg_object).unwrap();

    for (uid, tx) in user_sockets.read().await.iter() {
        if my_uuid != uid {
            if let Err(_disconnected) = tx.send(Message::text(msg.clone())) {
                // If the user is disconnected, the `on_user_disconnected` logic
                // should clean it up... nothing more to do here.
            }
        }
    }
}

async fn send_to_self(msg_object: SocketMessageToSend, my_uuid: &str, user_sockets: &UserSockets) {
    let msg = serde_json::to_string(&msg_object).unwrap();

    if let Some(tx) = user_sockets.read().await.get(my_uuid) {
        if let Err(_disconnected) = tx.send(Message::text(msg)) {
            // If the user is disconnected, the `on_user_disconnected` logic
            // should clean it up... nothing more to do here.
        }
    }
}

async fn on_message_type_text(
    msg_data: TextMessageData,
    my_uuid: &str,
    username: &str,
    user_sockets: &UserSockets,
) {
    println!("message type 'text': {}", msg_data.text);

    let reflected_msg = SocketMessageToSend {
        message_type: String::from("text"),
        from_username: username.to_string(),
        data: serde_json::to_value(msg_data).unwrap(),
    };

    send_to_all_other_users(reflected_msg, my_uuid, user_sockets).await;
}

async fn on_message_type_set_username(
    msg_data: SetUsernameData,
    my_uuid: &str,
    user_sockets: &UserSockets,
    conn: &mut redis::Connection,
) {
    let username = msg_data.username.clone();

    println!("message type 'set:username': {}", &username);
    set_user_field(conn, my_uuid, "username", &username);

    let reflected_msg = SocketMessageToSend {
        message_type: String::from("set:username"),
        from_username: username,
        data: serde_json::to_value(msg_data).unwrap(),
    };

    send_to_self(reflected_msg, my_uuid, user_sockets).await;
}

async fn on_user_disconnected(my_uuid: &str, user_sockets: &UserSockets) {
    println!("goodbye user: {}", my_uuid);

    // Stream closed up, so remove from the user list
    user_sockets.write().await.remove(my_uuid);
}

static INDEX_HTML: &str = r#"<!DOCTYPE html>
<html lang="en">
    <head>
        <title>Warp Chat</title>
    </head>
    <body>
        <h1>Warp chat</h1>
        <div id="chat">
            <p><em>Connecting...</em></p>
        </div>
        <input type="text" id="text" />
        <button type="button" id="send">Send</button>
        <script type="text/javascript">
        const chat = document.getElementById('chat');
        const text = document.getElementById('text');
        const uri = 'ws://' + location.host + '/chat';
        const ws = new WebSocket(uri);

        function message(data) {
            const line = document.createElement('p');
            line.innerText = data;
            chat.appendChild(line);
        }

        ws.onopen = function() {
            chat.innerHTML = '<p><em>Connected!</em></p>';
        };

        ws.onmessage = function(msg) {
            message(msg.data);
        };

        ws.onclose = function() {
            chat.getElementsByTagName('em')[0].innerText = 'Disconnected!';
        };

        send.onclick = function() {
            const msg = text.value;
            ws.send(msg);
            text.value = '';

            message('<You>: ' + msg);
        };
        </script>
    </body>
</html>
"#;
