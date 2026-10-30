package features;

import hx.well.websocket.socketio.SocketIOHandler;
import hx.well.websocket.socketio.SocketIOSession;

class ChatSocket extends SocketIOHandler {
    public function onConnection(socket:SocketIOSession):Void {
        socket.join("lobby");
        socket.on("echo", data -> socket.emit("reply", data));
        socket.on("room", data -> toRoom("lobby").emit("room-reply", data));
        socket.emit("welcome", "connected");
    }
    public function onDisconnect(socket:SocketIOSession):Void {}
}
