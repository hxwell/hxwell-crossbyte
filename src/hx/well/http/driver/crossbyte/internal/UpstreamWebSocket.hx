package hx.well.http.driver.crossbyte.internal;

import crossbyte.core.CrossByte;
import crossbyte.events.Event;
import crossbyte.events.ProgressEvent;
import crossbyte.http.HTTPRequestHandler;
import crossbyte.io.ByteArray;
import crossbyte.net.Socket;
import crossbyte._internal.websocket.WebSocket as Framer;
import haxe.io.Bytes;

/** The pinned Crossbyte 1.0.1 HTTP -> RFC6455 ownership handoff. */
@:access(crossbyte.http.HTTPRequestHandler)
@:access(crossbyte.net.Socket)
@:access(crossbyte._internal.websocket.WebSocket)
@:access(crossbyte.core.CrossByte)
class UpstreamWebSocket {
    public static function take(handler:HTTPRequestHandler):{framer:Framer, tail:Bytes, cleanup:Void->Void} {
        var runtime = CrossByte.current();
        var origin = handler.__origin;
        var raw:crossbyte._internal.socket.FlexSocket = cast origin.__socket;
        if(raw == null || !origin.connected) throw new haxe.Exception("WebSocket client disconnected");
        var input = handler.__incomingBuffer;
        var tail = input == null ? Bytes.alloc(0) : (input : Bytes).sub(input.position, input.length - input.position);
        origin.removeEventListener(ProgressEvent.SOCKET_DATA, handler.__onData);
        runtime.deregisterSocket(raw);
        origin.__socket = null; // The framer now owns the descriptor, including TLS.
        handler.__receiveDeadline = 0;
        handler.__incomingBuffer = null;
        handler.__responded = true;
        var framer:Framer;
        try {
            framer = crossbyte._internal.websocket.WebSocket.fromAcceptedSocket(raw, runtime);
            // This descriptor's TLS handshake already completed in HTTPServer.
            if(framer.__handshaking) {
                runtime.removeEventListener(Event.TICK, framer.__tickSSLHandshakeListener);
                framer.__tickSSLHandshakeListener = null;
                framer.__handshaking = false;
                framer.__connected = true;
            }
            framer.__noteEndpoints();
        } catch(error:Dynamic) {
            raw.close();
            origin.__cleanSocket();
            origin.__announceClose();
            throw error;
        }
        return {framer:framer, tail:tail, cleanup:() -> {
            // Release HTTPServer's connection slot exactly when the WS closes.
            // __socket is null, so this cannot close a reused descriptor.
            origin.__cleanSocket();
            origin.__announceClose();
        }};
    }

    public static function open(framer:Framer, headers:Bytes, protocol:String, tail:Bytes):Void {
        framer.protocol = protocol;
        framer.__writeBytes(headers);
        framer.readyState = Framer.OPEN;
        framer.__startHeartbeat();
        if(tail.length > 0) {
            framer.__input.writeBytes(ByteArray.fromBytes(tail));
            framer.__input.position = 0;
            framer.__onData();
        }
    }
}
