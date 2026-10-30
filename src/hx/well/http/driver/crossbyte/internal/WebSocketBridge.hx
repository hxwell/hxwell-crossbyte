package hx.well.http.driver.crossbyte.internal;

import crossbyte.core.CrossByte;
import crossbyte.http.HTTPRequestHandler;
import crossbyte.io.ByteArray;
import crossbyte._internal.websocket.WebSocket as Framer;
import haxe.Exception;
import haxe.crypto.Base64;
import haxe.crypto.Sha1;
import haxe.io.Bytes;
import hx.well.http.HttpSyntax;
import hx.well.http.Request;
import hx.well.http.RequestStatic;
import hx.well.http.Response;
import hx.well.http.ResponseStatic;
import hx.well.websocket.AbstractWebSocketHandler;
import hx.well.websocket.WebSocketSession;
import sys.thread.Deque;
import sys.thread.Lock;
import sys.thread.Mutex;

@:access(hx.well.http.Response)
class WebSocketBridge {
    var runtime:CrossByte;
    var native:Framer;
    var handler:AbstractWebSocketHandler;
    var request:Request;
    var config:CrossbyteDriverConfig;
    var session:WebSocketSession;
    var inbox = new Deque<WsEvent>();
    var mutex = new Mutex();
    var closed = false;
    var launched = false;
    var inBytes = 0;
    var inCount = 0;
    var outBytes = 0;
    var outCount = 0;
    var cleanup:Void->Void;
    var onClosed:WebSocketBridge->Void;
    var tail:Bytes;
    var head:{bytes:Bytes, protocol:String};

    /** Validate before releasing the HTTP context, so refusals are HTTP errors. */
    public static function handshake(request:Request, response:Response):{bytes:Bytes, protocol:String} {
        if(request.method != "GET" || request.version != "HTTP/1.1" || request.header("Host", "") == ""
            || request.header("Transfer-Encoding") != null
            || hx.well.http.RequestParser.contentLength(request.header("Content-Length")) != 0)
            ResponseStatic.abort(400);
        var connection = HttpSyntax.lowerAscii(request.header("Connection", "")).split(",").map(StringTools.trim);
        var key = request.header("Sec-WebSocket-Key", "");
        if(!connection.contains("upgrade") || !~/^[A-Za-z0-9+\/]{22}==$/.match(key)) ResponseStatic.abort(400);
        try { if(Base64.decode(key).length != 16) ResponseStatic.abort(400); }
        catch(_:Dynamic) { ResponseStatic.abort(400); }
        if(request.header("Sec-WebSocket-Version") != "13") {
            ResponseStatic.header("Sec-WebSocket-Version", "13");
            ResponseStatic.abort(426);
        }
        response.validate();
        var protocol:String = null;
        for(name => value in response.headers)
            if(HttpSyntax.lowerAscii(name) == "sec-websocket-protocol") protocol = value;
        if(protocol != null && (!HttpSyntax.isToken(protocol)
            || !request.header("Sec-WebSocket-Protocol", "").split(",").map(StringTools.trim).contains(protocol)))
            throw new Exception("Selected WebSocket subprotocol was not offered");
        var accept = Base64.encode(Sha1.make(Bytes.ofString(key + "258EAFA5-E914-47DA-95CA-C5AB0DC85B11")));
        var wire = new StringBuf();
        wire.add("HTTP/1.1 101 Switching Protocols\r\nUpgrade: websocket\r\nConnection: Upgrade\r\nSec-WebSocket-Accept: " + accept + "\r\n");
        for(name => value in response.headers) {
            var lower = HttpSyntax.lowerAscii(name);
            if(["connection", "upgrade", "content-length", "transfer-encoding", "content-type", "content-encoding", "sec-websocket-accept", "sec-websocket-extensions"].contains(lower)) continue;
            wire.add(name + ": " + value + "\r\n");
        }
        for(cookie in response.cookies) wire.add("Set-Cookie: " + cookie.toString() + "\r\n");
        wire.add("\r\n");
        return {bytes:Bytes.ofString(wire.toString()), protocol:protocol};
    }

    /** On the connection's owning runtime, after the HxWell middleware chain. */
    public function new(http:HTTPRequestHandler, request:Request, handler:AbstractWebSocketHandler,
        config:CrossbyteDriverConfig, head:{bytes:Bytes, protocol:String}, onClosed:WebSocketBridge->Void) {
        this.request = request;
        this.handler = handler;
        this.config = config;
        this.onClosed = onClosed;
        this.head = head;
        runtime = CrossByte.current();
        var taken = UpstreamWebSocket.take(http);
        native = taken.framer;
        cleanup = taken.cleanup;
        tail = taken.tail;
        native.maxMessageSize = config.maxWebSocketMessageSize;
        native.maxOutputBufferSize = config.maxWebSocketOutputBytes;
        native.closeTimeout = config.webSocketCloseTimeoutSeconds;
        native.onclose = event -> finished(event.code, event.reason ?? "");
        native.onerror = event -> inbox.add(Failed(event.text ?? "WebSocket error"));
        native.__onMessage = (bytes, text) -> {
            var size = bytes.length;
            mutex.acquire();
            var accept = !closed && inCount < config.maxWebSocketQueuedMessages && size <= config.maxWebSocketQueuedBytes - inBytes;
            if(accept) { inCount++; inBytes += size; }
            mutex.release();
            if(!accept) { native.close(1013, "Inbound queue full"); return; }
            var copy:Bytes = (bytes : Bytes).sub(0, size);
            inbox.add(text ? Text(copy.toString(), size) : Binary(copy));
        };
        session = new LimitedSession(hx.well.internal.SecureToken.generate(16), () -> close(1000), request.queries,
            reserveOutput, () -> close(1013));
        handler.addSession(session);
        inbox.add(Opened);
    }

    public function open():Void {
        UpstreamWebSocket.open(native, head.bytes, head.protocol, tail);
        head = null;
        tail = null;
    }

    function reserveOutput(size:Int):Bool {
        mutex.acquire();
        var accept = !closed && outCount < config.maxWebSocketQueuedMessages && size <= config.maxWebSocketQueuedBytes - outBytes;
        if(accept) { outCount++; outBytes += size; }
        mutex.release();
        return accept;
    }

    function finished(code:Int, reason:String):Void {
        mutex.acquire();
        var first = !closed;
        closed = true;
        mutex.release();
        if(!first) return;
        cleanup();
        session.shutdown();
        inbox.add(Closed(code, reason));
        onClosed(this);
    }

    public function close(code:Int = 1001):Void {
        runtime.post(() -> native.close(code, ""));
    }

    /** Started only after HTTP middleware disposal and thread-local cleanup. */
    public function start():Void {
        if(launched) return;
        launched = true;
        worker(receiveLoop);
        worker(sendLoop);
    }

    static function worker(work:Void->Void):Void {
        #if jvm
        jvm.thread.VirtualThread.create(work).start();
        #else
        sys.thread.Thread.create(work);
        #end
    }

    function receiveLoop():Void {
        while(true) {
            var event = inbox.pop(true);
            var ended = false;
            RequestStatic.set(request);
            ResponseStatic.reset();
            try {
                switch(event) {
                    case Opened: handler.onOpen(session);
                    case Text(value, size): consumed(size); handler.onMessage(session, value);
                    case Binary(bytes): consumed(bytes.length); handler.onBinary(session, bytes);
                    case Failed(message): handler.onError(session, new Exception(message));
                    case Closed(code, reason):
                        ended = true;
                        handler.removeSession(session.id);
                        handler.onClose(session, code, reason);
                }
            } catch(error:Dynamic) {
                try { handler.onError(session, new Exception(Std.string(error))); } catch(_:Dynamic) {}
            }
            if(!clearScope()) close(1011);
            if(ended) return;
        }
    }

    function consumed(size:Int):Void {
        mutex.acquire(); inCount--; inBytes -= size; mutex.release();
    }

    function sendLoop():Void {
        while(true) {
            var message = session.queue.pop(-1);
            if(message == null) continue;
            var bytes:Bytes;
            var text:Bool;
            var callback:Void->Void;
            switch(message) {
                case Shutdown: return;
                case Text(value, done): bytes = Bytes.ofString(value); text = true; callback = done;
                case Binary(value, done): bytes = value; text = false; callback = done;
            }
            var sent = new Lock();
            var failure:Dynamic = null;
            if(!runtime.post(() -> {
                try {
                    if(native.readyState != Framer.OPEN) throw new Exception("WebSocket is closed");
                    if(text) native.sendString(bytes.toString()); else native.sendBytes(ByteArray.fromBytes(bytes));
                } catch(error:Dynamic) { failure = error; }
                sent.release();
            })) return;
            var delivered = sent.wait(config.writeTimeoutSeconds);
            mutex.acquire(); outCount--; outBytes -= bytes.length; mutex.release();
            if(!delivered || failure != null) { close(1011); return; }
            if(callback != null) {
                RequestStatic.set(request);
                ResponseStatic.reset();
                var failed = false;
                try { callback(); } catch(_:Dynamic) { failed = true; }
                if(!clearScope()) failed = true;
                if(failed) { close(1011); return; }
            }
        }
    }

    function clearScope():Bool {
        var success = true;
        try { hx.well.database.Connection.free(); } catch(_:Dynamic) { success = false; }
        RequestStatic.reset();
        ResponseStatic.clear();
        return success;
    }
}

private enum WsEvent {
    Opened;
    Text(value:String, size:Int);
    Binary(bytes:Bytes);
    Failed(message:String);
    Closed(code:Int, reason:String);
}

private class LimitedSession extends WebSocketSession {
    var reserve:Int->Bool;
    var overflow:Void->Void;
    public function new(id:String, close:Void->Void, query:Map<String, String>, reserve:Int->Bool, overflow:Void->Void) {
        super(id, close, query);
        this.reserve = reserve;
        this.overflow = overflow;
    }
    override public function send(value:String, ?callback:Void->Void):Void {
        if(reserve(Bytes.ofString(value).length)) super.send(value, callback); else overflow();
    }
    override public function sendBinary(value:Bytes, ?callback:Void->Void):Void {
        if(reserve(value.length)) super.sendBinary(value.sub(0, value.length), callback); else overflow();
    }
}
