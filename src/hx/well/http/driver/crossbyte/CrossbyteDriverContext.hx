package hx.well.http.driver.crossbyte;

import crossbyte.http.HTTPRequestHandler;
import haxe.Exception;
import haxe.io.Bytes;
import haxe.io.BytesInput;
import haxe.io.Input;
import haxe.io.Output;
import hx.well.facades.Config;
import hx.well.http.AsyncResponse;
import hx.well.http.Request;
import hx.well.http.RequestBodyParser;
import hx.well.http.RequestParser;
import hx.well.http.Response;
import hx.well.http.ResponseStatic;
import hx.well.http.driver.IDriverContext;
import hx.well.http.driver.crossbyte.internal.ResponseBridge;
import hx.well.http.driver.crossbyte.internal.ResponseHead;
import hx.well.http.driver.crossbyte.internal.UpstreamAccess;
import hx.well.http.driver.crossbyte.internal.DeflateWriter;
import hx.well.http.driver.crossbyte.internal.WebSocketBridge;
import hx.well.websocket.AbstractWebSocketHandler;

@:access(hx.well.http.Request)
@:access(hx.well.http.Response)
class CrossbyteDriverContext implements IDriverContext {
    var request:Request;
    var bridge:ResponseBridge;
    var baseResponse:Response;
    var deferredResponse:Response;
    var responseStarted:Bool = false;
    var closed:Bool = false;
    var headerSize:Int;
    var targetLength:Int;
    var body:Bytes;
    var requestInput:Input;
    var responseOutput:Output;
    var encoder:DeflateWriter;
    var writeChunkSize:Int = ResponseBridge.CHUNK_SIZE;
    var driver:CrossbyteDriver;
    var webSocket:WebSocketBridge;

    @:isVar public var input(get, null):Input;
    public function get_input():Input { return requestInput; }
    @:isVar public var output(get, null):Output;
    public function get_output():Output {
        if(!responseStarted) throw new Exception("Call beginWrite() before accessing output");
        return responseOutput;
    }

    /** Called on Crossbyte's owning runtime; no native handler state escapes. */
    public function new(handler:HTTPRequestHandler, timeout:Float, onComplete:Void->Void, driver:CrossbyteDriver) {
        this.driver = driver;
        request = new Request();
        request.method = handler.method;
        @:bypassAccessor request.path = handler.requestPath;
        request.queries = RequestParser.parseQueryString("?" + handler.queryString);
        request.headers = UpstreamAccess.headers(handler);
        request.cookies = handler.getAllCookies();
        request.host = handler.getHeader("Host") ?? "";
        request.ip = handler.remoteAddress;
        request.version = UpstreamAccess.version(handler);
        request.context = this;
        targetLength = handler.requestPath.length + handler.queryString.length;
        headerSize = request.method.length + targetLength + request.version.length + 4;
        for(key => value in request.headers) headerSize += Bytes.ofString(key + ": " + value).length + 2;
        var received:Bytes = handler.requestBody;
        body = received.sub(0, received.length);
        requestInput = new BytesInput(body);
        responseOutput = new CrossbyteOutput(this);
        bridge = new ResponseBridge(handler, timeout, onComplete);
    }

    function buildRequest():Request {
        baseResponse = ResponseStatic.get();
        var pathLimit:Int = Config.get("http.max_path_length");
        var headerLimit:Int = Config.get("http.max_buffer");
        var bodyLimit:Int = Config.get("http.max_content_length");
        if(pathLimit > 0 && targetLength > pathLimit) ResponseStatic.abort(414);
        if(headerLimit > 0 && headerSize > headerLimit) ResponseStatic.abort(431);
        if(bodyLimit > 0 && body.length > bodyLimit) ResponseStatic.abort(413);
        return request;
    }

    function parseBody():Void {
        // Crossbyte has already decoded chunked/compressed bodies. Re-reading
        // Content-Length here would truncate them or wait for nonexistent bytes.
        request.bodyBytes = body;
        request._parsedBody = RequestBodyParser.fromBodyBytes(body);
        requestInput = new BytesInput(Bytes.alloc(0));
    }

    function inheritedResponse():Response {
        return deferredResponse ?? ResponseStatic.get() ?? baseResponse ?? new Response();
    }

    public function writeResponse(response:Response):Void {
        if(response is AsyncResponse) {
            if(!closed) deferredResponse = inheritedResponse();
            return;
        }
        if(closed || responseStarted) throw new Exception("HTTP response already started or closed");
        if(response == null) return;
        var head = new ResponseHead(response, inheritedResponse(), request.header("Accept-Encoding", ""));
        var source = response.toInput();
        try {
            var bodiless = head.status == 204 || head.status == 304;
            // A body that fits in one slice, the usual case, is read here and
            // handed to the runtime whole, without waiting: one hand-off and
            // one write with its length, rather than a round trip each for
            // the head, the body and the end.
            var prefix:Bytes = null;
            if(head.deflate == null) {
                var whole:Bytes;
                try { whole = bodiless ? Bytes.alloc(0) : readUpTo(source, ResponseBridge.CHUNK_SIZE); }
                catch(error:Dynamic) {
                    // As when the body fails partway through a stream: the
                    // head goes out and the response is then cut short.
                    bridge.begin(head);
                    responseStarted = true;
                    throw error;
                }
                if(bodiless || whole.length <= ResponseBridge.CHUNK_SIZE) {
                    source.close();
                    responseStarted = true;
                    closed = true;
                    bridge.respond(head, whole);
                    release();
                    if(response.after != null) response.after();
                    return;
                }
                prefix = whole;
            }
            bridge.begin(head);
            responseStarted = true;
            setEncoder(head);
            if(request.method != "HEAD" && !bodiless) {
                if(prefix != null) writeFullBytes(prefix, 0, prefix.length);
                copyInput(source, writeChunkSize);
            }
            source.close();
            close();
        } catch(error:Dynamic) {
            try { source.close(); } catch(_:Dynamic) {}
            if(responseStarted) abort();
            throw error;
        }
        if(response.after != null) response.after();
    }

    public function beginWrite():Void {
        if(closed) throw new Exception("HTTP response is closed");
        if(responseStarted) return;
        var base = inheritedResponse();
        var head = new ResponseHead(base, new Response(), request.header("Accept-Encoding", ""));
        bridge.begin(head);
        responseStarted = true;
        setEncoder(head);
    }

    function setEncoder(head:ResponseHead):Void {
        if(head.deflate != null && request.method != "HEAD" && head.status != 204 && head.status != 304) {
            encoder = new DeflateWriter(head.deflate.level, bridge.write);
            writeChunkSize = Std.int(Math.min(head.deflate.chunkSize, ResponseBridge.CHUNK_SIZE));
        }
    }

    function sendBytes(bytes:Bytes, pos:Int, length:Int):Void {
        if(encoder == null) bridge.write(bytes, pos, length); else encoder.write(bytes, pos, length);
    }

    public function writeInput(source:Input, ?bufsize:Int):Void {
        beginWrite();
        copyInput(source, bufsize == null ? ResponseBridge.CHUNK_SIZE : bufsize);
    }

    function copyInput(source:Input, size:Int):Void {
        if(size <= 0) throw new Exception("Buffer size must be positive");
        var buffer = Bytes.alloc(Std.int(Math.min(size, ResponseBridge.CHUNK_SIZE)));
        while(true) {
            var count:Int;
            try { count = source.readBytes(buffer, 0, buffer.length); }
            catch(_:haxe.io.Eof) { break; }
            if(count <= 0) throw new Exception("Input made no progress");
            sendBytes(buffer, 0, count);
        }
    }

    public function writeString(value:String, ?encoding:haxe.io.Encoding):Void {
        var bytes = Bytes.ofString(value, encoding);
        writeFullBytes(bytes);
    }

    public function writeFullBytes(bytes:Bytes, pos:Int = 0, len:Int = -1):Void {
        if(len == -1) len = bytes.length - pos;
        if(pos < 0 || len < 0 || pos > bytes.length - len) throw haxe.io.Error.OutsideBounds;
        beginWrite();
        while(len > 0) {
            var size = Std.int(Math.min(len, writeChunkSize));
            sendBytes(bytes, pos, size);
            pos += size;
            len -= size;
        }
    }

    public function writeByte(value:Int):Void {
        var bytes = Bytes.alloc(1);
        bytes.set(0, value);
        writeFullBytes(bytes);
    }

    public function flush():Void {
        // Each bridge write flushes on the socket's owning runtime.
    }

    public function close():Void {
        if(closed) return;
        if(encoder != null) {
            try { encoder.finish(); }
            catch(error:Dynamic) { abort(); throw error; }
        }
        closed = true;
        if(responseStarted) bridge.end(); else bridge.abort();
        release();
    }

    function release():Void {
        body = null;
        requestInput = null;
        baseResponse = null;
        deferredResponse = null;
    }

    /** What `source` has, read until it ends or until more than `limit` bytes are in. **/
    function readUpTo(source:Input, limit:Int):Bytes {
        var buffer = Bytes.alloc(limit + 1);
        var count = 0;
        while(count < buffer.length) {
            var read:Int;
            try { read = source.readBytes(buffer, count, buffer.length - count); }
            catch(_:haxe.io.Eof) { break; }
            if(read <= 0) throw new Exception("Input made no progress");
            count += read;
        }
        return count == buffer.length ? buffer : buffer.sub(0, count);
    }

    public function abort():Void {
        closed = true;
        if(encoder != null) encoder.dispose();
        if(webSocket != null) webSocket.close(1011);
        bridge.abort();
    }

    public function upgradeToWebSocket(request:Request, handler:AbstractWebSocketHandler):Void {
        var head = WebSocketBridge.handshake(request, inheritedResponse());
        bridge.upgrade(native -> {
            webSocket = new WebSocketBridge(native, request, handler, driver.config, head, driver.unregisterWebSocket);
            driver.registerWebSocket(webSocket);
            webSocket.open();
        });
        closed = true;
    }

    public function afterRequest():Void {
        if(webSocket != null) webSocket.start();
    }
}

private class CrossbyteOutput extends Output {
    var context:CrossbyteDriverContext;
    public function new(context:CrossbyteDriverContext) { this.context = context; }
    override public function writeByte(value:Int):Void { context.writeByte(value); }
    override public function writeBytes(bytes:Bytes, pos:Int, len:Int):Int {
        context.writeFullBytes(bytes, pos, len);
        return len;
    }
    override public function flush():Void { context.flush(); }
    override public function close():Void { context.close(); }
}
