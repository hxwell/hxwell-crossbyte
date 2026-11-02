package hx.well.http.driver.crossbyte.internal;

import crossbyte.http.HTTPRequestHandler;
import crossbyte.http.HTTPResponseStream;
import haxe.io.Bytes;

/**
 * Crossbyte 1.0.1 has public body/path/response APIs, but no complete HTTP header
 * iterator, protocol getter, original headers or response-abort method. Keep
 * compatibility accesses here, covered by the HTTP/1.1 and HTTP/2 wire tests.
 */
@:access(crossbyte.http.HTTPRequestHandler)
class UpstreamAccess {
    public static function headers(handler:HTTPRequestHandler):Map<String, String> {
        return handler.__headers.copy();
    }
    public static function version(handler:HTTPRequestHandler):String {
        return handler.__httpVersion;
    }
    public static function abort(handler:HTTPRequestHandler):Void {
        handler.__abandonResponse();
    }
    public static function wireHead(handler:HTTPRequestHandler, maximum:Int):Bytes {
        var buffer = handler.__incomingBuffer;
        if(buffer == null) return null; // HTTP/2 uses decoded header fields.
        var length = Std.int(Math.min(buffer.length, maximum + 1));
        for(i in 0...length - 1) {
            // Include LF-only endings too, so validation can reject rather than
            // accidentally scan body bytes as headers after the native parser.
            if(buffer[i] == 10 && buffer[i + 1] == 10) return (buffer : Bytes).sub(0, i + 2);
            if(i + 3 < length && buffer[i] == 13 && buffer[i + 1] == 10 && buffer[i + 2] == 13 && buffer[i + 3] == 10)
                return (buffer : Bytes).sub(0, i + 4);
        }
        return (buffer : Bytes).sub(0, length);
    }
    public static function reject(handler:HTTPRequestHandler, status:Int):Void {
        // Native middleware runs after body consumption. Force closure even if
        // the native parser considered the malformed request fully consumed.
        handler.__closeAfterResponse = true;
        handler.respond(status, "text/plain", hx.well.http.ResponseStatic.getStatusMessage(status));
    }
    public static function hasWriteRoom(handler:HTTPRequestHandler, length:Int):Bool {
        var writer = handler.__writer;
        var cap = writer.maxBufferedBytes;
        // Include HTTP chunk/frame delimiters and compressor flush overhead.
        return cap <= 0 || writer.bufferedBytes <= cap - length - 1024;
    }
    public static function awaitDrain(stream:HTTPResponseStream):Void {
        // Native write() marks this only AFTER it crosses its watermark. With
        // a cap below that watermark, the next whole slice can cross the hard
        // cap first. Arm the same notification before accepting that slice.
        @:privateAccess stream.__blocked = true;
    }

    public static function beginResponse(handler:HTTPRequestHandler, head:ResponseHead):HTTPResponseStream {
        var stream:HTTPResponseStream = null;
        withNegotiation(handler, head, () -> stream = handler.beginResponse(head.status, head.contentType, head.headers, head.reason));
        return stream;
    }

    /** A whole response in one native call, which sends it with its length. **/
    public static function respondBytes(handler:HTTPRequestHandler, head:ResponseHead, body:Bytes):Void {
        var data = crossbyte.io.ByteArray.fromBytes(body);
        withNegotiation(handler, head, () -> handler.respondBytes(head.status, head.contentType, data, head.headers, head.reason));
    }

    static function withNegotiation(handler:HTTPRequestHandler, head:ResponseHead, call:Void->Void):Void {
        if(head.nativeCompression) { call(); return; }
        // Limit this response's native negotiation without changing the server's
        // compression policy, which can be shared by several I/O runtimes.
        // HxWell already holds an independent snapshot of request headers.
        var previous = handler.__headers.get("accept-encoding");
        var generation = handler.__requestGeneration;
        handler.__headers.set("accept-encoding", "identity");
        try call()
        catch(error:Dynamic) {
            if(handler.__requestGeneration == generation) {
                if(previous == null) handler.__headers.remove("accept-encoding"); else handler.__headers.set("accept-encoding", previous);
            }
            throw error;
        }
        if(handler.__requestGeneration == generation) {
            if(previous == null) handler.__headers.remove("accept-encoding"); else handler.__headers.set("accept-encoding", previous);
        }
    }
}
