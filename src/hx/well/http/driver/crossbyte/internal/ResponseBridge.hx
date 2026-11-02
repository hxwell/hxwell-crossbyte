package hx.well.http.driver.crossbyte.internal;

import crossbyte.core.CrossByte;
import crossbyte.events.Event;
import crossbyte.http.HTTPRequestHandler;
import crossbyte.http.HTTPResponseStream;
import crossbyte.io.ByteArray;
import haxe.Exception;
import haxe.io.Bytes;
import sys.thread.Lock;
import sys.thread.Mutex;

/** Blocking Haxe Output calls rendezvous with the socket's owning runtime. */
class ResponseBridge {
    public static inline var CHUNK_SIZE:Int = 32768;
    var runtime:CrossByte;
    var handler:HTTPRequestHandler;
    var stream:HTTPResponseStream;
    var pending:BridgeOperation;
    var gone:Bool = false;
    var completed:Bool = false;
    var started:Bool = false;
    var isHead:Bool;
    var discards:Bool = false;
    var timeout:Float;
    var onComplete:Void->Void;
    var mutex = new Mutex();

    // Constructed on the runtime, before handing a snapshot to the worker.
    public function new(handler:HTTPRequestHandler, timeout:Float, onComplete:Void->Void) {
        this.handler = handler;
        isHead = handler.method == "HEAD";
        this.timeout = timeout;
        this.onComplete = onComplete;
        runtime = CrossByte.current();
        handler.addEventListener(Event.CLOSE, onClientClosed);
    }

    function onClientClosed(_:Event):Void {
        gone = true;
        if(pending != null) pending.finish(new Exception("HTTP client disconnected"));
        release();
    }

    function release():Void {
        if(completed) return;
        completed = true;
        if(handler != null) handler.removeEventListener(Event.CLOSE, onClientClosed);
        if(stream != null) stream.onDrain = null;
        onComplete();
    }

    function run(action:BridgeOperation->Void):Void {
        if(runtime.__isOwnThread())
            throw new Exception("Write a Crossbyte response from a worker, never its I/O runtime");
        mutex.acquire();
        var operation = new BridgeOperation();
        if(!runtime.post(() -> {
            if(gone || completed) { operation.finish(new Exception("HTTP response is closed")); return; }
            pending = operation;
            try { action(operation); }
            catch(error:Dynamic) { operation.finish(error); }
        })) {
            mutex.release();
            throw new Exception("Crossbyte runtime has stopped");
        }
        var signalled = operation.ready.wait(timeout);
        mutex.release();
        if(!signalled) {
            abort();
            throw new Exception("Timed out writing the HTTP response");
        }
        if(operation.error != null) throw operation.error;
    }

    /**
     * A whole response whose body is known: handed to the runtime in one
     * call that is not waited for, and sent there in one write with its
     * length. The worker goes on at once. A failure on the runtime aborts the
     * response, which is all a caller that had waited could have done.
     */
    public function respond(head:ResponseHead, body:Bytes):Void {
        var send = () -> {
            if(gone || completed || started) return;
            var current = handler;
            started = true;
            // Detached first: a response that completes in the call settles
            // the native handler, possibly dispatching its next request.
            release();
            handler = null;
            stream = null;
            try UpstreamAccess.respondBytes(current, head, body)
            catch(error:Dynamic) {
                UpstreamAccess.abort(current);
                trace(error);
            }
        };
        if(runtime.__isOwnThread()) send();
        else if(!runtime.post(send)) throw new Exception("Crossbyte runtime has stopped");
    }

    public function begin(head:ResponseHead):Void {
        run(operation -> {
            if(started) throw new Exception("HTTP response already started");
            discards = isHead || head.status == 204 || head.status == 304;
            // A bodiless response settles the native handler inside beginResponse,
            // potentially dispatching its next request before this call returns.
            if(discards) handler.removeEventListener(Event.CLOSE, onClientClosed);
            stream = UpstreamAccess.beginResponse(handler, head);
            started = true;
            if(discards) {
                handler = null;
                release();
            }
            operation.finish();
        });
    }

    public function write(bytes:Bytes, offset:Int, length:Int):Void {
        if(length == 0 || discards) return;
        // The worker waits until each slice is copied/accepted. A false result
        // means this slice was accepted: wait for drain, never send it twice.
        var slice = ByteArray.fromBytes(bytes.sub(offset, length));
        run(operation -> {
            if(!started) throw new Exception("HTTP response has not started");
            var send:Void->Void = null;
            send = () -> {
                try {
                    if(!stream.connected) { operation.finish(new Exception("HTTP response disconnected")); return; }
                    if(!UpstreamAccess.hasWriteRoom(handler, length)) {
                        stream.onDrain = send;
                        UpstreamAccess.awaitDrain(stream);
                        return;
                    }
                    stream.onDrain = null;
                    var room = stream.write(slice);
                    if(room) operation.finish();
                    else if(!stream.connected) operation.finish(new Exception("HTTP response disconnected"));
                    else stream.onDrain = () -> {
                        stream.onDrain = null;
                        operation.finish();
                    };
                } catch(error:Dynamic) { operation.finish(error); }
            };
            send();
        });
    }

    public function end():Void {
        if(discards) return;
        run(operation -> {
            // Detach before end can dispatch the next pipelined request on this
            // same HTTPRequestHandler. Late callbacks must never reach it.
            release();
            if(stream != null) stream.end();
            handler = null;
            stream = null;
            operation.finish();
        });
    }

    public function upgrade(transfer:HTTPRequestHandler->Void):Void {
        run(operation -> {
            if(started) throw new Exception("HTTP response already started");
            handler.removeEventListener(Event.CLOSE, onClientClosed);
            transfer(handler);
            release();
            handler = null;
            operation.finish();
        });
    }

    public function abort():Void {
        runtime.post(() -> {
            if(completed) return;
            gone = true;
            if(pending != null) pending.finish(new Exception("HTTP response aborted"));
            var current = handler;
            release();
            handler = null;
            stream = null;
            if(current != null) UpstreamAccess.abort(current);
        });
    }
}

private class BridgeOperation {
    public var ready = new Lock();
    public var error:Dynamic;
    var settled:Bool = false;
    public function new() {}
    // Only the owning runtime settles operations; Lock publishes to the worker.
    public function finish(error:Dynamic = null):Void {
        if(settled) return;
        settled = true;
        this.error = error;
        ready.release();
    }
}
