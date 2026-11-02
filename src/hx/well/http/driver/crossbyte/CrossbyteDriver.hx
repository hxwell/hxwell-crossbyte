package hx.well.http.driver.crossbyte;

import crossbyte.core.CrossByte;
import crossbyte.events.Event;
import crossbyte.http.HTTPRequestHandler;
import crossbyte.http.HTTPServer;
import crossbyte.http.HTTPServerConfig;
import haxe.Exception;
import hx.well.facades.Config;
import hx.well.http.HttpHandler;
import hx.well.http.driver.AbstractHttpDriver;
import hx.well.http.driver.crossbyte.internal.RuntimeHost;
import hx.well.http.driver.crossbyte.internal.RequestValidation;
import hx.well.http.driver.crossbyte.internal.UpstreamAccess;
import hx.well.http.driver.crossbyte.internal.WebSocketBridge;
import sys.thread.Lock;
import sys.thread.Mutex;

class CrossbyteDriver extends AbstractHttpDriver<CrossbyteDriverConfig> {
    public var localPort(default, null):Int = 0;
    var runtime:CrossByte;
    var server:HTTPServer;
    var stateMutex = new Mutex();
    var active:Int = 0;
    var accepting:Bool = false;
    var webSockets:Array<WebSocketBridge> = [];
    #if !jvm
    var executor:hx.well.http.driver.crossbyte.internal.WorkerPool;
    #end

    public function new(config:CrossbyteDriverConfig) {
        super(config);
        RuntimeHost.initialize();
    }

    public function start():Void {
        if(runtime != null) throw new Exception("Crossbyte driver already started");
        if(config.maxConcurrentRequests <= 0 || config.poolSize <= 0 || config.maxConnections <= 0
            || config.writeTimeoutSeconds <= 0 || config.shutdownTimeoutSeconds <= 0 || config.maxWebSocketMessageSize <= 0
            || config.maxWebSocketQueuedBytes <= 0 || config.maxWebSocketQueuedMessages <= 0 || config.maxWebSocketOutputBytes <= 0
            || config.webSocketCloseTimeoutSeconds <= 0)
            throw new Exception("Crossbyte limits and timeouts must be positive");
        if(config.ssl && (config.tlsCertificatePath == null || config.tlsKeyPath == null))
            throw new Exception("Crossbyte TLS requires certificate and key PEM paths");
        var http = new HTTPServerConfig(config.host, config.port);
        var limit:Null<Int> = Config.get("http.max_content_length");
        if(limit != null && limit > 0) http.maxRequestBodySize = limit;
        http.maxConnections = config.maxConnections;
        if(config.ssl) {
            http.tlsCertificatePath = config.tlsCertificatePath;
            http.tlsKeyPath = config.tlsKeyPath;
        }
        config.configureHttp(http);
        if(http.maxOutputBufferSize > 0 && http.maxOutputBufferSize < 65536)
            throw new Exception("Crossbyte output capacity must be at least 64 KiB for bounded response slices");
        // HxWell owns routing, static files and PHP/application execution.
        http.rootDirectory = null;
        http.phpEnabled = false;
        http.middleware = [(handler, next) -> dispatch(handler)];
        http.validate();
        #if !jvm
        executor = new hx.well.http.driver.crossbyte.internal.WorkerPool(config.poolSize);
        #end
        var ready = new Lock();
        var failure:Dynamic = null;
        runtime = CrossByte.make(POLL, HEAP, child -> {
            child.addEventListener(Event.INIT, _ -> {
                try {
                    server = new HTTPServer(http);
                    localPort = server.localPort;
                    stateMutex.acquire();
                    accepting = true;
                    stateMutex.release();
                } catch(error:Dynamic) { failure = error; }
                ready.release();
            });
        });
        if(!ready.wait(15)) {
            runtime.exit();
            runtime = null;
            throw new Exception("Timed out starting Crossbyte HTTP server");
        }
        if(failure != null) {
            runtime.exit();
            runtime = null;
            throw failure;
        }
        config.onStart();
    }

    function dispatch(handler:HTTPRequestHandler):Void {
        var rejected = RequestValidation.status(handler);
        if(rejected != 0) { UpstreamAccess.reject(handler, rejected); return; }
        stateMutex.acquire();
        var admitted = accepting && active < config.maxConcurrentRequests;
        if(admitted) active++;
        stateMutex.release();
        if(!admitted) { handler.respond(503, "text/plain", "Server busy"); return; }
        var context:CrossbyteDriverContext;
        try { context = new CrossbyteDriverContext(handler, config.writeTimeoutSeconds, releaseRequest, this); }
        catch(error:Dynamic) { releaseRequest(); throw error; }
        var work = () -> {
            try { HttpHandler.process(context); }
            catch(error:Dynamic) { context.abort(); trace(error); }
            context.afterRequest();
        };
        try {
            #if jvm
            jvm.thread.VirtualThread.create(work).start();
            #else
            executor.submit(work);
            #end
        } catch(error:Dynamic) { context.abort(); throw error; }
    }

    function releaseRequest():Void {
        stateMutex.acquire();
        active--;
        stateMutex.release();
    }

    public function registerWebSocket(socket:WebSocketBridge):Void {
        stateMutex.acquire(); webSockets.push(socket); stateMutex.release();
    }

    public function unregisterWebSocket(socket:WebSocketBridge):Void {
        stateMutex.acquire(); webSockets.remove(socket); stateMutex.release();
    }

    public function stop():Void {
        var current = runtime;
        if(current == null) return;
        stateMutex.acquire();
        accepting = false;
        var sockets = webSockets.copy();
        stateMutex.release();
        for(socket in sockets) socket.close(1001);
        var done = new Lock();
        if(current.post(() -> {
            server.drain(config.shutdownTimeoutSeconds, () -> {
                current.exit();
                done.release();
            });
        })) done.wait(config.shutdownTimeoutSeconds + 2);
        current.exit();
        runtime = null;
        server = null;
        localPort = 0;
        #if !jvm
        executor.stop();
        #end
    }
}
