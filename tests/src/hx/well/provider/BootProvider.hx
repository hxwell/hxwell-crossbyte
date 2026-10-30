package hx.well.provider;

import haxe.io.Bytes;
import hx.well.http.AbstractResponse;
import hx.well.http.HttpHandler;
import hx.well.http.InputResponse;
import hx.well.http.JsonResponse;
import hx.well.http.Response;
import hx.well.http.ResponseBuilder;
import hx.well.http.ResponseStatic;
import hx.well.http.StringResponse;
import hx.well.middleware.CsrfMiddleware;
import hx.well.route.Route;

class BootProvider extends AbstractProvider {
    public function boot():Void {
        hx.well.facades.Environment.set("APP_NAME", "crossbyte_test");
        hx.well.facades.Environment.set("APP_KEY", haxe.crypto.Base64.encode(Bytes.ofString("0123456789abcdef0123456789abcdef")));
        Route.get("/hello/{name}", req -> (new JsonResponse({
            name:req.route("name"), query:req.query("q"), path:req.path,
            protocol:req.version, remote:req.ip, header:req.header("X-Custom"), cookie:req.cookie("custom"),
            virtualThread:#if jvm jvm.thread.VirtualThread.VirtualThreadExtern.currentThread().isVirtual() #else false #end
        }) : AbstractResponse));
        Route.post("/echo", req -> (new JsonResponse({body:req.bodyBytes.toString(), field:req.input("field")}) : AbstractResponse));
        Route.post("/upload", req -> (new InputResponse(req.context.input, null).header("Content-Type", "application/octet-stream") : AbstractResponse)).setStream(true);
        Route.get("/headers", req -> {
            ResponseStatic.header("X-Base", "static");
            ResponseStatic.header("content-type", "text/wrong");
            ResponseStatic.cookie("first", "1");
            var response = new StringResponse("headers", 201).header("X-Reply", "hxwell-crossbyte");
            response.cookie("second", "2").path("/app").httpOnly(false);
            return (response : AbstractResponse);
        });
        Route.get("/session/set/{value}", req -> { req.session.put("stored", req.route("value")); return ("stored" : AbstractResponse); });
        Route.get("/session/get", req -> (Std.string(req.session.get("stored", "none")) : AbstractResponse));
        Route.get("/csrf", req -> ("token" : AbstractResponse)).middleware(CsrfMiddleware);
        Route.post("/csrf", req -> ("csrf-ok" : AbstractResponse)).middleware(CsrfMiddleware);
        Route.get("/abort/{status}", req -> { ResponseStatic.abort(Std.parseInt(req.route("status"))); return null; });
        Route.get("/redirect", req -> (ResponseBuilder.asRedirect(req.query("to")) : AbstractResponse));
        Route.get("/manual", req -> {
            ResponseStatic.header("Content-Type", "text/plain");
            req.context.beginWrite();
            req.context.output.writeString("first-");
            req.context.writeFullBytes(Bytes.ofString("second"));
            req.context.writeByte(33);
            req.context.flush();
            req.context.close();
            return (ResponseBuilder.asAsync() : AbstractResponse);
        });
        Route.get("/deferred/{id}", req -> {
            ResponseStatic.header("X-Owner", req.route("id"));
            var response = ResponseBuilder.asAsync();
            sys.thread.Thread.create(() -> {
                Sys.sleep(0.05);
                req.context.writeResponse(new StringResponse("deferred-" + req.route("id")));
            });
            return (response : AbstractResponse);
        });
        Route.get("/large", req -> (new InputResponse(new PatternInput(4 * 1024 * 1024 + 137), null).header("Content-Type", "application/octet-stream") : AbstractResponse));
        Route.get("/broken", req -> (new InputResponse(new BrokenInput(), null) : AbstractResponse));
        Route.get("/slow", req -> {
            Sys.sleep(0.1);
            return (req.query("id") : AbstractResponse);
        });
        Route.get("/compress", req -> ([for(_ in 0...1000) "crossbyte-compression!"].join("") : AbstractResponse));
        Route.get("/shutdown", req -> {
            var response = new StringResponse("stopping");
            response.onAfter(() -> {
                if(hx.well.config.InstanceConfig.tlsDriver != null) hx.well.config.InstanceConfig.tlsDriver.stop();
                hx.well.config.InstanceConfig.driver.stop();
                Sys.exit(0);
            });
            return (response : AbstractResponse);
        });
        Route.get("/restart", req -> (new StringResponse("restarting").onAfter(() -> {
            hx.well.config.InstanceConfig.driver.stop();
            hx.well.config.InstanceConfig.driver.start();
        }) : AbstractResponse));
        Route.status(500).handler(new hx.well.handler.DynamicHandler(_ -> (new StringResponse("internal error", 500) : AbstractResponse)));
        features.FeatureRoutes.boot();
    }
}

private class PatternInput extends haxe.io.Input {
    var remaining:Int;
    var offset:Int = 0;
    public function new(length:Int) { remaining = length; }
    override public function readByte():Int {
        if(remaining == 0) throw new haxe.io.Eof();
        remaining--;
        return (offset++ * 31 + 7) & 255;
    }
    override public function readBytes(bytes:Bytes, pos:Int, len:Int):Int {
        if(remaining == 0) throw new haxe.io.Eof();
        var count = Std.int(Math.min(remaining, len));
        for(i in 0...count) bytes.set(pos + i, readByte());
        return count;
    }
}

private class BrokenInput extends haxe.io.Input {
    override public function readByte():Int { throw new haxe.Exception("broken input"); }
    public function new() {}
}
