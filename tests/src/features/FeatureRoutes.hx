package features;

import hx.well.HxWell;
import hx.well.auth.IAuthenticatable;
import hx.well.facades.AuthStatic;
import hx.well.facades.Cache;
import hx.well.facades.Config;
import hx.well.facades.Crypt;
import hx.well.facades.Hash;
import hx.well.facades.Schedule;
import hx.well.http.AbstractResponse;
import hx.well.http.JsonResponse;
import hx.well.http.Request;
import hx.well.http.Response;
import hx.well.http.ResponseBuilder;
import hx.well.http.ResponseStatic;
import hx.well.http.StringResponse;
import hx.well.route.Route;
import hx.well.template.TemplateData;
import hx.well.validator.ValidatorRule;

class FeatureRoutes {
    public static function boot():Void {
        var guards:haxe.ds.StringMap<Class<IAuthenticatable>> = Config.get("session.guards");
        guards.set("default", Identity);
        guards.set("admin", Identity);
        HxWell.middlewares.push(hx.well.middleware.SessionAuthMiddleware);
        TemplateData.data.set("feature-template", "Hello ::name::!");
        hx.well.console.CommandExecutor.register(ProbeCommand);
        Route.path("/feature").group(() -> {
            Route.get("/optional/{name?}", req -> ("hello:" + (req.route("name") ?? "anon") : AbstractResponse));
            Route.get("/number/{id}", req -> (req.route("id") : AbstractResponse)).where("id", "[0-9]+");
            Route.get("/named", req -> ("named" : AbstractResponse)).name("crossbyte.named");
            Route.get("/named-redirect", req -> (ResponseBuilder.asRedirectRoute("crossbyte.named") : AbstractResponse));
            Route.get("/template/{name}", req -> (ResponseBuilder.asTemplate("feature-template", {name:req.route("name")}) : AbstractResponse));
            Route.get("/json-status", _ -> (ResponseBuilder.asJson({created:true, nested:new NestedResponse()}, 201) : AbstractResponse));
            Route.any("/method/{method}").handler(new ProbeMethods());
            Route.get("/file", _ -> (ResponseBuilder.asFileInput(sys.io.File.read("public/hello.txt")) : AbstractResponse));
            Route.get("/middleware", _ -> ("route" : AbstractResponse)).middleware(OrderMiddleware);
            Route.get("/services", req -> {
                var cache = Cache.store();
                cache.put("feature-value", "cached", 30);
                var scheduled = Schedule.get().once(_ -> { cache.put("feature-scheduled", "done", 30); return "done"; }, 10);
                var end = haxe.Timer.stamp() + 3;
                while(!cache.has("feature-scheduled") && haxe.Timer.stamp() < end) Sys.sleep(0.005);
                var encrypted = Crypt.encryptString("private value");
                var hash = Hash.make("password", 4);
                var result = {cache:cache.pull("feature-value", "missing"), scheduled:cache.get("feature-scheduled", "missing"),
                    decrypt:Crypt.decryptString(encrypted), checked:Hash.check("password", hash), rejected:!Hash.check("other", hash),
                    command:hx.well.console.CommandExecutor.execute("cb:probe", ["argument"])};
                cache.forget("feature-scheduled");
                return (new JsonResponse(result) : AbstractResponse);
            });
            Route.any("/encoding/{kind}", req -> {
                var response = new StringResponse([for(_ in 0...1000) "encoded-data!"].join(""));
                response.encodingOptions = req.route("kind") == "empty"
                    ? new hx.well.http.encoding.EmptyEncodingOptions()
                    : new hx.well.http.encoding.DeflateEncodingOptions(Std.parseInt(req.route("kind")), 1024);
                return (response : AbstractResponse);
            });
            Route.get("/hold", req -> (ResponseBuilder.asAsync() : AbstractResponse));
            Route.get("/login/{guard}/{id}", req -> {
                AuthStatic.guard(req.route("guard")).login(new Identity(req.route("id")));
                return ("logged in" : AbstractResponse);
            });
            Route.get("/who/{guard}", req -> {
                var auth = AuthStatic.guard(req.route("guard"));
                var user:IAuthenticatable = auth.user();
                return (new JsonResponse({id:auth.id(), checked:auth.check(), user:user == null ? null : user.getId()}) : AbstractResponse);
            });
            Route.get("/logout", _ -> { AuthStatic.logout(); return ("logged out" : AbstractResponse); });
            Route.get("/protected", _ -> ("protected" : AbstractResponse)).middleware(DefaultAuth);
            Route.get("/login-page", _ -> ("login" : AbstractResponse)).name("login");
        });
        Route.domain("tenant.example").group(() -> Route.get("/tenant", _ -> ("tenant" : AbstractResponse)));
        var echo = new FeatureWebSocket();
        Route.websocket("/ws").handler(echo);
        Route.websocket("/ws-guarded").handler(echo).middleware(hx.well.middleware.DenyMiddleware);
        Route.websocket("/ws-auth").handler(echo).middleware(DefaultAuth);
        Route.websocket("/socket.io").handler(new ChatSocket());
        #if neko
        DatabaseFeatures.boot();
        #end
    }
}

private class DefaultAuth extends hx.well.middleware.RequireAuthMiddleware<"default"> {
    public function new() { super(); }
}

@:keep private class Identity implements IAuthenticatable {
    public static var query = {find:(id:Dynamic) -> new Identity(Std.string(id))};
    var id:String;
    public function new(id:String) { this.id = id; }
    public function getId():Dynamic { return id; }
    public function getPassword():String { return ""; }
}

private class NestedResponse implements hx.well.http.IResponseInstance {
    public function new() {}
    public function getResponse():Response { return new JsonResponse({value:"nested"}); }
}

private class OrderMiddleware extends hx.well.middleware.AbstractMiddleware {
    public function new() { super(); }
    public function handle(request:Request, next:Request->Null<Response>):Null<Response> {
        ResponseStatic.header("X-Middleware", "before");
        var response = next(request);
        response.header("X-After", "after");
        return response;
    }
}

private class ProbeMethods extends hx.well.handler.MethodHandler {
    public function new() { super(); }
    @:post
    @:validator("field", [ValidatorRule.Required, ValidatorRule.String])
    public function validated(request:Request):AbstractResponse { return "valid:" + request.input("field"); }
    @:put
    @:patch
    @:delete
    @:options
    public function verbs(request:Request):AbstractResponse { return request.method; }
}

private class ProbeCommand extends hx.well.console.AbstractCommand<String> {
    public function new() { super(); }
    public function signature():String { return "cb:probe"; }
    public function description():String { return "Integration test command"; }
    public function handle():String { return "command:" + args.join(","); }
}
