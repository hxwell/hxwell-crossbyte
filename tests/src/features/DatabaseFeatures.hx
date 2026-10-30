package features;

#if neko
import hx.well.database.Connection;
import hx.well.facades.DB;
import hx.well.facades.Schema;
import hx.well.facades.AuthStatic;
import hx.well.http.AbstractResponse;
import hx.well.http.JsonResponse;
import hx.well.route.Route;

class DatabaseFeatures {
    public static function boot():Void {
        Connection.create("default", {path:"application.db"});
        Schema.create("cb_accounts", table -> {
            table.id(); table.string("username").unique(); table.string("password");
        });
        Account.query.create(["username" => "tester", "password" => hx.well.facades.Hash.make("correct", 4)]);
        var guards:haxe.ds.StringMap<Class<hx.well.auth.IAuthenticatable>> = hx.well.facades.Config.get("session.guards");
        guards.set("database", Account);
        Connection.free();
        Route.get("/db/exercise", _ -> {
            Schema.create("cb_items", table -> { table.id(); table.string("name").unique(); });
            var first = Item.query.create(["name" => "first"]);
            var second = Item.query.create(["name" => "second"]);
            first.update(["name" => "updated"]);
            second.delete();
            var db = new DB();
            try { db.transaction(() -> { Item.query.create(["name" => "rollback"]); throw new haxe.Exception("rollback"); }); }
            catch(_:Dynamic) {}
            Schema.table("cb_items", table -> table.string("extra").nullable());
            return (new JsonResponse({count:Item.query.count(), name:Item.query.find(first.id).name,
                columns:Schema.getColumnListing("cb_items"), index:Schema.hasIndex("cb_items", "cb_items_name_unique"),
                model:Item.query.find(first.id)}) : AbstractResponse);
        });
        Route.get("/db/rows", _ -> (Item.query.orderBy("id").getResultSetResponse() : AbstractResponse));
        Route.post("/db/login", req -> (new JsonResponse({loggedIn:AuthStatic.guard("database").attempt([
            "username" => req.input("username"), "password" => req.input("password")])}) : AbstractResponse));
    }
}

@:table("cb_items") private class Item extends hx.well.model.BaseModel<Item> {
    @:primary @:field @:visible public var id:Int;
    @:field @:visible public var name:String;
    public function new() { super(); }
}

@:table("cb_accounts") private class Account extends hx.well.model.BaseModel<Account> implements hx.well.auth.IAuthenticatable {
    @:primary @:field @:visible public var id:Int;
    @:field @:visible public var username:String;
    @:field @:hidden public var password:String;
    public function new() { super(); }
    public function getId():Dynamic { return id; }
    public function getPassword():String { return password; }
}
#end
