package hx.well.http.driver.crossbyte.macro;

#if macro
import haxe.io.Path;
import haxe.macro.Context;

class Compatibility {
    public static function check():Void {
        if(Context.defined("haxe5"))
            Context.fatalError("hxwell-crossbyte 0.1.0 requires Haxe 4.3.7: Crossbyte 1.0.1 has an HTTP/2 type-resolution failure on Haxe 5 previews.", Context.currentPos());
        if(!Context.defined("target.threaded") || Context.defined("js"))
            Context.fatalError("hxwell-crossbyte requires a threaded sys target. JVM with Java 21+ is the verified target.", Context.currentPos());
        var source = Context.resolvePath("crossbyte/http/HTTPServer.hx");
        var root = Path.normalize(Path.directory(source) + "/../../..");
        var metadata = haxe.Json.parse(sys.io.File.getContent(root + "/haxelib.json"));
        if(metadata.version != "1.0.1")
            Context.fatalError("hxwell-crossbyte 0.1.0 requires the Crossbyte 1.0.1 Git snapshot. See README.md for the tested revision and installation commands.", Context.currentPos());
    }
}
#end
