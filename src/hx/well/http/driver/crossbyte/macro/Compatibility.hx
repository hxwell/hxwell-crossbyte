package hx.well.http.driver.crossbyte.macro;

#if macro
import haxe.io.Path;
import haxe.macro.Context;
import haxe.macro.Compiler;

class Compatibility {
    public static function check():Void {
        if(Context.defined("haxe5")) {
            if(!Context.defined("jvm") || Context.definedValue("haxe") != "5.0.0-preview.1")
                Context.fatalError("Haxe 5 support is verified with 5.0.0-preview.1 on JVM only.", Context.currentPos());
            // Type aliases only: retain the same Crossbyte runtime source and
            // Java primitive array ABI on the new Haxe 5 JVM standard library.
            var root = Path.directory(Context.resolvePath("hx/well/http/driver/crossbyte/macro/Compatibility.hx"));
            for(_ in 0...7) root = Path.directory(root);
            Compiler.addClassPath(root + "/compat/haxe5");
        }
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
