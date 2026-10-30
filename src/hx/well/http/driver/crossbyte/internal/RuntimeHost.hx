package hx.well.http.driver.crossbyte.internal;

#if (!target.threaded || js)
#error "hxwell-crossbyte requires a threaded sys target (JVM, C++, HashLink, Neko or eval)."
#end

import crossbyte.core.Application;
import crossbyte.core.HostApplication;

/** Establishes a root without taking over HxWell's CLI/process lifetime. */
class RuntimeHost {
    public static function initialize():Void {
        if(Application.application == null) {
            // CrossByte.make uses EntryPoint.addThread. On JVM EntryPoint is
            // initialized lazily: a secondary listener must not capture its
            // event-loop-less startup thread as EntryPoint's main thread.
            haxe.EntryPoint.runInMainThread(() -> {});
            var host = new EmbeddedHost();
            host.advance(0);
        }
    }
}

private class EmbeddedHost extends HostApplication {
    public function new() { super(); }
}
