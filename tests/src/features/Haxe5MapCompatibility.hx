package features;

#if haxe5
/** Runtime contract check for the unmodified haxe-concurrent 5.1.5 package. */
class Haxe5MapCompatibility {
    public static function verify():Void {
        var map:hx.concurrent.collection.SynchronizedMap<MapKey, Null<String>> = hx.concurrent.collection.SynchronizedMap.newHashMap();
        var first = new MapKey(1);
        var second = new MapKey(2);
        if (map.size() != 0) throw new haxe.Exception("New hash map must be empty");
        map.set(first, "one");
        map.set(second, null);
        if (map.size() != 2) throw new haxe.Exception("Hash map size must include null-valued keys");
        map.remove(first);
        if (map.size() != 1) throw new haxe.Exception("Hash map removal must change its size");
        map.clear();
        if (map.size() != 0) throw new haxe.Exception("Cleared hash map must be empty");
    }
}

private class MapKey {
    var value:Int;
    public function new(value:Int) { this.value = value; }
    public function hashCode():Int { return value; }
}
#end
