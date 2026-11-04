package hx.well.http.driver.crossbyte.compat;

/** Haxe 5 preview 1 does not provide HashMap.size(), still called by haxe-concurrent 5.1.5. */
class HashMapTools {
    public static function size<K:{function hashCode():Int;}, V>(map:haxe.ds.HashMap<K, V>):Int {
        var count = 0;
        for (_ in map.keys()) count++;
        return count;
    }
}
