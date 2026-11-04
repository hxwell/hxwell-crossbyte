package java;

/** Minimal compatibility for the native class helpers used by Crossbyte. */
@:native("hx.well.http.driver.crossbyte.compat.JavaLib")
class Lib {
    public static inline function toNativeType<T>(value:Class<T>):java.lang.Class<T> { return cast value; }
    public static inline function getNativeType<T>(value:T):java.lang.Class<T> { return jvm.Jvm.getNativeType(value); }
}
