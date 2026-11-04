package hx.well.http.driver.crossbyte.internal;

import crossbyte.url.URLRequestHeader;
import hx.well.http.HttpSyntax;
import hx.well.http.Response;
import hx.well.http.CookieData;
import hx.well.http.encoding.DeflateEncodingOptions;
import hx.well.http.encoding.EmptyEncodingOptions;

@:access(hx.well.http.Response)
class ResponseHead {
    public var status:Int;
    public var reason:String;
    public var contentType:String;
    public var headers:Array<URLRequestHeader> = [];
    public var deflate:DeflateEncodingOptions;
    public var nativeCompression:Bool = true;

    public function new(response:Response, base:Response, acceptEncoding:String = "") {
        base.validate();
        response.validate();
        // Keep the nullable value boxed until the final fallback. Haxe 5
        // preview 1 unboxes an intermediate chained ?? result to zero.
        var selected:Null<Int> = response.statusCode;
        if(selected == null) selected = base.statusCode;
        status = selected == null ? 200 : selected;
        if(status < 200 || status > 599)
            throw new haxe.Exception("A final Crossbyte response requires a status from 200 through 599");
        reason = response.statusMessage ?? base.statusMessage;
        var merged:Map<String, URLRequestHeader> = [];
        for(source in [base, response])
            for(key => value in source.headers)
                merged.set(HttpSyntax.lowerAscii(key), new URLRequestHeader(key, value));
        var options = response.encodingOptions ?? base.encodingOptions;
        if(options != null) {
            nativeCompression = false;
            if(options is DeflateEncodingOptions) {
                var vary = merged.get("vary");
                merged.set("vary", new URLRequestHeader("Vary", (vary == null ? "" : vary.value + ", ") + "Accept-Encoding"));
                if(acceptsDeflate(acceptEncoding) && !merged.exists("content-encoding")) {
                    deflate = cast options;
                    merged.set("content-encoding", new URLRequestHeader("Content-Encoding", "deflate"));
                }
            } else if(!(options is EmptyEncodingOptions)) throw new haxe.Exception("Unsupported response encoding options");
        }
        var type = merged.get("content-type");
        contentType = type == null ? "application/octet-stream" : type.value;
        // Crossbyte owns the wire framing for HTTP/1.1 and HTTP/2.
        for(key => value in merged)
            if(key != "content-type" && key != "content-length" && key != "transfer-encoding") headers.push(value);
        var cookies:Map<String, CookieData> = [];
        for(source in [base, response])
            for(key => cookie in source.cookies) cookies.set(key, cookie);
        for(cookie in cookies) headers.push(new URLRequestHeader("Set-Cookie", cookie.toString()));
    }

    static function acceptsDeflate(header:String):Bool {
        var specific:Null<Float> = null;
        var wildcard:Float = 0;
        for(part in header.split(",")) {
            var fields = part.split(";");
            var name = HttpSyntax.lowerAscii(StringTools.trim(fields.shift()));
            var quality:Float = 1;
            for(param in fields) {
                var pair = param.split("=");
                if(pair.length == 2 && HttpSyntax.lowerAscii(StringTools.trim(pair[0])) == "q") {
                    var value = StringTools.trim(pair[1]);
                    quality = ~/^(0(\.[0-9]{0,3})?|1(\.0{0,3})?)$/.match(value) ? Std.parseFloat(value) : 0;
                }
            }
            if(name == "deflate") specific = quality;
            if(name == "*") wildcard = quality;
        }
        return (specific ?? wildcard) > 0;
    }
}
