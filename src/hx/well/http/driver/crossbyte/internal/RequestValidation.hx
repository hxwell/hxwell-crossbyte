package hx.well.http.driver.crossbyte.internal;

import crossbyte.http.HTTPRequestHandler;
import hx.well.exception.AbortException;
import hx.well.facades.Config;
import hx.well.http.HttpSyntax;
import hx.well.http.RequestParser;

class RequestValidation {
    /** 0 accepts the native parser's result; otherwise reject before routing. */
    public static function status(handler:HTTPRequestHandler):Int {
        var configured:Null<Int> = Config.get("http.max_buffer");
        var maximum = configured != null && configured > 0 ? configured : 65536;
        var raw = UpstreamAccess.wireHead(handler, maximum);
        if(raw == null) return 0; // HTTP/2's binary field validation is native.
        if(raw.length > maximum) return 431;
        var text = raw.toString();
        if(!StringTools.endsWith(text, "\r\n\r\n")) return 400;
        var lines = text.substr(0, text.length - 4).split("\r\n");
        var request = lines.shift().split(" ");
        if(request.length != 3 || !HttpSyntax.isToken(request[0]) || request[1].length == 0
            || !HttpSyntax.isFieldValue(request[1]) || request[1].indexOf("\t") >= 0
            || (request[2] != "HTTP/1.1" && request[2] != "HTTP/1.0")) return 400;
        var pathLimit:Null<Int> = Config.get("http.max_path_length");
        if(pathLimit != null && pathLimit > 0 && request[1].length > pathLimit) return 414;
        var seen:Map<String, Bool> = [];
        for(line in lines) {
            var colon = line.indexOf(":");
            if(colon <= 0) return 400;
            var name = line.substring(0, colon);
            var value = line.substring(colon + 1);
            // Check the original bytes before trimming: VT/FF are not HTTP OWS.
            if(!HttpSyntax.isToken(name) || !HttpSyntax.isFieldValue(value)) return 400;
            name = HttpSyntax.lowerAscii(name);
            value = HttpSyntax.trimOWS(value);
            if(name == "host" || name == "content-length" || name == "transfer-encoding") {
                if(seen.exists(name)) return 400;
                seen.set(name, true);
            }
            if(name == "content-length") {
                try { RequestParser.contentLength(value); }
                catch(_:AbortException) { return 400; }
            }
            if(name == "transfer-encoding" && HttpSyntax.lowerAscii(value) != "chunked") return 501;
        }
        if(seen.exists("content-length") && seen.exists("transfer-encoding")) return 400;
        return 0;
    }
}
