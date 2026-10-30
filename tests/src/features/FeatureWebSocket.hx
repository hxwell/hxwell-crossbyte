package features;

import hx.well.websocket.AbstractWebSocketHandler;
import hx.well.websocket.WebSocketSession;
import haxe.io.Bytes;
import haxe.Exception;

class FeatureWebSocket extends AbstractWebSocketHandler {
    public function onOpen(session:WebSocketSession):Void { session.setAttribute("query", session.queryParams.get("name")); }
    public function onMessage(session:WebSocketSession, message:String):Void {
        if(message == "query") session.send("query:" + session.getAttribute("query"));
        else if(message == "thread") session.send(#if jvm "virtual:" + jvm.thread.VirtualThread.VirtualThreadExtern.currentThread().isVirtual() #else "platform" #end);
        else if(message == "callback") session.send("first", () -> session.send("callback"));
        else if(message == "error") throw new Exception("expected callback failure");
        else if(message == "block") Sys.sleep(0.5);
        else if(message == "outbound-overflow") session.sendBinary(Bytes.alloc(2 * 1024 * 1024 + 1));
        #if neko
        else if(message == "database") session.send("database:" + new hx.well.facades.DB().select("SELECT COUNT(*) AS total FROM cb_accounts")[0].total);
        #end
        else if(StringTools.startsWith(message, "broadcast:")) broadcast(message.substr(10));
        else session.send("echo:" + message);
    }
    public function onBinary(session:WebSocketSession, data:Bytes):Void { session.sendBinary(data); }
    public function onClose(session:WebSocketSession, code:Int, reason:String):Void {}
    public function onError(session:WebSocketSession, error:Exception):Void { session.send("handled-error"); }
}
