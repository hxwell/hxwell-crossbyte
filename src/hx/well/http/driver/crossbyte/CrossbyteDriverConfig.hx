package hx.well.http.driver.crossbyte;

import crossbyte.http.HTTPServerConfig;
import hx.well.http.driver.AbstractDriverConfig;

class CrossbyteDriverConfig extends AbstractDriverConfig {
    /** Maximum active HxWell contexts, including deferred responses. */
    public var maxConcurrentRequests:Int = 256;
    /** How long a stalled response writer may wait for the client/runtime. */
    public var writeTimeoutSeconds:Float = 30;
    public var shutdownTimeoutSeconds:Float = 5;
    public var tlsCertificatePath:String;
    public var tlsKeyPath:String;
    public var maxWebSocketMessageSize:Int = 1024 * 1024;
    public var maxWebSocketQueuedBytes:Int = 2 * 1024 * 1024;
    public var maxWebSocketQueuedMessages:Int = 128;
    public var maxWebSocketOutputBytes:Int = 8 * 1024 * 1024;
    public var webSocketCloseTimeoutSeconds:Float = 1;
    /** Native HTTP/2, compression, rate limits, timeouts and runtime spreading. */
    public var configureHttp:HTTPServerConfig->Void = _ -> {};
}
