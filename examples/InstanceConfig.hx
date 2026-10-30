package hx.well.config;

import hx.well.http.driver.crossbyte.CrossbyteInstance;
import hx.well.server.instance.IInstance;

class InstanceConfig implements IConfig {
    public function new() {}
    public function get():Array<IInstance> {
        return [CrossbyteInstance.builder()
            .setHost("0.0.0.0")
            .setPort(3000)
            .setMaxConnections(1000)
            .setMaxConcurrentRequests(256)
            .configureHttp(http -> {
                http.http2Enabled = true;
                http.requestTimeout = 15;
                http.compression.enabled = true;
            })
            .build()];
    }
}
