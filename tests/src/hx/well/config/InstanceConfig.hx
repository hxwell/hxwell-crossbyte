package hx.well.config;

import hx.well.http.driver.crossbyte.CrossbyteInstance;
import hx.well.server.instance.IInstance;

class InstanceConfig implements IConfig {
    public static var driver:hx.well.http.driver.crossbyte.CrossbyteDriver;
    public static var tlsDriver:hx.well.http.driver.crossbyte.CrossbyteDriver;
    public function new() {}
    public function get():Array<IInstance> {
        var instance = CrossbyteInstance.builder().setHost("127.0.0.1").setPort(0)
            .setMaxConnections(128).setMaxConcurrentRequests(32).setWriteTimeout(3).setShutdownTimeout(1)
            .configureHttp(http -> {
                http.http2Enabled = true;
                http.runtimeCount = 2;
                http.rateLimitKey = _ -> null;
                http.compression.minimumSize = 32;
                http.maxOutputBufferSize = 128 * 1024;
            })
            .setOnStart(() -> {
                sys.io.File.saveContent("ready.json", haxe.Json.stringify({port:driver.localPort}));
            }).build();
        driver = instance.driver();
        var instances:Array<IInstance> = [new TestInstance(driver)];
        var cert = Sys.getEnv("CROSSBYTE_TEST_CERT");
        if(cert != null) {
            var tls = CrossbyteInstance.builder().setPort(0).setTls(cert, Sys.getEnv("CROSSBYTE_TEST_KEY"))
                .setShutdownTimeout(1)
                .configureHttp(http -> { http.http2Enabled = true; http.rateLimitKey = _ -> null; })
                .setOnStart(() -> sys.io.File.saveContent("tls-ready.json", haxe.Json.stringify({port:tlsDriver.localPort})))
                .build();
            tlsDriver = tls.driver();
            instances.push(new TestInstance(tlsDriver));
        }
        return instances;
    }
}

private class TestInstance implements IInstance {
    var instance:hx.well.http.driver.crossbyte.CrossbyteDriver;
    public function new(instance:hx.well.http.driver.crossbyte.CrossbyteDriver) { this.instance = instance; }
    public function driver():Dynamic {
        // Force the secondary listener to call CrossByte.make first. This
        // reproduces lazy EntryPoint initialization on the wrong JVM thread.
        if(instance == InstanceConfig.driver && InstanceConfig.tlsDriver != null) Sys.sleep(0.05);
        return instance;
    }
}
