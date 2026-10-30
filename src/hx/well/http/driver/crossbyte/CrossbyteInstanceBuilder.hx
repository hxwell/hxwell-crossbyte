package hx.well.http.driver.crossbyte;

import crossbyte.http.HTTPServerConfig;
import hx.well.http.driver.AbstractInstanceBuilder;
import hx.well.server.instance.IInstance;
import hx.well.http.driver.crossbyte.internal.RuntimeHost;

class CrossbyteInstanceBuilder extends AbstractInstanceBuilder<CrossbyteInstanceBuilder, CrossbyteDriverConfig> {
    public function new() { super(new CrossbyteDriverConfig()); }

    public function setMaxConcurrentRequests(value:Int):CrossbyteInstanceBuilder {
        config.maxConcurrentRequests = value;
        return this;
    }

    public function setWriteTimeout(seconds:Float):CrossbyteInstanceBuilder {
        config.writeTimeoutSeconds = seconds;
        return this;
    }

    public function setShutdownTimeout(seconds:Float):CrossbyteInstanceBuilder {
        config.shutdownTimeoutSeconds = seconds;
        return this;
    }

    public function setTls(certificatePath:String, keyPath:String):CrossbyteInstanceBuilder {
        config.ssl = true;
        config.tlsCertificatePath = certificatePath;
        config.tlsKeyPath = keyPath;
        return this;
    }

    public function configureHttp(callback:HTTPServerConfig->Void):CrossbyteInstanceBuilder {
        config.configureHttp = callback;
        return this;
    }

    public function build():IInstance {
        // InstanceConfig is evaluated on HxWell's main thread, before it starts
        // secondary listeners on background threads.
        RuntimeHost.initialize();
        return new CrossbyteInstance(config);
    }
}
