package hx.well.http.driver.crossbyte;

import hx.well.server.instance.AbstractInstance;

class CrossbyteInstance extends AbstractInstance<CrossbyteDriver, CrossbyteDriverConfig> {
    public static function builder():CrossbyteInstanceBuilder { return new CrossbyteInstanceBuilder(); }
    public function driver():CrossbyteDriver { return new CrossbyteDriver(config); }
}
