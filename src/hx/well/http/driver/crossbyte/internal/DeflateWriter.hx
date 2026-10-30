package hx.well.http.driver.crossbyte.internal;

import haxe.io.Bytes;
import haxe.Exception;

/** Zlib bytes only; HTTP chunking is always the transport's responsibility. */
class DeflateWriter {
    var output = Bytes.alloc(65536);
    var emit:Bytes->Int->Int->Void;
    var ended = false;
    #if jvm
    var compressor:java.util.zip.Deflater;
    #else
    var compressor:haxe.zip.Compress;
    #end

    public function new(level:Int, emit:Bytes->Int->Int->Void) {
        this.emit = emit;
        #if jvm
        compressor = new java.util.zip.Deflater(level);
        #else
        compressor = new haxe.zip.Compress(level);
        compressor.setFlushMode(SYNC);
        #end
    }

    public function write(bytes:Bytes, pos:Int, length:Int):Void {
        if(ended) throw new Exception("Compressor is closed");
        #if jvm
        compressor.setInput(bytes.getData(), pos, length);
        var written:Int;
        do {
            written = compressor.deflate(output.getData(), 0, output.length, java.util.zip.Deflater.SYNC_FLUSH);
            if(written > 0) emit(output, 0, written);
            if(written == 0 && !compressor.needsInput()) throw new Exception("Compressor made no progress");
        } while(written == output.length || !compressor.needsInput());
        #else
        var source = bytes.sub(pos, length);
        var at = 0;
        var result:{done:Bool, read:Int, write:Int};
        do {
            result = compressor.execute(source, at, output, 0);
            at += result.read;
            if(result.write > 0) emit(output, 0, result.write);
            if(result.read == 0 && result.write == 0 && at < source.length) throw new Exception("Compressor made no progress");
        } while(at < source.length || result.write == output.length);
        #end
    }

    public function finish():Void {
        if(ended) return;
        #if jvm
        compressor.finish();
        while(!compressor.finished()) {
            var written = compressor.deflate(output.getData(), 0, output.length);
            if(written > 0) emit(output, 0, written);
            if(written == 0 && !compressor.finished()) throw new Exception("Compressor did not finish");
        }
        #else
        compressor.setFlushMode(FINISH);
        while(true) {
            var result = compressor.execute(Bytes.alloc(0), 0, output, 0);
            if(result.write > 0) emit(output, 0, result.write);
            if(result.done) break;
            if(result.write == 0) throw new Exception("Compressor did not finish");
        }
        #end
        dispose();
    }

    public function dispose():Void {
        if(ended) return;
        ended = true;
        #if jvm
        compressor.end();
        #else
        compressor.close();
        #end
    }
}
