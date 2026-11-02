package hx.well.http.driver.crossbyte.internal;

#if !jvm
import sys.thread.Deque;
import sys.thread.Lock;
import sys.thread.Thread;

/**
 * Request workers that wait on their queue and are woken by each submission.
 * hx.concurrent's pool polls an empty queue every millisecond instead, so a
 * request handed to an idle pool could wait that long before a worker saw it.
 */
class WorkerPool {
    // One task per item; an empty item tells a worker to stop.
    final tasks = new Deque<Array<Void->Void>>();
    final stopped = new Lock();
    final size:Int;

    public function new(size:Int) {
        this.size = size;
        for(_ in 0...size) Thread.create(work);
    }

    function work():Void {
        while(true) {
            var item = tasks.pop(true);
            if(item.length == 0) break;
            try item[0]()
            catch(error:Dynamic) trace(error);
        }
        stopped.release();
    }

    public function submit(task:Void->Void):Void {
        tasks.add([task]);
    }

    /** Stops each worker once it has run what was submitted before, waiting up to `timeout` seconds in all. */
    public function stop(timeout:Float = 5):Void {
        for(_ in 0...size) tasks.add([]);
        var deadline = haxe.Timer.stamp() + timeout;
        for(_ in 0...size) {
            var left = deadline - haxe.Timer.stamp();
            if(left <= 0 || !stopped.wait(left)) break;
        }
    }
}
#end
