package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * Created by musteryu on 2016/11/12.
 */
public class SemaphoreTpLock extends TpLock {
    private Semaphore semaphore;
    private int readNum;


    public SemaphoreTpLock() {
        super();
        semaphore = new Semaphore(1);
        readNum = 0;
    }

    public synchronized void readLock(TransactionId tid) {
        if (holders.contains(tid)) return;
        acquirers.put(tid, false);
        if (readNum++ == 0) {
            try {
                semaphore.acquire();
            } catch (InterruptedException e) {
                e.printStackTrace();
                acquirers.remove(tid);
            }
        }
        acquirers.remove(tid);
        holders.add(tid);
        exclusive = false;
    }

    public synchronized void writeLock(TransactionId tid) {
        if (holders.contains(tid) && exclusive) return;
        acquirers.put(tid, true);
        try {
            semaphore.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            acquirers.remove(tid);
        }
        acquirers.remove(tid);
        // holders.clear();
        holders.add(tid);
        exclusive = true;
    }

    public synchronized void readUnlock(TransactionId tid) {
        if (--readNum == 0)
            semaphore.release();
        holders().remove(tid);
    }

    public synchronized void writeUnlock(TransactionId tid) {
        semaphore.release();
        holders().remove(tid);
        exclusive = false;
    }

    public synchronized void unlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        if (exclusive)
            writeUnlock(tid);
        else readUnlock(tid);
    }
}
