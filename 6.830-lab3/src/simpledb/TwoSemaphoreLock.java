package simpledb;

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Semaphore;

/**
 * Created by musteryu on 2016/11/14.
 */
public class TwoSemaphoreLock extends TpLock {
    private Semaphore readSema, writeSema;
    private boolean exclusive;
    private Set<TransactionId> holders;
    private Set<TransactionId> acquirers;
    private int readNum;


    public TwoSemaphoreLock() {
        readSema = new Semaphore(1);
        writeSema = new Semaphore(1);
        exclusive = false;
        holders = new HashSet<TransactionId>();
        acquirers = new HashSet<TransactionId>();
        readNum = 0;
    }

    public synchronized void readLock(TransactionId tid) {
        if (holders.contains(tid)) return;
        try {
            acquirers.add(tid);
            writeSema.acquire();
            if (readNum++ == 0)
                readSema.acquire();
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        } finally {
            writeSema.release();
            acquirers.remove(tid);
        }
        holders.add(tid);
        exclusive = false;
    }

    public synchronized void writeLock(TransactionId tid) {
        if (holders.contains(tid) && exclusive) return;
        try {
            acquirers.add(tid);
            if (holders.contains(tid)) {
                writeSema.acquire();
                if (holders().size() == 1 && acquirers.size() == 1)
                    readSema.release();
            } else {
                writeSema.acquire();
                readSema.acquire();
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
            return;
        } finally {
            acquirers.remove(tid);
            writeSema.release();
            if (!holders.contains(tid)) readSema.release();
        }
        holders.add(tid);
        exclusive = true;
    }

    public synchronized void readUnlock(TransactionId tid) {
        if (--readNum == 0)
            readSema.release();
        holders().remove(tid);
    }

    public synchronized void writeUnlock(TransactionId tid) {
        writeSema.release();
        readSema.release();
        holders().remove(tid);
        exclusive = false;
    }

    public synchronized void unlock(TransactionId tid) {
        if (!holders.contains(tid)) return;
        if (exclusive)
            writeUnlock(tid);
        else readUnlock(tid);
    }

    public synchronized Set<TransactionId> holders() {
        return holders;
    }

    public synchronized boolean isExclusive() {
        return exclusive;
    }
}
