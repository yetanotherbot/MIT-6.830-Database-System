package simpledb;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by musteryu on 2016/11/13.
 */
public abstract class TpLock {

    Set<TransactionId> holders;
    Map<TransactionId, Boolean> acquirers;
    boolean exclusive;

    public TpLock() {
        holders = new HashSet<TransactionId>();
        acquirers = new HashMap<TransactionId, Boolean>();
        exclusive = false;
    }

    abstract void readLock(TransactionId tid);

    abstract void writeLock(TransactionId tid);

    abstract void unlock(TransactionId tid);

    public Set<TransactionId> holders() {
        return holders;
    }

    public Set<TransactionId> acquirers() {
        return acquirers.keySet();
    }

    public boolean isExclusive() {
        return exclusive;
    }

    boolean heldBy(TransactionId tid) {
        return holders().contains(tid);
    }
}
