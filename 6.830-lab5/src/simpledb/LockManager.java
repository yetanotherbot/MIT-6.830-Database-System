package simpledb;


import java.util.*;

/**
 * Created by musteryu on 2016/11/10.
 */

public class LockManager {

    Map<PageId, TpLock> pageToLocks;
    Map<TransactionId, Set<TransactionId>> preGraph;
    Map<TransactionId, Set<PageId>> pagesHeldByTid;

    public LockManager() {
        pageToLocks = new HashMap<PageId, TpLock>();
        preGraph = new HashMap<TransactionId, Set<TransactionId>>();
        pagesHeldByTid = new HashMap<TransactionId, Set<PageId>>();
    }

    public void acquireReadLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        TpLock lock;
//        System.out.println("acquireReadLock tid: " + tid + " pid: " + pid);
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.heldBy(tid)) return;
            if (!lock.holders().isEmpty() && lock.isExclusive()) {
                preGraph.put(tid, lock.holders());
                if (hasDeadLock(tid)) {
                    preGraph.remove(tid);
                    throw new TransactionAbortedException();
                }
            }
        }
        lock.readLock(tid);
        synchronized (this) {
            preGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public void acquireWriteLock(TransactionId tid, PageId pid)
            throws TransactionAbortedException {
        TpLock lock;
//        System.out.println("acquireWriteLock tid: " + tid + " pid: " + pid);
        synchronized (this) {
            lock = getOrCreateLock(pid);
            if (lock.isExclusive() && lock.heldBy(tid))
                return;
            preGraph.put(tid, lock.holders());
            if (hasDeadLock(tid)) {
                preGraph.remove(tid);
                throw new TransactionAbortedException();
            }
        }
        lock.writeLock(tid);
        synchronized (this) {
            preGraph.remove(tid);
            getOrCreatePagesHeld(tid).add(pid);
        }
    }

    public synchronized void releaseLock(TransactionId tid, PageId pid) {
//        System.out.println("releaseLock tid: " + tid + " pid: " + pid);
        if (!pageToLocks.containsKey(pid)) return;
        TpLock lock = pageToLocks.get(pid);
        lock.unlock(tid);
        pagesHeldByTid.get(tid).remove(pid);
    }

    public synchronized void releaseAllLocks(TransactionId tid) {
//        System.out.println("releaseAllLocks tid: " + tid);
        if (!pagesHeldByTid.containsKey(tid)) return;
        Set<PageId> pages = pagesHeldByTid.get(tid);
        for (Object pageId: pages.toArray()) {
            releaseLock(tid, ((PageId) pageId));
        }
        pagesHeldByTid.remove(tid);
    }

    private TpLock getOrCreateLock(PageId pageId) {
        if (!pageToLocks.containsKey(pageId))
            pageToLocks.put(pageId, new UpgradableLock());
        return pageToLocks.get(pageId);
    }

    private Set<PageId> getOrCreatePagesHeld(TransactionId tid) {
        if (!pagesHeldByTid.containsKey(tid))
            pagesHeldByTid.put(tid, new HashSet<PageId>());
        return pagesHeldByTid.get(tid);
    }

    private boolean hasDeadLock(TransactionId tid) {
        Set<TransactionId> visited = new HashSet<TransactionId>();
        Queue<TransactionId> q = new LinkedList<TransactionId>();
        visited.add(tid);
        q.offer(tid);
        while (!q.isEmpty()) {
            TransactionId head = q.poll();
            if (!preGraph.containsKey(head)) continue;
            for (TransactionId adj: preGraph.get(head)) {
                if (adj.equals(head)) continue;
                /* TODO: 2016/11/17 I am not sure...
                   self-loop should not cause deadlock
                 */
                if (!visited.contains(adj)) {
                    visited.add(adj);
                    q.offer(adj);
                } else if (adj.equals(tid)) {
                    System.err.println("DEAD LOCK: " + tid);
                    return true;
                }
            }
        }
        return false;
    }

    public boolean holdsLock(TransactionId tid, PageId pid) {
        return pagesHeldByTid.containsKey(tid)
                && pagesHeldByTid.get(tid).contains(pid);
    }

    public Set<PageId> getPagesHeldBy(TransactionId tid) {
        if (pagesHeldByTid.containsKey(tid))
            return pagesHeldByTid.get(tid);
        return null;
    }
}
