package simpledb;

/**
 * The delete operator.  Delete reads tuples from its child operator and
 * removes them from the table they belong to.
 */
public class Delete extends AbstractDbIterator {

    private TransactionId tid;
    private DbIterator child;
    private final static TupleDesc deleteTd;
    private boolean deleted;

    static {
        deleteTd = new TupleDesc(new Type[]{Type.INT_TYPE});
    }

    /**
     * Constructor specifying the transaction that this delete belongs to as
     * well as the child to read from.
     * @param t The transaction this delete runs in
     * @param child The child operator from which to read tuples for deletion
     */
    public Delete(TransactionId t, DbIterator child) {
        this.tid = t;
        this.child = child;
        this.deleted = false;
    }

    public TupleDesc getTupleDesc() {
        return deleteTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        deleted = false;
    }

    public void close() {
        child.close();
        deleted = true;
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        deleted = false;
    }

    /**
     * Deletes tuples as they are read from the child operator. Deletes are
     * processed via the buffer pool (which can be accessed via the
     * Database.getBufferPool() method.
     * @return A 1-field tuple containing the number of deleted records.
     * @see Database#getBufferPool
     * @see BufferPool#deleteTuple
     */
    protected Tuple readNext() throws TransactionAbortedException, DbException {
        if (deleted) return null;
        int count = 0;
        while (child.hasNext()) {
            Database.getBufferPool().deleteTuple(tid, child.next());
            count++;
        }
        Tuple tp = new Tuple(deleteTd);
        tp.setField(0, new IntField(count));
        deleted = true;
        return tp;
    }
}
