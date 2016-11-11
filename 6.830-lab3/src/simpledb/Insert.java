package simpledb;
import java.io.IOException;
import java.util.*;

/**
 * Inserts tuples read from the child operator into
 * the tableid specified in the constructor
 */
public class Insert extends AbstractDbIterator {

    private TransactionId t;
    private DbIterator child;
    private int tableid;
    private boolean inserted;
    private static final TupleDesc insertTd = new TupleDesc(new Type[] { Type.INT_TYPE });;
    /**
     * Constructor.
     * @param t The transaction running the insert.
     * @param child The child operator from which to read tuples to be inserted.
     * @param tableid The table in which to insert tuples.
     * @throws DbException if TupleDesc of child differs from table into which we are to insert.
     */
    public Insert(TransactionId t, DbIterator child, int tableid)
        throws DbException {
        this.t = t;
        this.child = child;
        this.tableid = tableid;
        if (!Database.getCatalog().getTupleDesc(tableid).equals(child.getTupleDesc()))
            throw new DbException("TupleDesc of child differs from table into which we are to insert.");
    }

    public TupleDesc getTupleDesc() {
//        return child.getTupleDesc();
        return insertTd;
    }

    public void open() throws DbException, TransactionAbortedException {
        child.open();
        inserted = false;
    }

    public void close() {
        child.close();
    }

    public void rewind() throws DbException, TransactionAbortedException {
        child.rewind();
        inserted = false;
    }

    /**
     * Inserts tuples read from child into the tableid specified by the
     * constructor. It returns a one field tuple containing the number of
     * inserted records. Inserts should be passed through BufferPool.
     * An instances of BufferPool is available via Database.getBufferPool().
     * Note that insert DOES NOT need check to see if a particular tuple is
     * a duplicate before inserting it.
     *
     * @return A 1-field tuple containing the number of inserted records, or
    * null if called more than once.
     * @see Database#getBufferPool
     * @see BufferPool#insertTuple
     */
    protected Tuple readNext()
            throws TransactionAbortedException, DbException {
        if (inserted) return null;
        int count = 0;
        while (child.hasNext()) {
            try {
                Database.getBufferPool().insertTuple(t, tableid, child.next());
                ++count;
            } catch (IOException ioe) {
                ioe.printStackTrace();
            }
        }
        Tuple tp = new Tuple(insertTd);
        tp.setField(0, new IntField(count));
        inserted = true;
        return tp;
    }
}
