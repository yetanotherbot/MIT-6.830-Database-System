package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection
 * of tuples in no particular order.  Tuples are stored on pages, each of
 * which is a fixed size, and the file is simply a collection of those
 * pages. HeapFile works closely with HeapPage.  The format of HeapPages
 * is described in the HeapPage constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private File file;
    private TupleDesc tupleDesc;
    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        return file;
    }

    /**
    * Returns an ID uniquely identifying this HeapFile. Implementation note:
    * you will need to generate this tableid somewhere ensure that each
    * HeapFile has a "unique id," and that you always return the same value
    * for a particular HeapFile. We suggest hashing the absolute file name of
    * the file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
    *
    * @return an ID uniquely identifying this HeapFile.
    */
    public int getId() {
        return file.getAbsoluteFile().hashCode();
    }
    
    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
    	return tupleDesc;
    }

    // see DbFile.java for javadocs
    public Page readPage(PageId pid) {
        int pageLen = BufferPool.PAGE_SIZE;
        int pageOffest = pid.pageno() * pageLen;
        byte[] pageData = new byte[pageLen];
        try {
            FileInputStream fis = new FileInputStream(file);
            fis.read(pageData, pageOffest, pageLen);
            return new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return (int) file.length() / BufferPool.PAGE_SIZE;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        // some code goes here
        return null;
        // not necessary for lab1
    }

    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        final BufferPool bufferPool = Database.getBufferPool();
        final TransactionId tid_ = tid;
        try {
            return new DbFileIterator() {
                int pageno = 0;
                HeapPage selectedPage = (HeapPage) bufferPool.getPage(tid_, new HeapPageId(getId(), pageno), Permissions.READ_ONLY);
                Iterator<Tuple> tupleIterator = selectedPage.iterator();


                @Override
                public void open() throws DbException, TransactionAbortedException {

                }

                @Override
                public boolean hasNext() throws DbException, TransactionAbortedException {
                    if (pageno < numPages()) return true;
                    else return tupleIterator.hasNext();
                }

                @Override
                public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                    if (!tupleIterator.hasNext())
                        selectedPage = (HeapPage) bufferPool.getPage(tid_, new HeapPageId(getId(), ++pageno), Permissions.READ_ONLY);
                    return tupleIterator.next();
                }

                @Override
                public void rewind() throws DbException, TransactionAbortedException {

                }

                @Override
                public void close() {

                }
            };
        } catch (DbException de) {

        } catch (TransactionAbortedException tae) {

        }
        return null;
    }
    
}

