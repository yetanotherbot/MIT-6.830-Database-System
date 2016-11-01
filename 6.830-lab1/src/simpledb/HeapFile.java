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
    private BufferPool bufferPool = Database.getBufferPool();

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
        final TransactionId tid_ = tid;
        final int numPages = numPages();

        return new DbFileIterator() {
            boolean open = false;
            int pageno = -1;
            HeapPage selectedPage = null;
            Iterator<Tuple> tupleIterator = null;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                setNextPage();
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open) return false;
                while (pageno < numPages && !tupleIterator.hasNext())
                    setNextPage();
                return (selectedPage != null);
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!open) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {

            }

            @Override
            public void close() {
                selectedPage = null;
                tupleIterator = null;
                pageno = -1;
                open = false;
            }

            private void setNextPage() throws DbException, TransactionAbortedException {
                if (pageno == numPages - 1) {
                    ++pageno;
                    selectedPage = null;
                    tupleIterator = null;
                } else {
                    selectedPage = (HeapPage) bufferPool.getPage(
                            tid_, new HeapPageId(getId(), ++pageno), Permissions.READ_ONLY
                    );
                    tupleIterator = selectedPage.iterator();
                }
            }
        };
    }
    
}

