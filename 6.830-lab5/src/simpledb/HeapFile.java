package simpledb;

import java.io.*;
import java.nio.ByteBuffer;
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

    private int numPages;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f the file that stores the on-disk backing store for this heap file.
     */
    public HeapFile(File f, TupleDesc td) {
        file = f;
        tupleDesc = td;
        numPages = (int) f.length() / BufferPool.PAGE_SIZE;
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
        byte[] pageData = HeapPage.createEmptyPageData();
        Page page = null;
        try {
            FileInputStream fis = new FileInputStream(file);
            fis.skip(BufferPool.PAGE_SIZE * pid.pageno());
            if (fis.read(pageData) == BufferPool.PAGE_SIZE)
                page = new HeapPage((HeapPageId) pid, pageData);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return page;
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        FileOutputStream fos = new FileOutputStream(file);
        fos.getChannel()
                .position(page.getId().pageno() * BufferPool.PAGE_SIZE)
                .write(ByteBuffer.wrap(page.getPageData()));
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        return numPages;
    }

    // see DbFile.java for javadocs
    public ArrayList<Page> addTuple(TransactionId tid, Tuple t)
        throws DbException, IOException, TransactionAbortedException {
        if (t == null)
            throw new DbException("tuple to add is null");
        ArrayList<Page> dirtyPages = new ArrayList<Page>();
        HeapPage page;
        for (int pageno = 0; pageno <= numPages; ++pageno) {
            page = ((HeapPage) Database.getBufferPool().getPage(
                    tid, new HeapPageId(getId(), pageno), Permissions.READ_WRITE));
            if (page.getNumEmptySlots() > 0) {
                page.addTuple(t);
                dirtyPages.add(page);
                if (pageno == numPages)
                    ++numPages;
                break;
            }
        }
        return dirtyPages;
    }

    // see DbFile.java for javadocs
    public Page deleteTuple(TransactionId tid, Tuple t)
        throws DbException, TransactionAbortedException {
        PageId pid = t.getRecordId().getPageId();
        if (pid.getTableId() != getId())
            throw new DbException("the tuple to delete is not on the table");
        HeapPage page = ((HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE));
        page.deleteTuple(t);
        return page;
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

            public void open() throws DbException, TransactionAbortedException {
                open = true;
                setNextPage();
            }

            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (!open) return false;
                while (pageno < numPages && !tupleIterator.hasNext())
                    setNextPage();
                return (selectedPage != null);
            }

            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!open) throw new NoSuchElementException();
                return tupleIterator.next();
            }

            public void rewind() throws DbException, TransactionAbortedException {
                close();
                open();
            }
            
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
                    selectedPage = (HeapPage) Database.getBufferPool().getPage(
                            tid_, new HeapPageId(getId(), ++pageno), Permissions.READ_ONLY
                    );
                    tupleIterator = selectedPage.iterator();
                }
            }
        };
    }
    
}

