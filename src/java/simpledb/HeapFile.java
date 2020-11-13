package simpledb;

import java.io.*;
import java.util.*;

/**
 * HeapFile is an implementation of a DbFile that stores a collection of tuples
 * in no particular order. Tuples are stored on pages, each of which is a fixed
 * size, and the file is simply a collection of those pages. HeapFile works
 * closely with HeapPage. The format of HeapPages is described in the HeapPage
 * constructor.
 *
 * @see simpledb.HeapPage#HeapPage
 * @author Sam Madden
 */
public class HeapFile implements DbFile {

    private final File f;
    private final TupleDesc td;

    /**
     * Constructs a heap file backed by the specified file.
     *
     * @param f
     *            the file that stores the on-disk backing store for this heap
     *            file.
     */
    public HeapFile(File f, TupleDesc td) {
        // some code goes here
        this.f = f;
        this.td = td;
    }

    /**
     * Returns the File backing this HeapFile on disk.
     *
     * @return the File backing this HeapFile on disk.
     */
    public File getFile() {
        // some code goes here
        return f;
    }

    /**
     * Returns an ID uniquely identifying this HeapFile. Implementation note:
     * you will need to generate this tableid somewhere ensure that each
     * HeapFile has a "unique id," and that you always return the same value for
     * a particular HeapFile. We suggest hashing the absolute file name of the
     * file underlying the heapfile, i.e. f.getAbsoluteFile().hashCode().
     *
     * @return an ID uniquely identifying this HeapFile.
     */
    public int getId() {
        // some code goes here
        return f.getAbsoluteFile().hashCode();
    }

    /**
     * Returns the TupleDesc of the table stored in this DbFile.
     *
     * @return TupleDesc of this DbFile.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        return td;
    }

    /**
     * Read the specified page from disk.
     *
     * @throws IllegalArgumentException if the page does not exist in this file.
     */
    public Page readPage(PageId pid) {
        // some code goes here
        if (getId() == pid.getTableId()) {      // if the page exist in this file.
            byte[] data = HeapPage.createEmptyPageData();

            // try(condition){} with resources, will auto call raf.close() when exit try
            try (RandomAccessFile raf = new RandomAccessFile(getFile(), "r")) {
                int offset = pid.pageNumber() * BufferPool.getPageSize();
                raf.seek(offset);
                raf.read(data, 0, BufferPool.getPageSize());
                return new HeapPage((HeapPageId) pid, data);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        throw new IllegalArgumentException();
    }

    // see DbFile.java for javadocs
    public void writePage(Page page) throws IOException {
        // some code goes here
        // not necessary for lab1
        HeapPageId id = (HeapPageId) page.getId();
        try (RandomAccessFile raf = new RandomAccessFile(getFile(), "rw")) {
            int offset = page.getId().pageNumber() * BufferPool.getPageSize();
            raf.seek(offset);
            raf.write(page.getPageData());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * Returns the number of pages in this HeapFile.
     */
    public int numPages() {
        // some code goes here
        return (int)f.length() / BufferPool.getPageSize();
    }

    /**
     * Inserts the specified tuple to the file on behalf of transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to add.  This tuple should be updated to reflect that
     *          it is now stored in this file.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be added
     * @throws IOException if the needed file can't be read/written
     */
    public ArrayList<Page> insertTuple(TransactionId tid, Tuple t) throws DbException, IOException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        boolean isFull = true;
        HeapPage heapPage = null;
        ArrayList<Page> modifiedPages = new ArrayList<>();
        for (int i = 0; i < numPages(); i++) {
            heapPage = (HeapPage) Database.getBufferPool().getPage(tid, new HeapPageId(getId(), i), Permissions.READ_ONLY);
            if (heapPage.getNumEmptySlots() > 0) {  // find empty page and insert tuple
                isFull = false;
                break;
            }
        }
        if (isFull) {
            heapPage = new HeapPage(new HeapPageId(getId(), numPages()), HeapPage.createEmptyPageData());
            writePage(heapPage);        // write into disk
        }
        heapPage = (HeapPage) Database.getBufferPool().getPage(tid, heapPage.getId(), Permissions.READ_WRITE);
        heapPage.insertTuple(t);
//        heapPage.markDirty(true, tid);
        modifiedPages.add(heapPage);
        return modifiedPages;
    }

    /**
     * Removes the specified tuple from the file on behalf of the specified
     * transaction.
     * This method will acquire a lock on the affected pages of the file, and
     * may block until the lock can be acquired.
     *
     * @param tid The transaction performing the update
     * @param t The tuple to delete.  This tuple should be updated to reflect that
     *          it is no longer stored on any page.
     * @return An ArrayList contain the pages that were modified
     * @throws DbException if the tuple cannot be deleted or is not a member
     *   of the file
     */
    public ArrayList<Page> deleteTuple(TransactionId tid, Tuple t) throws DbException, TransactionAbortedException {
        // some code goes here
        // not necessary for lab1
        ArrayList<Page> modifiedPages = new ArrayList<>();
        PageId pid = t.getRecordId().getPageId();
        for (int i = 0; i < numPages(); i++) {
            if (i == pid.pageNumber()) {
                HeapPage page = (HeapPage) Database.getBufferPool().getPage(tid, pid, Permissions.READ_WRITE);
                page.deleteTuple(t);
//                page.markDirty(true, tid);
                modifiedPages.add(page);
                return modifiedPages;
            }
        }
        throw new DbException("the tuple cannot be deleted or is not a member of the file!");
    }

    /**
     * Returns an iterator over all the tuples stored in this DbFile. The
     * iterator must use {@link BufferPool#getPage}, rather than
     * {@link #readPage} to iterate through the pages.
     *
     * @return an iterator over all the tuples stored in this DbFile.
     */
    // see DbFile.java for javadocs
    public DbFileIterator iterator(TransactionId tid) {
        // some code goes here
        return new DbFileIterator() {
            private int pNo = -1;
            private Iterator<Tuple> pageIterator;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                pNo = 0;
                pageIterator = null;
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (null != pageIterator && pageIterator.hasNext()) {
                    return true;
                } else if (pNo < 0 || pNo >= numPages()) {         // closed or run out of pages
                    return false;
                }
                pageIterator = ((HeapPage)Database.getBufferPool().getPage(tid, new HeapPageId(getId(), pNo++),
                        Permissions.READ_ONLY)).iterator();         // load next page
                return hasNext();
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (!hasNext()) {
                    throw new NoSuchElementException();
                }
                return pageIterator.next();
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                open();
            }

            @Override
            public void close() {
                pNo = -1;                     // mark as closed
                pageIterator = null;
            }
        };
    }

}

