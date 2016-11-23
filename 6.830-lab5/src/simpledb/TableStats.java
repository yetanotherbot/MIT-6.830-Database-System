package simpledb;


import java.util.Arrays;

/** TableStats represents statistics (e.g., histograms) about base tables in a query */
public class TableStats {
    
    /**
     * Number of bins for the histogram.
     * Feel free to increase this value over 100,
     * though our tests assume that you have at least 100 bins in your histograms.
     */
    static final int NUM_HIST_BINS = 100;

    private int ioCostPerPage;
    private int numPages, numTuples;
    private Object[] hists;
    /**
     * Create a new TableStats object, that keeps track of statistics on each column of a table
     * 
     * @param tableid The table over which to compute statistics
     * @param ioCostPerPage The cost per page of IO.  
     * 		                This doesn't differentiate between sequential-scan IO and disk seeks.
     */
    public TableStats (int tableid, int ioCostPerPage) {
        // For this function, you'll have to get the DbFile for the table in question,
    	// then scan through its tuples and calculate the values that you need.
    	// You should try to do this reasonably efficiently, but you don't necessarily
    	// have to (for example) do everything in a single scan of the table.
    	// some code goes here
        HeapFile file = ((HeapFile) Database.getCatalog().getDbFile(tableid));
        this.ioCostPerPage = ioCostPerPage;
        numPages = file.numPages();
        numTuples = 0;
        DbFileIterator itr = file.iterator(new TransactionId());
        TupleDesc td = file.getTupleDesc();
        int numFields = td.numFields();
        int[] mins = new int[numFields], maxs = new int[numFields];
        Arrays.fill(mins, Integer.MAX_VALUE);
        Arrays.fill(maxs, Integer.MIN_VALUE);
        hists = new Object[numFields];
        try {
            itr.open();
            while (itr.hasNext()) {
                Tuple t = itr.next();
                for (int i = 0; i < numFields; ++i) {
                    if (td.getType(i) == Type.INT_TYPE) {
                        int v = ((IntField) t.getField(i)).getValue();
                        mins[i] = Math.min(mins[i], v);
                        maxs[i] = Math.max(maxs[i], v);
                    }
                }
                ++numTuples;
            }
            for (int i = 0; i < numFields; ++i) {
                if (td.getType(i) == Type.INT_TYPE)
                    hists[i] = new IntHistogram(NUM_HIST_BINS, mins[i], maxs[i]);
                else
                    hists[i] = new StringHistogram(NUM_HIST_BINS);
            }
            itr.rewind();
            while (itr.hasNext()) {
                Tuple t = itr.next();
                for (int i = 0; i < numFields; ++i) {
                    if (td.getType(i) == Type.INT_TYPE)
                        ( (IntHistogram) hists[i] ).addValue(( (IntField) t.getField(i) ).getValue());
                    else
                        ( (StringHistogram) hists[i] ).addValue(( (StringField) t.getField(i) ).getValue());
                }
            }
            itr.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /** 
     * Estimates the
     * cost of sequentially scanning the file, given that the cost to read
     * a page is costPerPageIO.  You can assume that there are no
     * seeks and that no pages are in the buffer pool.
     * 
     * Also, assume that your hard drive can only read entire pages at once,
     * so if the last page of the table only has one tuple on it, it's just as
     * expensive to read as a full page.  (Most real hard drives can't efficiently
     * address regions smaller than a page at a time.)
     * 
     * @return The estimated cost of scanning the table.
     */ 
    public double estimateScanCost() {
    	return ioCostPerPage * numPages;
    }

    /** 
     * This method returns the number of tuples in the relation,
     * given that a predicate with selectivity selectivityFactor is
     * applied.
	 *
     * @param selectivityFactor The selectivity of any predicates over the table
     * @return The estimated cardinality of the scan with the specified selectivityFactor
     */
    public int estimateTableCardinality(double selectivityFactor) {
        return ((int) (numTuples * selectivityFactor));
    }

    /** 
     * Estimate the selectivity of predicate <tt>field op constant</tt> on the table.
     * 
     * @param field The field over which the predicate ranges
     * @param op The logical operation in the predicate
     * @param constant The value against which the field is compared
     * @return The estimated selectivity (fraction of tuples that satisfy) the predicate
     */
    public double estimateSelectivity(int field, Predicate.Op op, Field constant) {
        Object h = hists[field];
        if (constant.getType() == Type.INT_TYPE) {
            return ((IntHistogram) h).estimateSelectivity(op, ((IntField) constant).getValue());
        } else {
            return ((StringHistogram) h).estimateSelectivity(op, ((StringField) constant).getValue());
        }
    }

}
