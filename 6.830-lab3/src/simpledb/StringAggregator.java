package simpledb;

import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.Map;

/**
 * Knows how to compute some aggregate over a set of StringFields.
 */
public class StringAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbfield the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbfieldtype the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param afield the 0-based index of the aggregate field in the tuple
     * @param what aggregation operator to use -- only supports COUNT
     * @throws IllegalArgumentException if what != COUNT
     */
    private HashMap<Field, Integer> count;
    private int gbFieldId, aFieldId;
    private Type gbFieldType;
    private Op what;
    private static final Field NO_GROUPING_FIELD = new IntField(Integer.MAX_VALUE);

    public StringAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        gbFieldId = gbfield;
        gbFieldType = gbfieldtype;
        aFieldId = afield;
        if (what != Op.COUNT)
            throw new IllegalArgumentException("unsupported operator");
        this.what = what;
        count = new HashMap<Field, Integer>();
    }

    private Field initGroupby(Tuple tup) {
        if (gbFieldId == NO_GROUPING)
            return NO_GROUPING_FIELD;
        else
            return tup.getField(gbFieldId);
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        Field gbField = initGroupby(tup);
        if (! count.containsKey(gbField) )
            count.put(gbField, 1);
        else count.put(gbField, count.get(gbField) + 1);
    }

    /**
     * Create a DbIterator over group aggregate results.
     *
     * @return a DbIterator whose tuples are the pair (groupVal,
     *   aggregateVal) if using group, or a single (aggregateVal) if no
     *   grouping. The aggregateVal is determined by the type of
     *   aggregate specified in the constructor.
     */
    public DbIterator iterator() {
        TupleDesc td = new TupleDesc(
                gbFieldId == NO_GROUPING
                        ? new Type[] {Type.INT_TYPE}
                        : new Type[] {gbFieldType, Type.INT_TYPE}
        );
        List<Tuple> tuples = new LinkedList<Tuple>();
        for (Map.Entry<Field, Integer> entry: count.entrySet()) {
            Tuple tp = new Tuple(td);
            if (gbFieldId == NO_GROUPING)
                tp.setField(0, new IntField(entry.getValue()));
            else {
                tp.setField(0, entry.getKey());
                tp.setField(1, new IntField(entry.getValue()));
            }
            tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }
}
