package simpledb;

import java.util.*;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntAggregator implements Aggregator {

    /**
     * Aggregate constructor
     * @param gbFieldId the 0-based index of the group-by field in the tuple, or NO_GROUPING if there is no grouping
     * @param gbFieldType the type of the group by field (e.g., Type.INT_TYPE), or null if there is no grouping
     * @param aFieldId the 0-based index of the aggregate field in the tuple
     * @param what the aggregation operator
     */
    private int gbFieldId, aFieldId;
    private Type gbFieldType;
    private Op what;
    private HashMap<Field, Integer> vals, cnts;
    private TupleDesc baseTd = null;

    static final Field NO_GROUPPING_FIELD = new IntField(Integer.MAX_VALUE);

    public IntAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        this.gbFieldId = gbfield;
        this.gbFieldType = gbfieldtype;
        this.aFieldId = afield;
        this.what = what;
        vals = new HashMap<Field, Integer>();
        cnts = new HashMap<Field, Integer>();
    }

    private void validate(Tuple tup) {
        if (baseTd == null)
            baseTd = tup.getTupleDesc();
        if (!tup.getTupleDesc().equals(baseTd))
            throw new IllegalArgumentException("not compatible tuple");
        if (baseTd.numFields() < gbFieldId || baseTd.numFields() < aFieldId)
            throw new IllegalArgumentException("length of tuple is too short");
    }

    private Field initGroupby(Tuple tup) {
        Field gbField;
        if (gbFieldId == NO_GROUPING) {
            gbField = NO_GROUPPING_FIELD;
        } else {
            gbField = tup.getField(gbFieldId);
        }
        return gbField;
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the constructor
     * @param tup the Tuple containing an aggregate field and a group-by field
     */
    public void merge(Tuple tup) {
        validate(tup);
        int rawValue = ((IntField) tup.getField(aFieldId)).getValue();
        Field gbField = initGroupby(tup);
        if (! cnts.containsKey(gbField) ) {
            cnts.put(gbField, 1);
            vals.put(gbField, rawValue);
        } else {
            cnts.put(gbField, cnts.get(gbField) + 1);
            vals.put(gbField, aggr(vals.get(gbField), rawValue));
        }
    }

    private int aggr(int a, int b) {
        switch (what) {
            case COUNT: case AVG: case SUM:
                return a + b;
            case MAX:
                return Math.max(a, b);
            case MIN:
                return Math.min(a, b);
            default: return Integer.MAX_VALUE;
        }
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
        for (Field field: cnts.keySet()) {
            int val = vals.get(field);
            int cnt = cnts.get(field);
            Tuple tp = new Tuple(td);
            if (td.numFields() == 1)
                switch (what) {
                    case COUNT:
                        tp.setField(0, new IntField(cnt));
                        break;
                    case AVG:
                        tp.setField(0, new IntField(val / cnt));
                        break;
                    default:
                        tp.setField(0, new IntField(val));
                        break;
                }
            else {
                tp.setField(0, field);
                switch (what) {
                    case COUNT:
                        tp.setField(1, new IntField(cnt));
                        break;
                    case AVG:
                        tp.setField(1, new IntField(val / cnt));
                        break;
                    default:
                        tp.setField(1, new IntField(val));
                        break;
                }
            }
            tuples.add(tp);
        }
        return new TupleIterator(td, tuples);
    }
}
