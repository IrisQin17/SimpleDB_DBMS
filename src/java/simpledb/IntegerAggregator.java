package simpledb;

import java.util.HashMap;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Knows how to compute some aggregate over a set of IntFields.
 */
public class IntegerAggregator implements Aggregator {

    private static final long serialVersionUID = 1L;
    private final int gbfield;
    private final Type gbfieldtype;
    private final int afield;
    private final Op op;
    private HashMap<Field, Integer> aggVals;
    private HashMap<Field, Integer> groupTuples;

    /**
     * Aggregate constructor
     * 
     * @param gbfield
     *            the 0-based index of the group-by field in the tuple, or
     *            NO_GROUPING if there is no grouping
     * @param gbfieldtype
     *            the type of the group by field (e.g., Type.INT_TYPE), or null
     *            if there is no grouping
     * @param afield
     *            the 0-based index of the aggregate field in the tuple
     * @param what
     *            the aggregation operator
     */

    public IntegerAggregator(int gbfield, Type gbfieldtype, int afield, Op what) {
        // some code goes here
        this.gbfield = gbfield;
        this.gbfieldtype = gbfieldtype;
        this.afield = afield;
        this.op = what;
        aggVals = new HashMap<>();
        if (what == Op.AVG)
            groupTuples = new HashMap<>();
    }

    /**
     * Merge a new tuple into the aggregate, grouping as indicated in the
     * constructor
     * 
     * @param tup
     *            the Tuple containing an aggregate field and a group-by field
     */
    public void mergeTupleIntoGroup(Tuple tup) {
        // some code goes here
        Field groupField = (gbfield == Aggregator.NO_GROUPING) ? null : tup.getField(gbfield);
        Integer aggValue = ((IntField) tup.getField(afield)).getValue();
        switch (op) {
            case MIN:
                aggVals.merge(groupField, aggValue, (oldMin, val) -> Math.min(oldMin, val));
                break;
            case MAX:
                aggVals.merge(groupField, aggValue, (oldMax, val) -> Math.max(oldMax, val));
                break;
            case SUM:
                aggVals.merge(groupField, aggValue, (oldSum, val) -> oldSum + val);
                break;
            case AVG:
                aggVals.merge(groupField, aggValue, (oldAvg, val) -> oldAvg + val);
//                System.out.println(aggVals.toString() + " "+ groupTuples.toString());
                groupTuples.merge(groupField, 1, (oldCount, val) -> oldCount + 1);
                break;
            case COUNT:
                aggVals.merge(groupField, 1, (oldCount, val) -> oldCount + 1);
                break;
        }
    }

    /**
     * Create a DbIterator over group aggregate results.
     * 
     * @return a DbIterator whose tuples are the pair (groupVal, aggregateVal)
     *         if using group, or a single (aggregateVal) if no grouping. The
     *         aggregateVal is determined by the type of aggregate specified in
     *         the constructor.
     */
    public DbIterator iterator() {
        // some code goes here
        return new DbIterator() {
            private final TupleDesc td = (gbfield == Aggregator.NO_GROUPING) ?
                    new TupleDesc(new Type[]{Type.INT_TYPE}) : new TupleDesc(new Type[]{gbfieldtype, Type.INT_TYPE});

            private boolean open;
            private Tuple[] tuples;
            private int currentIdx = 0;

            @Override
            public void open() throws DbException, TransactionAbortedException {
                open = true;
                tuples = new Tuple[aggVals.size()];
                if (gbfield == Aggregator.NO_GROUPING) {
                    IntField aggregateVal = (op == Op.AVG) ?
                            new IntField(aggVals.get(null) / groupTuples.get(null)) : new IntField(aggVals.get(null));
                    tuples[0] = new Tuple(td);
                    tuples[0].setField(0, aggregateVal);
                } else {
                    int i = 0;
                    for (Map.Entry<Field, Integer> entry : aggVals.entrySet()) {
                        IntField aggregateVal = (op == Op.AVG) ?
                                new IntField(entry.getValue() / groupTuples.get(entry.getKey())) :
                                new IntField(entry.getValue());
                        tuples[i] = new Tuple(td);
                        tuples[i].setField(0, entry.getKey());
                        tuples[i].setField(1, aggregateVal);
                        i++;
                    }
                }
            }

            @Override
            public boolean hasNext() throws DbException, TransactionAbortedException {
                if (open)
                    return currentIdx < tuples.length;
                throw new IllegalStateException("Operator not yet open");
            }

            @Override
            public Tuple next() throws DbException, TransactionAbortedException, NoSuchElementException {
                if (open)
                    if (currentIdx < tuples.length)
                        return tuples[currentIdx++];
                    else
                        throw new NoSuchElementException();
                throw new IllegalStateException("Operator not yet open");
            }

            @Override
            public void rewind() throws DbException, TransactionAbortedException {
                if (open)
                    currentIdx = 0;
            }

            @Override
            public TupleDesc getTupleDesc() {
                return td;
            }

            @Override
            public void close() {
                open = false;
            }
        };
    }

}
