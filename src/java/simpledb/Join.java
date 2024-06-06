package simpledb;

import java.util.*;

/**
 * The Join operator implements the relational join operation.
 */
public class Join extends Operator {

    private static final long serialVersionUID = 1L;

    /**
     * Constructor. Accepts to children to join and the predicate to join them
     * on
     * 
     * @param p
     *            The predicate to use to join the children
     * @param child1
     *            Iterator for the left(outer) relation to join
     * @param child2
     *            Iterator for the right(inner) relation to join
     */
    private JoinPredicate p;
    private DbIterator[] children;
    private HashEquiJoin hashEquiJoin;


    public Join(JoinPredicate p, DbIterator child1, DbIterator child2) {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            this.p = p;
            hashEquiJoin = new HashEquiJoin(p, child1, child2);
        }
        else {
            this.p = p;
            children = new DbIterator[2];
            children[0] = child1;
            children[1] = child2;
        }
    }

    public JoinPredicate getJoinPredicate() {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.getJoinPredicate();
        }
        else return p;
    }

    /**
     * @return
     *       the field name of join field1. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField1Name() {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.getJoinField1Name();
        }
        else return children[0].getTupleDesc().getFieldName(p.getField1());
    }

    /**
     * @return
     *       the field name of join field2. Should be quantified by
     *       alias or table name.
     * */
    public String getJoinField2Name() {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.getJoinField2Name();
        }
        else return children[1].getTupleDesc().getFieldName(p.getField2());
    }

    /**
     * @see simpledb.TupleDesc#merge(TupleDesc, TupleDesc) for possible
     *      implementation logic.
     */
    public TupleDesc getTupleDesc() {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.getTupleDesc();
        }
        else return TupleDesc.merge(children[0].getTupleDesc(), children[1].getTupleDesc());
    }

    public void open() throws DbException, NoSuchElementException,
            TransactionAbortedException {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            hashEquiJoin.open();
        }
        else {
            children[0].open();
            children[1].open();
        }
        super.open();
    }

    public void close() {
        // some code goes here
        super.close();
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            hashEquiJoin.close();
        }
        else {
            children[0].close();
            children[1].close();
        }
    }

    public void rewind() throws DbException, TransactionAbortedException {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            hashEquiJoin.rewind();
        }
        else {
            children[0].rewind(); children[1].rewind();
            fetchTmp1 = null;
        }
    }

    /**
     * Returns the next tuple generated by the join, or null if there are no
     * more tuples. Logically, this is the next tuple in r1 cross r2 that
     * satisfies the join predicate. There are many possible implementations;
     * the simplest is a nested loops join.
     * <p>
     * Note that the tuples returned from this particular implementation of Join
     * are simply the concatenation of joining tuples from the left and right
     * relation. Therefore, if an equality predicate is used there will be two
     * copies of the join attribute in the results. (Removing such duplicate
     * columns can be done with an additional projection operator if needed.)
     * <p>
     * For example, if one tuple is {1,2,3} and the other tuple is {1,5,6},
     * joined on equality of the first column, then this returns {1,2,3,1,5,6}.
     * 
     * @return The next matching tuple.
     * @see JoinPredicate#filter
     */
    private Tuple fetchTmp1 = null;

    protected Tuple fetchNext() throws TransactionAbortedException, DbException {
        // some code goes here
        // todo: find a much better way to do this: temporary using nested join
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.fetchNext();
        }
        else {
            if (fetchTmp1 == null) {
                if (children[0].hasNext()) {
                    fetchTmp1 = children[0].next();
                } else return null;
            }
            do {
                while (children[1].hasNext()) {
                    Tuple fetchTmp2 = children[1].next();
                    if (p.filter(fetchTmp1, fetchTmp2)) {
                        return Tuple.mergeJoinTuples(fetchTmp1, fetchTmp2);
                    }
                }
                if (children[0].hasNext()) {
                    fetchTmp1 = children[0].next();
                    children[1].rewind();
                } else break;
            } while (true);
            return null;
        }
    }

    @Override
    public DbIterator[] getChildren() {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            return hashEquiJoin.getChildren();
        }
        else return children;
    }

    @Override
    public void setChildren(DbIterator[] children) {
        // some code goes here
        if (p.getOperator().equals(Predicate.Op.EQUALS)){
            hashEquiJoin.setChildren(children);
        }
        else this.children = children;
    }

}
