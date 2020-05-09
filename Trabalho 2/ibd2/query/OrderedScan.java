/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;

/**
 *
 * @author Sergio
 */
public class OrderedScan implements UnaryOperation {


    Operation op;

    Tuple nextTuple = null;

    int currentIndex = -1;

    ArrayList<Tuple> tt;

    boolean opened = false;

    public OrderedScan(Operation op) throws Exception {
        this.op = op;
    }

    @Override
    public void open() throws Exception {
        currentIndex = -1;

        if (opened) return;

        tt = new ArrayList<>();
        op.open();
        while (op.hasNext()){
            Tuple tuple = (Tuple)op.next();
            tt.add(tuple);

        }

        Collections.sort(tt, new TupleComparator() );

        opened = true;

    }

    @Override
    public Operation getOperation() {
        return op;
    }

    public class TupleComparator implements Comparator<Tuple> {

    @Override
    public int compare(Tuple tt1, Tuple tt2) {
       return Long.compare(tt1.primaryKey, tt2.primaryKey);
    }
}

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        currentIndex++;
        if (currentIndex == tt.size()) {
            throw new Exception("No records after this point");
        }


        return tt.get(currentIndex);


    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null)
            return true;

        if (currentIndex+1 >= tt.size())
            return false;

        currentIndex++;
        nextTuple = tt.get(currentIndex);
        return true;
    }

    @Override
    public void close() {

    }

}
