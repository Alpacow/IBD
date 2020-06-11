/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

/**
 *
 * @author SergioI
 */
public class NestedLoopJoin implements BinaryOperation {

    Operation op1;
    Operation op2;

    Tuple curTuple1;
    Tuple nextTuple;

    public NestedLoopJoin(Operation op1, Operation op2) throws Exception {
        super();
        this.op1 = op1;
        this.op2 = op2;
    }

    @Override
    public void open() throws Exception {

        op1.open();
        op2.open();
        curTuple1 = null;
        nextTuple = null;

    }

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        while (op1.hasNext() || curTuple1 != null) {
            if (curTuple1 == null) {
                curTuple1 = op1.next();
                op2.open();
            }

            while (op2.hasNext()) {
                Tuple curTuple2 = op2.next();
                //MockRecord rec2 = op2.next();
                if (curTuple1.primaryKey == curTuple2.primaryKey) {
                    Tuple rec = new Tuple();
                    rec.primaryKey = curTuple1.primaryKey;
                    rec.content = curTuple1.content + " " + curTuple2.content;
                    return rec;
                }

            }
            curTuple1 = null;
        }

        throw new Exception("No record after this point");

    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }

        while (curTuple1 != null || op1.hasNext()) {
            if (curTuple1 == null) {
                curTuple1 =op1.next();
                op2.open();
            }
            while (op2.hasNext()) {
                Tuple curTuple2 = (Tuple)op2.next();
                //if (cur2.primaryKey>cur1.primaryKey) break;
                if (curTuple1.primaryKey == curTuple2.primaryKey) {
                    nextTuple = new Tuple();
                    nextTuple.primaryKey = curTuple1.primaryKey;
                    nextTuple.content = curTuple1.content + " " + curTuple2.content;
                    return true;
                }

            }
            curTuple1 = null;
        }

        return false;
    }

    @Override
    public void close() {

    }
    
    @Override
    public Operation getLeftOperation() {
        return op1;
    }

    @Override
    public Operation getRigthOperation() {
        return op2;
    }

}
