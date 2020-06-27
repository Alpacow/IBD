/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

import ibd2.Block;
import java.util.ArrayList;

/**
 *
 * @author SergioI
 */
public class BlockNestedLoopJoin implements BinaryOperation {

    Operation op1;
    Operation op2;

    Tuple curTuple2;
    Tuple nextTuple;
    
    int currentTuple1;
    
    boolean emptyTable = false;
    
    ArrayList<Tuple> joinBuffer = new ArrayList();

    public BlockNestedLoopJoin(Operation op1, Operation op2) throws Exception {
        super();
        this.op1 = op1;
        this.op2 = op2;
    }

    @Override
    public void open() throws Exception {

        op1.open();
        op2.open();
        curTuple2 = null;
        nextTuple = null;
        
        joinBuffer.clear();
        int count = 0;
        if (!op1.hasNext())
            emptyTable = true;

        while (op1.hasNext() && count < Block.RECORDS_AMOUNT){
            joinBuffer.add(op1.next());
            count++;
        }
        currentTuple1 = 0;

    }

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        while (true) {
            if (curTuple2 == null) {
                if(!op2.hasNext()){
                    op2.close();
                    
                    op2.open();
                    if(!op2.hasNext()){
                        return null;
                    }
                    else 
                        curTuple2 =op2.next();
                    
                    joinBuffer.clear();
                    
                    if (!op1.hasNext())
                        throw new Exception("No record after this point");
                    
                    int count = 0;
                    while (op1.hasNext() && count < Block.RECORDS_AMOUNT){
                        joinBuffer.add(op1.next());
                        count++;
                    }
                    currentTuple1 = 0;
                    
                    
                }
                else {
                    curTuple2 =op2.next();
                }
            }
            for (int i = currentTuple1; i < joinBuffer.size(); i++) {
 
                Tuple rec = joinBuffer.get(i);
                //if (cur2.primaryKey>cur1.primaryKey) break;
                if (rec.primaryKey == curTuple2.primaryKey) {
                    nextTuple = new Tuple();
                    nextTuple.primaryKey = rec.primaryKey;
                    nextTuple.content = rec.content + " " + curTuple2.content;
                    currentTuple1++;
                    return rec;
                }

            }
            currentTuple1 = 0;
            curTuple2 = null;
        }

    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }

        while (true) {
            if (curTuple2 == null) {
                if(!op2.hasNext()){
                    op2.close();
                    
                    op2.open();
                    if(!op2.hasNext()){
                        return false;
                    }
                    else 
                        curTuple2 =op2.next();
                    
                    joinBuffer.clear();
                    
                    if (!op1.hasNext())
                        return false;
                    
                    int count = 0;
                    while (op1.hasNext() && count < Block.RECORDS_AMOUNT){
                        joinBuffer.add(op1.next());
                        count++;
                    }
                    currentTuple1 = 0;
                    
                    
                }
                else {
                    curTuple2 =op2.next();
                }
            }
            for (int i = currentTuple1; i < joinBuffer.size(); i++) {
 
                Tuple curTuple1 = joinBuffer.get(i);
                //if (cur2.primaryKey>cur1.primaryKey) break;
                if (curTuple1.primaryKey == curTuple2.primaryKey) {
                    nextTuple = new Tuple();
                    nextTuple.primaryKey = curTuple1.primaryKey;
                    nextTuple.content = curTuple1.content + " " + curTuple2.content;
                    currentTuple1 = i + 1;
                    return true;
                }

            }
            currentTuple1 = 0;
            curTuple2 = null;
        }

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
