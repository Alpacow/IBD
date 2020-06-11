/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

import ibd2.Table;
import ibd2.Block;
import ibd2.Record;

/**
 *
 * @author Sergio
 */
public class TableScan implements Operation {

    public long currentBlock;
    public int currentRecord;
    Table table;

    Tuple nextTuple = null;
    boolean reachedEnd = false;

    public TableScan(Table table) {
        this.table = table;
    }

    @Override
    public void open() throws Exception {

        table.initLoad();
        currentBlock = 0;
        currentRecord = -1;
        nextTuple = null;
        reachedEnd = false;
    }

    @Override
    public Tuple next() throws Exception {

        if (nextTuple != null) {
            Tuple next_ = nextTuple;
            nextTuple = null;
            return next_;
        }

        if (reachedEnd)
            throw new Exception("No records after this point");

        Record rec = null;
        while (rec == null) {

            currentRecord++;
            if (currentRecord >= Block.RECORDS_AMOUNT) {
                currentRecord = 0;
                {
                    currentBlock++;
                    if (currentBlock == Table.BLOCKS_AMOUNT) {
                        reachedEnd = true;
                        //System.out.println("not supposed to get here");
                        throw new Exception("No records after this point");
                    }
                }
            }

            Block block = table.getBlock(currentBlock);
            rec = block.getRecord(currentRecord);
        }
        if (rec == null) {
            reachedEnd = true;
            System.out.println("not supposed to get here");
            throw new Exception("No records after this point");
        }

        Tuple tuple = new Tuple();
        tuple.primaryKey = rec.getPrimaryKey();
        tuple.content = rec.getContent();

        return tuple;
    }

    @Override
    public boolean hasNext() throws Exception {

        if (nextTuple != null) {
            return true;
        }

        if (reachedEnd)
            return false;


        Record rec = null;
        while (rec == null) {

            currentRecord++;
            if (currentRecord >= Block.RECORDS_AMOUNT) {
                currentRecord = 0;
                {
                    currentBlock++;
                    if (currentBlock == Table.BLOCKS_AMOUNT) {
                        reachedEnd = true;
                        return false;
                    }
                }
            }

            Block block = table.getBlock(currentBlock);
            rec = block.getRecord(currentRecord);

        }
        if (rec != null) {
            nextTuple = new Tuple();
            nextTuple.primaryKey = rec.getPrimaryKey();
            nextTuple.content = rec.getContent();
            return true;
        }
        System.out.println("not suppoed to get here");
        return (rec != null);
    }

    @Override
    public void close() {

    }

}
