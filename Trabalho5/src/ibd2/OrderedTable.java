/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2;

import java.util.Iterator;

public class OrderedTable extends Table{


    public OrderedTable() {
    super();
    }


    public OrderedTable(String folder, String name) throws Exception {
        super(folder, name);
    }


    @Override
    protected Long selectBlock(long primaryKey) throws Exception {

        IndexRecord ir = getLargestSmallerKey(primaryKey);
        Block b = null;
        if (ir != null) {
            b = getBlock(ir.getBlockId());
        } else {
            b = getBlock(0L);
        }

        if (b.isFull()) {

            Record r = findLargest(b);

            removeRecord(r);

            recursiveSlide(b, r);
            return b.block_id;
        }
        return b.block_id;
    }




        private IndexRecord getLargestSmallerKey(long primaryKey) {
        IndexRecord ir = null;
        for (long i = primaryKey; i >= 0; i--) {
            ir = index.getEntry(i);
            if (ir != null) {
                break;
            }
        }
        return ir;
    }


    private void recursiveSlide(Block b, Record rec) throws Exception {

        Long next = b.block_id + 1;
        Block b2 = getBlock(next);

        if (b2.isFull()) {
            Record r2 = findLargest(b2);
            removeRecord(r2);

            addRecord(b2, rec);

            recursiveSlide(b2, r2);

        } else {
            addRecord(b2, rec);

        }
    }

    private Record findLargest(Block b) throws Exception {

        Long max = -1L;
        Record maxR = null;
        Iterator<Record> it = b.iterator();
        //for (int x = 0; x < b.getRecordsCount(); x++) {
        while (it.hasNext()){
            //Record rec = b.getRecord(x);
            Record rec = it.next();
            if (rec.getPrimaryKey() > max) {
                max = rec.getPrimaryKey();
                maxR = rec;
            }
        }
        return maxR;
    }




}
