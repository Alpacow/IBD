/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2;

import java.util.ArrayList;
import java.util.Collections;
 
public class SlackedOrderedTable extends Table{


    public SlackedOrderedTable() {
    super();
    }


    public SlackedOrderedTable(String folder, String name) throws Exception {
        super(folder, name);
    }




    @Override
    protected Long selectBlock(long primaryKey) throws Exception {

        IndexRecord ir = getLargestSmallerKey(primaryKey);
        Block b;
        if (ir != null) {
            b = getBlock(ir.getBlockId());
        } else {
            b = getBlock(0L);
        }

        if (b.isFull()) {
            splitBlock(b, new ArrayList<>());
            if (b.maxPrimaryKey().getPrimaryKey()>primaryKey)
                return b.block_id;
            else return b.block_id+1;
        }
        return b.block_id;
    }

    private void splitBlock(Block b, ArrayList<Record> recordsToMove) throws Exception{
        long halfSize = Block.RECORDS_AMOUNT / 2;
        for (int i = 0; i < b.getRecordsCount(); i++) {
            Record rec = b.getRecord(i);
            if (rec==null) continue;
            recordsToMove.add(rec);
            removeRecord(rec);

        }

        Collections.sort(recordsToMove, new RecordComparator());

        if (recordsToMove.size()<Block.RECORDS_AMOUNT){
        for (int i = 0;i<recordsToMove.size();  i++) {
            Record rec = recordsToMove.get(i);
            addRecord(b, rec);
        }
        return;
        }
        Collections.reverse(recordsToMove);
        int recordsToMoveSize = recordsToMove.size();
        for (int i = recordsToMoveSize-1;i>=recordsToMoveSize-1-halfSize;  i--) {
            Record rec = recordsToMove.get(i);
            addRecord(b, rec);
            recordsToMove.remove(i);
        }
        if (recordsToMove.isEmpty()) return;
        //Block nextBlock = bufferManager.getBlock(b.block_id+1, tableIO);
        Block nextBlock = getBlock(b.block_id+1);
        splitBlock(nextBlock, recordsToMove);

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




}
