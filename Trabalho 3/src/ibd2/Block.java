/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

import java.util.Iterator;
import java.util.TreeSet;

/**
 *
 * @author pccli
 */
public class Block implements Iterable{

    public final static Long RECORDS_AMOUNT = 31L;

    public final static Long HEADER_LEN = Long.BYTES + RECORDS_AMOUNT * Long.BYTES;

    public final static Long FILLER = 24L;

    public final static Long BLOCK_LEN = HEADER_LEN + RECORDS_AMOUNT * Record.RECORD_SIZE + FILLER;




    public Long block_id;

    TreeSet<Long> freeRecords = new TreeSet<Long>();
    private Record records[] = new Record[(int)(long)RECORDS_AMOUNT];
    private int cursor = 0;

    @Override
    public Iterator iterator() {
        return new CustomIterator();
    }


    class CustomIterator implements Iterator{

        public CustomIterator() {
            cursor = 0;
        }



        @Override
        public boolean hasNext() {
            for (; cursor < records.length; cursor++) {
                if (records[cursor]!=null){
                    return true;
                }
            }
            return false;
        }

        @Override
        public Record next() {
            return records[cursor++];
        }

    }


    public Block(Long block_id){

        this.block_id = block_id;
        resetFreeRecords();
    }



    public boolean hasChanged()
    {
        for (int i = 0; i < records.length; i++) {
            if (records[i]!=null && records[i].changed)
                return true;
        }
        return false;
    }

    private void resetFreeRecords(){
        freeRecords.clear();
        for (int i = 0; i < RECORDS_AMOUNT; i++) {
            freeRecords.add(new Long(i));
        }
    }

    public void removeAllRecords() throws Exception{
        for (int i = 0; i < RECORDS_AMOUNT; i++) {
            Record rec = records[i];
            if (rec!=null){
                removeRecord(rec);
            }
        }
    }



    public boolean isFull()
    {
    return freeRecords.size()==0;
    }

    public boolean isEmpty()
    {
    return freeRecords.size()==RECORDS_AMOUNT;
    }

    public Record getRecord(int recId){
        return records[(int)(long)recId];
    }

    public int getRecordsCount(){
        return records.length;
    }

    public Record addRecord(Record rec, Long rec_id) throws Exception{

    if (rec_id==-1){
        rec_id = freeRecords.first();
        Params.RECORDS_ADDED++;
        //System.out.println("adding record to "+block_id+" now contains free records = "+freeRecords.size());
    }

    rec.setRec_id(rec_id);
    rec.setBlock(this);
    records[(int)(long)rec_id] = rec;

    freeRecords.remove(rec_id);



    return rec;

    }

    public void removeRecord(Record rec) throws Exception{

    if (records[(int)(long)rec.getRecordId()]!=rec){
        throw new Exception("record error");
    }

    records[(int)(long)rec.getRecordId()] = null;
    freeRecords.add(rec.getRecordId());

    Params.RECORDS_REMOVED++;
    //System.out.println("removing record from "+block_id+" now contains free records = "+freeRecords.size());

    }




    public Record maxPrimaryKey(){
        Record max = records[0];


        for(int i=0;i<records.length;i++)
        {
            if (records[i]==null) continue;

            if(records[i].getPrimaryKey() > max.getPrimaryKey())
            {
                max = records[i];
            }
        }

        return max;
    }



}
