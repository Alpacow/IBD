/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

import java.io.RandomAccessFile; 
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import static ibd2.Table.BD_LEN;
import static ibd2.Table.BLOCKS_AMOUNT;
import static ibd2.Table.HEADER_LEN;
import static ibd2.Table.INDEX_LEN;
import java.nio.charset.StandardCharsets;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author pccli
 */
public class TableIO {

    RandomAccessFile file;

    int offset = 0;

    public TableIO(String folder, String name) throws Exception{

        Path path = Paths.get(folder);
        Files.createDirectories(path);

        file = new RandomAccessFile(folder+"\\"+name, "rw");
    }


    public void createTable() throws Exception {

        file.setLength(BD_LEN);

        //index
        file.seek(0);
        file.writeLong(0);

        //freeBlocks
        file.seek(INDEX_LEN);
        file.writeLong(BLOCKS_AMOUNT);
        for (long i = 0; i < BLOCKS_AMOUNT; i++) {
            file.writeLong(i);

        }

        //save blocks
        for (long i = 0; i < BLOCKS_AMOUNT; i++) {

            //save block header
            Long block_id = i;
            Long blockOffset = HEADER_LEN + Block.BLOCK_LEN * block_id;
            file.seek(blockOffset);
            file.writeBoolean(false);
        }

    }


    public void loadIndex(Index index) throws Exception{
    index.clear();
        file.seek(0);

        //load index
        Long len = file.readLong();
        for (int i = 0; i < len; i++) {
            Long blockId = file.readLong();
            Long recordId = file.readLong();
            Long primaryKey = file.readLong();
            index.addEntry(blockId, recordId, primaryKey);
        }
    }

    public void loadFreeBlocks(FreeBlocks organizer) throws Exception{
    //load freeBlocks
        file.seek(INDEX_LEN);
        organizer.freeBlocks.clear();
        Long len = file.readLong();
        for (int i = 0; i < len; i++) {
            Long freeBlock = file.readLong();
            organizer.freeBlocks.add(freeBlock);
        }
    }



    public void loadBlock(Block block, Long block_id) throws Exception{

        Params.BLOCKS_LOADED++;
    //start read
        Long blockOffset = HEADER_LEN + Block.BLOCK_LEN * block_id;
        file.seek(blockOffset);

        byte[] bytes = new byte[Block.BLOCK_LEN.intValue()];
        file.read(bytes);

        offset = 0;
        //check if block has at least one record
        Boolean used = readBoolean(bytes);
        if (!used) {
            for (long i = 0; i < Block.RECORDS_AMOUNT; i++) {
                block.freeRecords.add(i);
            }
            return;
        }

        //load block headder
        Long len = readLong(bytes);
        ArrayList<Long> freeRecords = new ArrayList<>();
        for (int i = 0; i < len; i++) {
            Long freeRecord = readLong(bytes);
            freeRecords.add(freeRecord);
        }

        //load records
        for (long rec_id = 0; rec_id < Block.RECORDS_AMOUNT; rec_id++) {
            if (freeRecords.contains(rec_id)) {
                continue;
            }
            loadRecord(block, rec_id, bytes);

        }

    }



    private void loadRecord(Block block, long rec_id, byte[] bytes) throws Exception {

        offset = (int)(Block.HEADER_LEN + Record.RECORD_SIZE * rec_id);

        Long primaryKey = readLong(bytes);
        String content = readUTF8(bytes);
        Record rec = new LoadedRecord(primaryKey);
        rec.setContent(content);

        block.addRecord(rec, rec_id); 

        //block.records.put(rec.getRec_id(), rec);
        //index.put(rec.getContent_id(), rec);
    }


    public void flushIndex(Hashtable<Long, IndexRecord> index) throws Exception{
    file.seek(0);
        file.writeLong(index.size());

        Iterator<IndexRecord> i = index.values().iterator();
        while (i.hasNext()) {
            IndexRecord record = i.next();
            file.writeLong(record.getBlockId());
            file.writeLong(record.getRecordId());
            file.writeLong(record.getPrimaryKey());
        }
    }

    public void flushFreeBlocks(Long[] ids)throws Exception{

    file.seek(INDEX_LEN);
        file.writeLong(ids.length);
        for (int i = 0; i < ids.length; i++) {
            file.writeLong(ids[i]);
        }

    }

    public void flushBlocks(Block[] blocks) throws Exception{
        for (int i = 0; i < blocks.length; i++) {
            flushBlock(blocks[i]);
        }
    }

    public void flushBlock(Block block) throws Exception {

        if (!block.hasChanged()) {
            return;
        }

        Params.BLOCKS_SAVED++;

        //save block header
        Long blockOffset = HEADER_LEN + Block.BLOCK_LEN * block.block_id;
        file.seek(blockOffset);
        file.writeBoolean(true);
        file.writeLong(block.freeRecords.size());
        for (Long freeBlock : block.freeRecords) {
            file.writeLong(freeBlock);
        }

        //save record
        //for (int x = 0; x < block.getRecordsCount(); x++) {
            //Record rec_ = block.getRecord(x);
            //if (rec_==null) continue;
        Iterator<Record> it =  block.iterator();
        while (it.hasNext()){
            Record rec_ = it.next();
            file.seek(blockOffset + Block.HEADER_LEN + rec_.getRecordId() * Record.RECORD_SIZE);
            file.writeLong(rec_.getPrimaryKey());
            file.writeUTF(rec_.getContent());
        }

    }



    public long readLong(byte[] b){
    long l = ((long) b[offset++] << 56)
       | ((long) b[offset++] & 0xff) << 48
       | ((long) b[offset++] & 0xff) << 40
       | ((long) b[offset++] & 0xff) << 32
       | ((long) b[offset++] & 0xff) << 24
       | ((long) b[offset++] & 0xff) << 16
       | ((long) b[offset++] & 0xff) << 8
       | ((long) b[offset++] & 0xff);
    return l;
    }

    public short readShort(byte[] b){
    long l = ((long) b[offset++] << 8)
       | ((long) b[offset++] & 0xff);
    return (short)l;
    }



    public String readUTF8(byte[] b){
    short len = readShort(b);
    return new String(b, offset, len, StandardCharsets.UTF_8);
    }


    public boolean readBoolean(byte[] b){

        return ((b[offset++] & 0xff) == 1);



    }



    //private long getBoolean(byte[] b, int start){
    //boolean bo = ((boolean) b[start] & 0xff);
    //return bo;
    //}

    public static void main(String[] args) {
        try {

            TableIO t = new TableIO("c:\\teste\\ibd\\", "xxx");

            /*
            RandomAccessFile io = new RandomAccessFile("c:\\teste\\ibd\\teste", "rw");
            io.writeLong(100);
            io.writeLong(200);
            io.writeLong(300);
            io.writeBoolean(true);
            io.writeBoolean(false);
            io.writeBoolean(true);
            io.writeBoolean(false);
            io.writeUTF("amigo do rei");

            io.close();
            System.out.println("file wrote");
                    */

            RandomAccessFile io = new RandomAccessFile("c:\\teste\\ibd\\teste", "rw");
            byte[] bytes = new byte[(8*3) + 4 + 15];
            io.read(bytes);

            System.out.println(t.readLong(bytes));
            System.out.println(t.readLong(bytes));
            System.out.println(t.readLong(bytes));

            System.out.println(t.readBoolean(bytes));
            System.out.println(t.readBoolean(bytes));
            System.out.println(t.readBoolean(bytes));
            System.out.println(t.readBoolean(bytes));

            System.out.println(t.readUTF8(bytes));

            //System.out.println(io.readLong());
            //System.out.println(io.readLong());
            //System.out.println(io.readLong());


        } catch (Exception ex) {
            Logger.getLogger(TableIO.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

}
