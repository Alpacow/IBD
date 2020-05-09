/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2;

public abstract class Table {

    public final static Long BLOCKS_AMOUNT = 64L;

    //header
    public final static Long INDEX_LEN = Long.BYTES + 3 * BLOCKS_AMOUNT * Block.RECORDS_AMOUNT * Long.BYTES;
    public final static Long FREE_BLOCKS_LEN = Long.BYTES + BLOCKS_AMOUNT * Long.BYTES;
    public final static Long FILLER = 1008L;
    public final static Long HEADER_LEN = INDEX_LEN + FREE_BLOCKS_LEN + FILLER;

    public final static Long BD_LEN = HEADER_LEN + BLOCKS_AMOUNT * Block.BLOCK_LEN;

    protected FreeBlocks freeBlocks = new FreeBlocks();
    protected TableIO tableIO = null;
    protected BufferManager bufferManager = new BufferManager();
    Index index = new Index();



    public boolean loaded = false;

    public String key;

    public Table() {
    }


    public Table(String folder, String name) throws Exception {
        tableIO = new TableIO(folder, name);
    }

    public void initLoad() throws Exception {

        if (loaded) return;
        index.clear();
        tableIO.loadIndex(index);

        bufferManager.clear();
        freeBlocks.clear();
        tableIO.loadFreeBlocks(freeBlocks);

        loaded = true;

    }

    public Block getBlock(long block_id) throws Exception{

        if (!loaded) initLoad();

        return bufferManager.getBlock(block_id, tableIO);
    }

    public Record getRecord(Long primaryKey) throws Exception {

        if (!loaded) initLoad();

        IndexRecord index_rec = index.getEntry(primaryKey);
        if (index_rec == null) {
            return null;
        }

        Block block = getBlock(index_rec.getBlockId());

        //now the block contains the record
        return (Record) block.getRecord((int) (long) index_rec.getRecordId());
    }

    public boolean isFull() throws Exception{

        if (!loaded) initLoad();

        return (freeBlocks.getFreeBlocksCount() == 0);
    }

    public Record addRecord(long primaryKey, String content) throws Exception {


        if (!loaded) initLoad();

        if (index.getEntry(primaryKey) != null) {
            throw new Exception("ID already exists");
        }

        if (isFull()) {
            throw new Exception("No Space");
        }



        Record rec = new CreatedRecord(primaryKey);
        rec.setContent(content);


        Long free_block_id = selectBlock(primaryKey);


        Block block = bufferManager.getBlock(free_block_id, tableIO);


        addRecord(block, rec);

        return rec;

    }

    public Record updateRecord(long primaryKey, String content) throws Exception {

        if (!loaded) initLoad();

        IndexRecord index_rec = index.getEntry(primaryKey);
        if (index_rec == null) {
            return null;
        } 

        Block block = getBlock(index_rec.getBlockId());
        Record rec = (Record) block.getRecord((int) (long) index_rec.getRecordId());
        //now the block contains the updated record
        rec.setContent(content);
        return rec;

    }

    public Record removeRecord(long primaryKey) throws Exception {

        if (!loaded) initLoad();

        IndexRecord index_rec = index.getEntry(primaryKey);
        if (index_rec == null) {
            return null;
        }

        Block block = getBlock(index_rec.getBlockId());
        Record rec = (Record) block.getRecord((int) (long) index_rec.getRecordId());
        //now the block contains the updated record
        removeRecord(rec);
        return rec;

    }



    protected void addRecord(Block block, Record rec) throws Exception {

        if (!loaded) initLoad();

        block.addRecord(rec, -1L);

        if (block.isFull()) {
            freeBlocks.removeFreeBlock(block.block_id);
        }

        index.addEntry(block.block_id, rec.getRecordId(), rec.getPrimaryKey());
    }


    protected abstract Long selectBlock(long primaryKey) throws Exception;

    protected void removeRecord(Record record) throws Exception {

        if (!loaded) initLoad();


        Block block = record.getBlock();
        boolean wasFull = block.isFull();

        block.removeRecord(record);

        if (wasFull) {
            freeBlocks.addFreeBlock(block.block_id);
        }

        index.removeEntry(record.getPrimaryKey());

    }

    public void flushDB() throws Exception {

        if (!loaded) return;

        tableIO.flushIndex(index.index);

        tableIO.flushFreeBlocks(freeBlocks.getFreeBlocksIds());

        tableIO.flushBlocks(bufferManager.getBufferedBlocks());

    }

    public void createTable() throws Exception {
        tableIO.createTable();
    }

    public int getRecordsAmount(){
        return index.getRecordsAmount();
    }

}
