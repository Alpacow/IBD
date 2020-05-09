/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

import java.util.Hashtable;
import java.util.Iterator;
import java.util.TreeSet;

/**
 *
 * @author pccli
 */
public class BufferManager {
    
    public static final int BUFFER_SIZE = 1;
    
    protected Hashtable<Long, Block> blocksBuffer = new Hashtable();
    
    protected TreeSet<BlockTime> blocksLRU = new TreeSet();
    
    
    
    class BlockTime implements Comparable<BlockTime>{
    
    public Block block;
    public Long time;

    public BlockTime(Block b){
        block = b;
        time = System.currentTimeMillis();
    }
    
    @Override
    public int compareTo(BlockTime o) {
        return Long.compare(this.time,o.time);
    }
}
    
    
    
    public void clear(){
        blocksBuffer.clear();
        //freeBlocks.clear();
        blocksLRU.clear();
    
    }
    
    
    
    
    public Block[] getBufferedBlocks() {
    Block[] blocks = new Block[blocksBuffer.size()];
    Iterator<Block> it = blocksBuffer.values().iterator();
    int x=0;
        while (it.hasNext()) {
            Block bl = it.next();
            blocks[x] = bl;
            x++;
        }
    return blocks;
    }
    
    
    /*
    public Block getFreeBlock_(DatabaseIO databaseIO) throws Exception{
        Long free_block_id = freeBlocks.first();
        Block block = blocksBuffer.get(free_block_id);
        
        while (block!=null && block.isFull()){
           freeBlocks.remove(block.block_id);
           free_block_id = freeBlocks.first();
           block = blocksBuffer.get(free_block_id);
        
        }
        
        if (block == null) {
            block = loadBlock(free_block_id, databaseIO);
        }
        return block;
    }*/
    
    public Block getBlock(Long block_id, TableIO databaseIO) throws Exception {
    //check if record is in buffer
        Block block = blocksBuffer.get(block_id);
        if (block!=null) {
            return block;
        }

        //needs to load block and all its records
        //System.out.println("loading block "+block_id);
        return loadBlock(block_id, databaseIO);
    
    }
    
    //load block and all its records
    private Block loadBlock(Long block_id, TableIO databaseIO) throws Exception {


        Block block = new Block(block_id);

        //if buffer is full, needs to flush a block
        if (blocksBuffer.size() == BUFFER_SIZE) {
            BlockTime bt = blocksLRU.pollFirst();
            removeBlock(bt.block, databaseIO);
        }

        //System.out.println("needed to load block");
        
        //adds block to buffer
        blocksBuffer.put(block_id, block);

        //add timestamp to block
        blocksLRU.add(new BlockTime(block));

        //load from disk
        databaseIO.loadBlock(block, block_id);

        return block;
    }
    
    

    private void removeBlock(Block block, TableIO databaseIO) throws Exception {

        //save block
        databaseIO.flushBlock(block);

        blocksBuffer.remove(block.block_id);
    }
    
}
