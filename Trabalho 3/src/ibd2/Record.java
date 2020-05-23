/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

/**
 *
 * @author pccli
 */ 
public abstract class Record extends AbstractRecord{
    
    
    public final static Integer RECORD_SIZE = 1048;
    
    
    private final Long primaryKey;
    private String content;
    private Block block;
    private long rec_id;
    
    protected boolean changed;
    
    public Record(Long pk){
        primaryKey = pk;
    }
    
    /**
     * @return the content
     */
    public String getContent() {
        return content;
    }

    /**
     * @param content the content to set
     */
    public void setContent(String content) {
        StringBuffer sb = new StringBuffer();
        changed = true;
        
        Integer size = RECORD_SIZE;
        if (content.length()<RECORD_SIZE  )
            size = content.length();
        
        for (int i = 0; i < size; i++) {
            sb.append(content.charAt(i));
        }
        this.content = sb.toString();
    }

    /**
     * @return the block
     */
    public Block getBlock() {
        return block;
    }

    /**
     * @param block the block to set
     */
    public void setBlock(Block block) {
        this.block = block;
    }


    /**
     * @param rec_id the rec_id to set
     */
    public void setRec_id(long rec_id) {
        this.rec_id = rec_id;
    }
    
    
    

    /**
     * @return the data_id
     */
    public Long getPrimaryKey() {
        return primaryKey;
    }


    @Override
    public Long getRecordId() {
        return rec_id;
    }

    @Override
    public Long getBlockId() {
        return block.block_id;
    }
    
    public String toString(){
        return primaryKey+":"+content;
    }
    
}
