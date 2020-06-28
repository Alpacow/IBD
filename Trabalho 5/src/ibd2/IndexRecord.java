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
public class IndexRecord {

    long recordId;
    long blockId;
    private long primaryKey;

    public IndexRecord(Long bid,Long rid, Long pk){
    recordId = rid;
    blockId = bid;
    primaryKey = pk;
    }


    public Long getRecordId() {
        return recordId;
    }

    public Long getBlockId() {
        return blockId;
    }

    /**
     * @return the primaryKey
     */
    public long getPrimaryKey() {
        return primaryKey;
    }




}
