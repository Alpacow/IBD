/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

import java.util.Hashtable;

/**
 *
 * @author pccli
 */
public class Index {
    Hashtable<Long, IndexRecord> index = new Hashtable();
    
    public void clear(){
        index.clear();
        
    }
    
    
    public void addEntry(Long blockId, Long recordId, Long primaryKey){
        IndexRecord ir = new IndexRecord(blockId, recordId, primaryKey);
        index.put(primaryKey, ir);
    }
    
    public void removeEntry(Long primaryKey){
        index.remove(primaryKey);
    }
    
    public IndexRecord getEntry(Long primaryKey){
        return index.get(primaryKey);
    }
    
}
