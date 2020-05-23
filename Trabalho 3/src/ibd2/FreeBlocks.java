/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;

import java.util.TreeSet;

/**
 *
 * @author pccli
 */
public class FreeBlocks {

    TreeSet<Long> freeBlocks = new TreeSet<Long>();

    public void clear(){
        freeBlocks.clear();
    }

    public void addFreeBlock(Long id){
                freeBlocks.add(id);
            }

    public void removeFreeBlock(Long id){
            freeBlocks.remove(id);
            }

    public int getFreeBlocksCount(){
        return freeBlocks.size();
            }

    public Long[] getFreeBlocksIds(){
     Long[] ids = new Long[freeBlocks.size()];

     int x = 0;
        for (Long freeBlock : freeBlocks) {
            ids[x] = freeBlock;
            x++;
        }
        return ids;
    }


    public Long getFirstFreeBlock(){
    return freeBlocks.first();
    }

}
