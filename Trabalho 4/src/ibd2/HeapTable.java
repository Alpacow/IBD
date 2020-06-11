/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2;

public class HeapTable extends Table{


    public HeapTable() {
    super();
    }


    public HeapTable(String folder, String name) throws Exception {
        super(folder, name);
    }



    @Override
    protected Long selectBlock(long primaryKey) {

        return freeBlocks.getFirstFreeBlock();
    }



}
