/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2.transaction;

import ibd2.Record;
import ibd2.Table;

/**
 *
 * @author pccli
 */
public class Instruction {
    
    public static final int READ = 0;
    public static final int WRITE = 1;
    
    private int mode;
    private Table table;
    private long pk;
    private String content;
    
    private Item item;
    private Transaction transaction;
    
    public Instruction(Table table, int mode, long pk, String content) throws Exception{
        this.table = table;
        this.mode = mode;
        this.pk = pk;
        this.content = content;
    }
    
    public Instruction(Table table, int mode, char pk, String content) throws Exception{
        this(table, mode, SimulatedIterations.getValue(pk), content);
    }
    
    
    public Record process() throws Exception{
    if (mode==Instruction.READ){
        return getTable().getRecord(pk);
    }
    else{
        Record rec = getTable().getRecord(pk);
        return getTable().updateRecord(getPk(), content);
    }
        
        
    }
    
    public int getMode(){
    return mode;
    }
    
    public static String getModeType(int mode){
        switch(mode){
            case READ: return "read";
            case WRITE: return "write";
            default: return "error";
        }
        
        
    }
    
    public String getModeType(){
        return Instruction.getModeType(mode);
    }
    
    
    public String toString (){
        
        return getMode()+" "+getTable().key;
    }

    /**
     * @return the transaction
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * @param transaction the transaction to set
     */
    public void setTransaction(Transaction transaction) {
        this.transaction = transaction;
    }

    /**
     * @return the table
     */
    public Table getTable() {
        return table;
    }

    /**
     * @param table the table to set
     */
    public void setTable(Table table) {
        this.table = table;
    }

    /**
     * @return the pk
     */
    public long getPk() {
        return pk;
    }

    /**
     * @param pk the pk to set
     */
    public void setPk(long pk) {
        this.pk = pk;
    }

    /**
     * @return the item
     */
    public Item getItem() {
        return item;
    }

    /**
     * @param item the item to set
     */
    public void setItem(Item item) {
        this.item = item;
    }
    
    
    
    
}
