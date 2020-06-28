/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.transaction;

import ibd2.Record;
import static ibd2.transaction.Instruction.WRITE;
import java.util.ArrayList;


/**
 *
 * @author pccli
 */
public class ConcurrencyManager {

    LockTable lockTable = new LockTable();
    
    static int cont = 0;
    
    ArrayList<Transaction> activeTransactions = new ArrayList<>();
    
    

    
    public ConcurrencyManager() throws Exception{
    }
    
    
    public Record processNextInstruction(Transaction t) throws Exception {
        
        if (!activeTransactions.contains(t)){
            activeTransactions.add(t);
        }
        
        if (!t.waitingLockRelease()) {
            Transaction toAbort = queueTransaction(t.getCurrentInstruction());
            if (toAbort!=null)
            {
                
                abort(toAbort); 
                return null;
            }
        }

        if (t.canLockCurrentInstruction()) {
            return t.processCurrentInstruction();
        }
        
        return null;

    }
    

    public void commit(Transaction t) throws Exception{
        t.commit();
        lockTable.removeTransaction(t);
        activeTransactions.remove(t);
        
        
    }
    
    public void abort(Transaction t) throws Exception{
        //System.out.println("Aborting "+t);
        System.out.println(SimulatedIterations.getTab(t.getId()-1)+t.getId()+" Abort");
        t.abort();
        lockTable.removeTransaction(t);
        activeTransactions.remove(t);
        
    }
    
    
    public Transaction queueTransaction(Instruction instruction) {
        Item item = lockTable.getItem(instruction);

        if (!alreadyInQueue(item, instruction)) {
            return addToQueue(item, instruction);
        }
        else return null;
        
        
    }
    
    public Transaction addToQueue(Item item, Instruction instruction) {
        Transaction t = instruction.getTransaction();
        Lock l = new Lock(t, instruction.getMode());
        item.locks.add(l);
        instruction.setItem(item);
        return null;
    }
    
    
    private boolean alreadyInQueue(Item item,  Instruction instruction){
        Transaction t = instruction.getTransaction();
        int mode = instruction.getMode();
        for (int i = 0; i < item.locks.size(); i++) {
            Lock l = item.locks.get(i);
            if (l.transaction.equals(t)) {
                if (mode == l.mode || l.mode==WRITE) {
                return true;
            }
        }
        }
        return false;
    } 
    
    
    
    
}
