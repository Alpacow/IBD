/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2.transaction;

import ibd2.Record;
import java.util.ArrayList;

/**
 *
 * @author pccli
 */
public class Transaction {
    
    private ArrayList<Instruction> instructions = new ArrayList<>();
    private boolean waitingLockRelease = false;
    private int currrentInstructionIndex = 0;
    private boolean commited = false;
    private int id;
    
        
    
    
    public int getId(){
    return id;
    }
    
    public void setId(int id){
    this.id = id;
    }
    
    public int getInstructionsSize(){
        return instructions.size();
    }
    
    public Instruction getInstruction(int index){
        return instructions.get(index);
    }
    public Instruction getCurrentInstruction(){
        return instructions.get(currrentInstructionIndex);
    }
    
    public int getCurrentInstructionIndex(){
        return currrentInstructionIndex;
    }
    
        
    public void addInstruction(Instruction i){
        instructions.add(i);
        i.setTransaction(this);
    }
    
    
    
    
    public boolean hasNext(){
        return currrentInstructionIndex<instructions.size();
    }

    public boolean waitingLockRelease(){
        return waitingLockRelease;
    }
    
    public boolean canLockCurrentInstruction(){
        Instruction i = instructions.get(currrentInstructionIndex);
        
        boolean canGetLock = i.getItem().canBeLockedBy(this);
        if (!canGetLock)
            waitingLockRelease = true;
        return canGetLock;
        
    }
    
    
    public void abort(){
        currrentInstructionIndex = 0;
        waitingLockRelease = false;
    }
    
    public void commit(){
        commited = true;
    }
    
    public boolean isCOmmited(){
    return commited;
    }
    
    public Record processCurrentInstruction() throws Exception{
        Record r = instructions.get(currrentInstructionIndex).process();
        currrentInstructionIndex++;
        waitingLockRelease = false;
        return r;
    }
    
    
    public String toString(){
        return "T:" + id;
    }

    
}
