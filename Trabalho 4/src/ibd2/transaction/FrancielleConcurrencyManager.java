/*
    Extensão classe ConcurrencyManager com implementação de detecção de deadlock
 */
package ibd2.transaction;

import java.util.ArrayList;

/**
 *
 * @author flan
 */
public class FrancielleConcurrencyManager extends ConcurrencyManager {
    
    boolean isPreemptive;
    /*
        Wait-Die (não preemptiva)
        Wound-Wait (preemptiva)
    */
    public FrancielleConcurrencyManager (boolean preemptive) throws Exception{
        super();
        this.isPreemptive = preemptive;
    }
    
    /*
        devolve a transação a ser abortada com base na estratégia escolhida
    */
    @Override
    public Transaction addToQueue (Item item, Instruction instruction) {
        return (isPreemptive) ? woundWaitEstrategy(item, instruction) : waitDieEstrategy(item, instruction);
    }
    
    private Transaction woundWaitEstrategy (Item item, Instruction instruction) {
        Lock l = addToLockQueue(item, instruction);
        for (Lock lock : item.locks) {
            if (instruction.getTransaction().getId() < lock.transaction.getId()) {
                if (instruction.getMode() == Instruction.READ && lock.mode == Instruction.READ) {}
                else {
                    changePositions(l, lock, item.locks);
                    return lock.transaction;
                }
            }
        }
        return null;
    }
    
    private Transaction waitDieEstrategy (Item item, Instruction instruction) {
        addToLockQueue(item, instruction);
        for (Lock lock : item.locks) {
            if (instruction.getTransaction().getId() > lock.transaction.getId()) {
                if (instruction.getMode() == Instruction.READ && lock.mode == Instruction.READ) {}
                else
                    return instruction.getTransaction();
            }
        }
        return null;
    }
    
    private Lock addToLockQueue (Item item, Instruction instruction) {
        Lock l = new Lock(instruction.getTransaction(), instruction.getMode());
        item.locks.add(l);
        instruction.setItem(item);
        return l;
    }
    
    /* musa os locks l1 e l2 de lugar
    private void changePositions (Lock l1, Lock l2, ArrayList<Lock> list) {
        int idx1 = list.indexOf(l1);
        int idx2 = list.indexOf(l2);
        list.set(idx1, l2);
        list.set(idx2, l1);
    }
}
