/*
    Extensão classe ConcurrencyManager com implementação de detecção de deadlock
 */
package ibd2.transaction;

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
        addToLockQueue(item, instruction);
        for (Lock lock : item.locks) {
            if (instruction.getTransaction().getId() < lock.transaction.getId()) {
                return lock.transaction;
            }
        }
        return null;
    }
    
    private Transaction waitDieEstrategy (Item item, Instruction instruction) {
        addToLockQueue(item, instruction);
        for (Lock lock : item.locks) {
            if (instruction.getTransaction().getId() > lock.transaction.getId()) {
                return lock.transaction;
            }
        }
        return null;
    }
    
    private void addToLockQueue (Item item, Instruction instruction) {
        Lock l = new Lock(instruction.getTransaction(), instruction.getMode());
        item.locks.add(l);
        instruction.setItem(item);
    }
}
