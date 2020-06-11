/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.transaction;

import ibd2.Table;
import java.util.ArrayList;

/**
 *
 * @author pccli
 */
public class Item {

    public Table table;
    public long primaryKey;

    public LockTable lockTable;

    ArrayList<Lock> locks = new ArrayList<>();

    public Item(LockTable lockTable, Table table, long pk) {
        this.table = table;
        primaryKey = pk;
        this.lockTable = lockTable;
    }

    

    public void removeTransaction(Transaction t) {

        for (int i = locks.size() - 1; i >= 0; i--) {
            Lock lock = locks.get(i);
            if (lock.transaction.equals(t)) {
                locks.remove(i);
            }

        }

    }

    public void printLocks() {
        System.out.print("Item:" + SimulatedIterations.getChar((int) primaryKey) + "=>");
        for (int i = 0; i < locks.size(); i++) {
            Lock lock = locks.get(i);
            System.out.print(lock.transaction.getId() + ":" + Instruction.getModeType(lock.mode));

        }
        System.out.println("");

    }

    public boolean canBeLockedBy(Transaction t) {
        int currentMode = t.getCurrentInstruction().getMode();
        for (int i = 0; i < locks.size(); i++) {
            Lock l = locks.get(i);
            if (l.transaction.equals(t)) {
                return true;
            }
            if (l.mode == Instruction.WRITE) {
                return false;
            }
            if (currentMode == Instruction.WRITE) {
                return false;
            }
        }
        System.out.println("não deveria chegar aqui");
        return false;
    }

    

}
