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
public class SimulatedIterations {

    private ArrayList<Transaction> transactions = new ArrayList<>();
    

    public void addTransaction(Transaction t) {
        transactions.add(t);
        t.setId(transactions.size());
    }

    public static char getChar(int value) {
        switch (value) {
            case 1:
                return 'A';
            case 2:
                return 'B';
            case 3:
                return 'C';
            case 4:
                return 'D';
            case 5:
                return 'E';
            case 6:
                return 'F';
            case 7:
                return 'G';
            case 8:
                return 'H';
            case 9:
                return 'I';
            case 10:
                return 'J';
            default:
                return '-';
        }
    }
    
    public static int getValue(char c) {
        switch (c) {
            case 'A':
                return 1;
            case 'B':
                return 2;
            case 'C':
                return 3;
            case 'D':
                return 4;
            case 'E':
                return 5;
            case 'F':
                return 6;
            case 'G':
                return 7;
            case 'H':
                return 8;
            case 'I':
                return 9;
            case 'J':
                return 10;
            default:
                return '-';
        }
    }

    public static String getTab(int index){
    StringBuffer sb = new StringBuffer();
        for (int i = 0; i < index*15; i++) {
            sb.append(' ');
        }
    return sb.toString();
    
    }
    
    public void run(int error) throws Exception {

        FrancielleConcurrencyManager manager = new FrancielleConcurrencyManager(true);
        int commited = 0;

        int step = 0;
        
        while (commited < transactions.size()) {
            
           
            
            for (int i = 0; i < transactions.size(); i++) {
                
            if (step==error)
                throw new Exception("kaputt");
                
                Transaction t = transactions.get(i);
                if (!t.hasNext()) {
                    if (!t.isCOmmited()) {
                        manager.commit(t);
                        System.out.println(getTab(t.getId()-1)+t.getId() + " commit");
                        commited++;
                        step++;
                    }
                } else {
                    Instruction inst = t.getCurrentInstruction();
                    Record r = manager.processNextInstruction(t);
                    if (r != null) {
                        System.out.print(getTab(i)+t.getId() + " ");
                        //System.out.print(inst);
                        System.out.print(inst.getModeType()+" ");
                        //System.out.print(" values ");
                        char value = getChar((int)r.getPrimaryKey().intValue());
                        System.out.println(value);
                        //System.out.println(r);
                        step++;
                    }
                }
            }
        }
        
        //manager.flushLog();

    }

}
