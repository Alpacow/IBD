/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2.transaction;

import static ibd2.transaction.Instruction.WRITE;

/**
 *
 * @author pccli
 */
public class Lock {
    Transaction transaction;
    int mode;
    
    public Lock(Transaction t, int mode){
        transaction =  t;
        this.mode = mode;
    }
    
    public void upgrade(){
        this.mode = WRITE;
    }
}
