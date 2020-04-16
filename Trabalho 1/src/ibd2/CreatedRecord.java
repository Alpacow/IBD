/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package ibd2;


/**
 *
 * @author pccli
 */
public class CreatedRecord extends Record{

    public CreatedRecord(Long cid) {
        super(cid);
        changed = true; 
    }
    
    
}
