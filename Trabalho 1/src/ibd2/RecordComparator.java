/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2;

import java.util.Comparator;

/**
 *
 * @author Sergio
 */
public class RecordComparator implements Comparator<Record>{

    @Override
    public int compare(Record o1, Record o2) {
        return o1.getPrimaryKey().compareTo(o2.getPrimaryKey());
    }
    
}
