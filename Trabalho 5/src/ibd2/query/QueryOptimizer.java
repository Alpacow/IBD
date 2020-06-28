/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

import ibd2.Table;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 *
 * @author luizf
 */
public class QueryOptimizer {
    public Operation optimizeQuery(Operation op) throws Exception{
        List<Table> tables = new ArrayList<>();
        findTables(op, tables);
        //Comparator c = (Comparator<Table>) (Table o1, Table o2) -> Long.compare(o1.getRecordsAmount(), o2.getRecordsAmount());
        //Collections.sort(tables, c);
        Collections.sort(tables, Comparator.comparingInt(o -> o.getRecordsAmount())); 
        Operation query = buildTree(tables);
        return query;
    }
    
    private Operation buildTree(List<Table> tables) throws Exception{
    
        Operation opAnt = new TableScan(tables.get(0));

        for (int i = 1; i < tables.size(); i++) {
            Operation op = new TableScan(tables.get(i));
            Operation join = new BlockNestedLoopJoin(opAnt, op);
            opAnt = join;
        }
        return opAnt;

    }

    
    private void findTables(Operation op, List<Table> tables) throws Exception{
    
        if (op instanceof TableScan){
            TableScan ts = (TableScan)op;
            tables.add(ts.table);
        }
        else if (op instanceof BinaryOperation){
            BinaryOperation join = (BinaryOperation)op;
            findTables(join.getLeftOperation(), tables);
            findTables(join.getRigthOperation(), tables);
        }
        else throw new Exception ("Invalid Operation");
    }
    
}
