/*
    Otimizador de plano de execução
 */
package ibd2.query;


import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author flan
 */
public class FrancielleQueryOptimizer {
    
    public Operation query;
    private final List<TableScan> listOp;
    
    public FrancielleQueryOptimizer() {
        this.query = null;
        this.listOp = new ArrayList<>();
    }
    
    public Operation optimizeQuery(Operation op) throws Exception{
        try {
            op.open();
            if (op instanceof MergeJoin || op instanceof NestedLoopJoin) { // se é junção binária
                optimizeQueryRecursive(op);
            }
            else if (op instanceof TableScan) {
                listOp.add((TableScan) op);
            }
            else {
                throw new Exception("Operação unária inválida.");
            }
            createQuery();
            return query;
        } catch (Exception ex) {
            Logger.getLogger(FrancielleQueryOptimizer.class.getName()).log(Level.WARNING, ex.getMessage(), ex);
        }
        return null;
    }
    
    public Operation optimizeQueryRecursive (Operation op) throws Exception{
        try {
            op.open();
            if (op.hasNext()) {
                if (op instanceof MergeJoin) { // se é junção binária
                    MergeJoin join = (MergeJoin)op;
                    optimizeQueryRecursive(join.getLeftOperation()); // faz a recursão acessando no esquerdo
                    optimizeQueryRecursive(join.getRigthOperation()); // acessa no direito
                }
                else if (op instanceof NestedLoopJoin) {
                    NestedLoopJoin join = (NestedLoopJoin)op;
                    optimizeQueryRecursive(join.getLeftOperation()); // faz a recursão acessando no esquerdo
                    optimizeQueryRecursive(join.getRigthOperation()); // acessa no direito
                }
                else if (op instanceof TableScan) {
                    listOp.add((TableScan) op);
                }
                else {
                    throw new Exception("Operação unária inválida.");
                }
            }
            return query;
        } catch (Exception ex) {
            Logger.getLogger(FrancielleQueryOptimizer.class.getName()).log(Level.WARNING, ex.getMessage(), ex);
        }
        return null;
    }
    
    public void createQuery() throws Exception {
        quickSort(listOp, 0, listOp.size() - 1); // ordena lista
        if (query == null && listOp.size() >= 2) { // query vazia
            Operation tb1 = listOp.get(0);
            Operation tb2 = listOp.get(1);
            Operation join = new NestedLoopJoin(tb1, tb2);
            query = join;
            
            for(int i = 2; i < listOp.size(); i++) {
                Operation tb = listOp.get(i);
                join = new NestedLoopJoin(query, tb);
                query = join;
            }
        }
    }
    
    private void quickSort (List<TableScan> list, int init, int end) {
        if (init < end) {
               int pivot = split(list, init, end);
               quickSort(list, init, pivot - 1);
               quickSort(list, pivot + 1, end);
        }
    }
  
    private static int split (List<TableScan> list, int init, int end) {
        TableScan pivot = list.get(init);
        int i = init + 1, f = end;
        while (i <= f) {
            if (list.get(i).table.getRecordsAmount() <= pivot.table.getRecordsAmount())
                i++;
            else if (pivot.table.getRecordsAmount() < list.get(f).table.getRecordsAmount())
                f--;
            else {
                TableScan troca = list.get(i);
                list.set(i,list.get(f));
                list.set(f, troca);
                i++;
                f--;
            }
        }
        list.set(init, list.get(f));
        list.set(f, pivot);
        return f;
    }
}