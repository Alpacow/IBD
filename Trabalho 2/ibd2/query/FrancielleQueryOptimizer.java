/*
    Otimizador de plano de execução
 */
package ibd2.query;


import ibd2.Record;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author flan
 */
public class FrancielleQueryOptimizer {
    
    public Operation query;
    private List<Operation> listOp;
    
    public FrancielleQueryOptimizer() {
        this.query = null;
        this.listOp = new ArrayList<>();
    }
    
    public Operation optimizeQuery(Operation op) throws Exception{
        try {
            op.open();
            if (op.hasNext()) {
                if (op instanceof MergeJoin || op instanceof NestedLoopJoin) { // se é junção binária
                    MergeJoin join = (MergeJoin)op;
                    optimizeQuery(join.getLeftOperation()); // faz a recursão acessando no esquerdo
                    optimizeQuery(join.getRigthOperation()); // acessa no direito
                }
                else if (op instanceof TableScan) {
                    listOp.add(op);
                }
                else {
                    throw new Exception("Operação unária inválida.");
                }
                // ordenar lista e inserilas na query
            }
            return query;
        } catch (Exception ex) {
            Logger.getLogger(FrancielleQueryOptimizer.class.getName()).log(Level.WARNING, ex.getMessage(), ex);
        }
        return null;
    }
    
    /*
        Recebe a tabela a ser inserida e a raíz da arvore
    */
    public void insertInQuery(Operation tb) {
        //TableScan tbScan = (TableScan)op;
        //size = tbScan.table.getRecordsAmount();
    }
}