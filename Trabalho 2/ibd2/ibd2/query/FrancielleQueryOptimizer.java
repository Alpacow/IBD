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
            if (op != null) {
                op.open();
                if (op instanceof MergeJoin || op instanceof NestedLoopJoin) { // se é alguma junção binária
                    optimizeQueryRecursive(op); // percorre a árvore
                }
                else if (op instanceof TableScan) { // se é tabela
                    listOp.add((TableScan) op); // add tabela na lista de operações TableScan
                }
                else {
                    throw new Exception("Operação unária inválida."); // só aceita tabelas do tipo TableScan
                }
                createQuery(); // cria a árvore otimizada
                return query;
            } else {
                throw new Exception("Sem operações para otimizar");
            }
        } catch (Exception ex) {
            Logger.getLogger(FrancielleQueryOptimizer.class.getName()).log(Level.WARNING, ex.getMessage(), ex);
        }
        return null;
    }
    
    public Operation optimizeQueryRecursive (Operation op) throws Exception{
        try {
            op.open();
            if (op.hasNext()) {
                if (op instanceof MergeJoin) { // se é junção binária do tipo MergeJoin
                    MergeJoin join = (MergeJoin)op; // altera o tipo
                    optimizeQueryRecursive(join.getLeftOperation()); // faz a recursão acessando no esquerdo
                    optimizeQueryRecursive(join.getRigthOperation()); // acessa no direito
                }
                else if (op instanceof NestedLoopJoin) { // se é junção binária do tipo NestedLoopJoin
                    NestedLoopJoin join = (NestedLoopJoin)op;// altera o tipo
                    optimizeQueryRecursive(join.getLeftOperation()); // faz a recursão acessando no esquerdo
                    optimizeQueryRecursive(join.getRigthOperation()); // acessa no direito
                }
                else if (op instanceof TableScan) { // se for operação unaria do tipo TableScan
                    listOp.add((TableScan) op); // adiciona operação na lista
                }
                else { // só aceita tabelas do tipo TableScan
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
        quickSort(listOp, 0, listOp.size() - 1); // ordena lista de acordo com o tamanho de cada tabela
        if (listOp.size() >= 2) { // se a lista tem mais de 2 tabelas
            Operation tb1 = listOp.get(0);
            Operation tb2 = listOp.get(1);
            Operation join = new NestedLoopJoin(tb1, tb2); // realiza a primeira junção
            query = join;
            
            for(int i = 2; i < listOp.size(); i++) { // percorre as demais tabelas (se houver)
                Operation tb = listOp.get(i);
                join = new NestedLoopJoin(query, tb); // realiza join do join anterior e a tabela
                query = join;
            }
        } else if (listOp.size() == 1) { // se na operação foi passada apenas uma tabela, retorna ela
            query = listOp.get(0);
        }
    }
    
    /* métodos auxiliares para ordenar a lista de TableScan */
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