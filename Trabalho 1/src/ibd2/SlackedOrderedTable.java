package ibd2;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;

public class SlackedOrderedTable extends Table {
    
    public SlackedOrderedTable() {
        super();
    }
    
    public SlackedOrderedTable (String folder, String name) throws Exception {
        super(folder, name);
    }

    @Override
    protected Long selectBlock(long primaryKey) throws Exception {
        Block b = getBlockByIndexRecord(primaryKey);
        if (b.isFull()) {
            List<Record> list = null;
            list = orderedRecords(b, list); // pega todos os registros do bloco e ordena
            b.removeAllRecords(); // remove todos os registros do bloco atual
            showList(list);
            for (int i = 0; i < Block.RECORDS_AMOUNT / 2; i++) { // percorre metade superior
                Record rec = list.get(i);
                System.out.println("ADD NO BLOCO " + rec.getBlockId() + ": " + rec.getPrimaryKey());
                addRecord(b, rec); // // insere no bloco os registros RECORDS_AMOUNT / 2;
            }
            clearUpper(list);
            // metade inferior (resto da lista) é inserida no bloco seguinte
            recursiveSlide(b, list);
            showList(list);
            b = getBlockByIndexRecord(primaryKey);
            return b.block_id;
        }
        System.out.println("Insere " + primaryKey + " no bloco: " + b.block_id);
        return b.block_id;
    }
    
    private void showList (List<Record> list) {
        list.stream().forEach((rec) -> {
            System.out.println(rec.getPrimaryKey());
        });
    }
    
    /*
        Remove os elementos da metade superior da lista
    */
    private void clearUpper(List<Record> list) {
        for (int i = 0; i < Block.RECORDS_AMOUNT / 2; i++) {
            list.remove(0);
        }
    }
    
    private void recursiveSlide (Block b, List<Record> rec) throws Exception {
        Long next = b.block_id + 1;
        Block bn = getBlock(next);

        if (bn.isFull() || bn.freeRecords.size() < rec.size()) {
            List<Record> list = orderedRecords(bn, rec); // pega todos os registros do bloco e ordena
            bn.removeAllRecords(); // remove todos os registros do bloco atual
            showList(list);
            for (int i = 0; i < Block.RECORDS_AMOUNT / 2; i++) { // percorre metade superior
                Record r = list.get(i);
                addRecord(bn, r);
            }
            clearUpper(list);
            showList(list);
            recursiveSlide(bn, list);
        } else {
            if (rec.size() < bn.freeRecords.size()) { // se tem espaço p/ todos os registros no bloco
                System.out.println("RECURSIVE");
                showList(rec);
                for (Record r : rec) { // insere metade inferior no bloco q tem espaços
                    addRecord(bn, r);
                    System.out.println("ADD NO BLOCO " + r.getBlockId() + ": " + r.getPrimaryKey());
                }
                rec.clear(); // limpa a lista, todos os elementos pendentes foram inseridos
            }
        }
    }
    
    private Block getBlockByIndexRecord (long pk) throws Exception {
        IndexRecord ir = getLargestSmallerKey(pk);
        return (ir != null) ? getBlock(ir.getBlockId()) : getBlock(0L);
    }
    
    private List<Record> orderedRecords (Block b, List<Record> list) {
        Iterator<Record> it = b.iterator();
        if (list == null) {
            list = new ArrayList();
        }
        while (it.hasNext()) { // coloca conteúdo do iterator em uma lista auxiliar
            list.add(it.next());
        }
        Collections.sort(list, Comparator.comparing(Record::getPrimaryKey)); // ordena a lista
        return list; // converte a lista em um iterator
    }
    
    /*
        Retorna a maior chave primária que seja menor do que a chave que se quer inserir
    */
    private IndexRecord getLargestSmallerKey(long primaryKey) {
        IndexRecord ir = null;
        for (long i = primaryKey; i >= 0; i--) {
            ir = index.getEntry(i);
            if (ir != null) {
                break;
            }
        }
        return ir;
    }
}
