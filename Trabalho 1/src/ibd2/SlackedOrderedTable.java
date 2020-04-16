package ibd2;

import java.util.ArrayList;
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
        IndexRecord ir = getLargestSmallerKey(primaryKey);
        Block b = null;
        if (ir != null) {
            b = getBlock(ir.getBlockId());
        } else {
            b = getBlock(0L);
        }

        if (b.isFull()) {
            Iterator<Record> ordered = orderedBlock(b); // pega todos os registros do bloco e ordena
            b.removeAllRecords(); // remove todos os registros do bloco atual
            for (long i = 0; i < b.RECORDS_AMOUNT / 2; i++) { // insere metade inferior
                Record rec = ordered.next();
                System.out.println("ADD NO BLOCO " + rec.getBlockId() + ": " + rec.getPrimaryKey());
                addRecord(b, rec); // // insere no bloco os registros RECORDS_AMOUNT / 2;
            }
            
            // novo registro sera inserido no bloco atual (pois há espaços agora)
            // demais registros vao no bloco seguinte
            recursiveSlide(b, ordered);
            return b.block_id;
        }
        return b.block_id;
    }
    
    private void recursiveSlide (Block b, Iterator<Record> rec) throws Exception {
        Long next = b.block_id + 1;
        Block b2 = getBlock(next);

        if (b2.isFull()) { // MODIFICAR AQUI TBM
            Iterator<Record> ordered = orderedBlock(b2); // pega todos os registros do bloco e ordena
            b2.removeAllRecords(); // remove todos os registros do bloco atual
            for (long i = 0; i < b.RECORDS_AMOUNT / 2; i++) { // insere metade inferior
                Record r = ordered.next();
                addRecord(b2, r); // // insere no bloco os registros RECORDS_AMOUNT / 2;
            }
            recursiveSlide(b2, ordered);
        } else {
            // insiro metade inferior no bloco q tem espaços
            for (long i = b.RECORDS_AMOUNT / 2; i < b.RECORDS_AMOUNT; i++) {
                Record r = rec.next();
                addRecord(b2, r);
            }
        }
    }
    
    private Iterator<Record> orderedBlock (Block b) {
        Iterator<Record> it = b.iterator();
        List list = new ArrayList();
        while (it.hasNext()) { // coloca conteúdo do iterator em uma lista auxiliar
            list.add(it.next());
        }
        Collections.sort(list, Comparator.comparing(Record::getPrimaryKey)); // ordena a lista
        return list.iterator(); // converte a lista em um iterator
    }
    

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

    private Record findLargest(Block b) throws Exception {
        Long max = -1L;
        Record maxR = null;
        Iterator<Record> it = b.iterator();
        //for (int x = 0; x < b.getRecordsCount(); x++) {
        while (it.hasNext()){
            //Record rec = b.getRecord(x);
            Record rec = it.next();
            if (rec.getPrimaryKey() > max) {
                max = rec.getPrimaryKey();
                maxR = rec;
            }
        }
        return maxR;
    } 
}
