package ibd2;

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
            List<Record> ordered = orderedBlock(b); // pega todos os registros do bloco e ordena
            b.removeAllRecords(); // remove todos os registros do bloco atual
            for(int i = 0; i < b.RECORDS_AMOUNT / 2; i++) { // insere metade inferior
                System.out.println("LISTA ORDENADA: " + ordered.get(i));
                addRecord(b, ordered.get(i)); // // insere no bloco os registros RECORDS_AMOUNT / 2;
            }
            
            
            // novo registro sera inserido no bloco atual (pois há espaço)
            // demais registros vao no bloco seguinte
            // 
            Record r = findLargest(b);

            removeRecord(r);

            recursiveSlide(b, r);
            return b.block_id;
        }
        return b.block_id;
    }
    
    private void recursiveSlide (Block b, Record rec) throws Exception {
        Long next = b.block_id + 1;
        Block b2 = getBlock(next);

        if (b2.isFull()) { // MODIFICAR AQUI TBM
            Record r2 = findLargest(b2);
            removeRecord(r2);

            addRecord(b2, rec);

            recursiveSlide(b2, r2);
        } else {
            addRecord(b2, rec);
        }
    }
    
    private List<Record> orderedBlock (Block b) {
        Iterator<Record> it = b.iterator();
        List<Record> list = (List<Record>) it;
        Collections.sort(list, Comparator.comparing((record) -> record.getPrimaryKey()));
        return list;
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
