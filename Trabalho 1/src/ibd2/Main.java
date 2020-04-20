package ibd2;

import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main {

    public void testCreation(Table table) throws Exception {
        table.createTable();
    }

    public void testLoad(Table table) throws Exception {
        table.initLoad();
    }

    public void testInsertion(Table table) throws Exception {
        table.initLoad();
        Record rec = table.addRecord(3, "lddldf fdçonfd faofhdofh odhfod ofsdf");
        table.flushDB();

    }

    public void testSearch(Table table) throws Exception {
        
        table.initLoad();
        Record rec = table.getRecord(3L);
        if (rec == null) {
            System.out.println("não tem");
        } else {
            System.out.println(rec.getContent());
        }
    }

    public void testUpdate(Table table) throws Exception {
        table.initLoad();
        Record rec = table.getRecord(3L);
        if (rec == null) {
            System.out.println("não tem");
        } else {
            rec.setContent("mudança de planos");
        }
        table.flushDB();
    }

    public long testMultipleInsertions(Table table, int amount, boolean ordered, boolean display) throws Exception {
        table.initLoad();

        long[] array = new long[amount];
        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        if (!ordered) {
            Utils.shuffleArray(array);
        }

        long start = System.currentTimeMillis();
        for (int i = 0; i < array.length; i++) {
            if (display) {
                System.out.println("adding primary key =  " + array[i]);
                System.out.println(Block.BLOCK_LEN + " - " + Table.FILLER + " - " + Block.FILLER);
            }
            table.addRecord(array[i], "Novo registros " + array[i]);

        }

        table.flushDB();
        long end = System.currentTimeMillis();
        return (end-start);
    }

    public long testMultipleSearches(Table table, int amount, boolean ordered, boolean display) throws Exception {
        
        table.initLoad();

        long[] array = new long[amount];
        for (int i = 0; i < array.length; i++) {
            array[i] = i;
        }

        if (!ordered) {
            Utils.shuffleArray(array);
        }

        long start = System.currentTimeMillis();

        for (int i = 0; i < array.length; i++) {
            Record rec = table.getRecord(array[i]);
            if (!display) {
                continue;
            }

            if (rec != null) {
                System.out.println(rec.getContent() + " in block " + rec.getBlockId());
            } else {
                System.out.println("erro: inexistente");
            }
        }
        long end = System.currentTimeMillis();
        return (end-start);
    }

    public static void main(String[] args) {
        try {
            Main m = new Main();
            Table table = Directory.getTable("", "table.ibd");
            m.testCreation(table);
            Params.RECORDS_ADDED = 0;
            Params.RECORDS_REMOVED = 0;
            Params.BLOCKS_LOADED = 0;
            Params.BLOCKS_SAVED = 0;
            long timeInsert = m.testMultipleInsertions(table, 1000, false, false);
            
            long timeQuery = m.testMultipleSearches(table, 1000, true, true);
            
            System.out.println("records added during reorganization " + Params.RECORDS_ADDED);
            System.out.println("records removed during reorganization " + Params.RECORDS_REMOVED);
            System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
            System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);
            
            System.out.println("time to insert: " + timeInsert);
            System.out.println("time to query: " + timeQuery);

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
