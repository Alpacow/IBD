package ibd2.query;

import ibd2.Params;
import ibd2.Table;
import static ibd2.Utils.createTable;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Sergio
 */
public class Main {



    public void testSimpleQuery() throws Exception {
        Table table = createTable("c:\\teste\\ibd","t1.ibd",1000, true, 1);
        Operation scan = new TableScan(table);

        scan.open();
        while (scan.hasNext()){
            Tuple r = scan.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }

    public void testOrderedQuery() throws Exception {
        Table table = createTable("c:\\teste\\ibd","t1.ibd",1000, true, 1);
        //Operation table = new OrderedSource(new TableSource(new Table("c:\\teste\\ibd", "t1.ibd")));
        //Operation scan = new OrderedScan(new TableScan(table));
        Operation scan = new OrderedScan(new TableScan(table));

        scan.open();
        while (scan.hasNext()){
            Tuple r = (Tuple)scan.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }

    public void testMultipleJoinsQuery() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd","t1.ibd",1000, false, 1);
        Table table2 = createTable("c:\\teste\\ibd","t2.ibd",1000, false, 1);
        Table table3 = createTable("c:\\teste\\ibd","t3.ibd",1000, false, 1);

        Operation scan1 = new TableScan(table1);
        Operation scan2 = new TableScan(table2);
        Operation scan3 = new TableScan(table3);

        Operation join1 = new NestedLoopJoin(scan1, scan2);
        Operation join2 = new NestedLoopJoin(join1, scan3);

        join2.open();
        while (join2.hasNext()){
            Tuple r = (Tuple)join2.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }

    public void testNestedLoopJoinQuery() throws Exception {

        Table table1 = createTable("c:\\teste\\ibd","t1.ibd",1000, false, 1);
        Table table2 = createTable("c:\\teste\\ibd","t2.ibd",1000, false, 1);

        Operation scan1 = new TableScan(table1);
        Operation scan2 = new TableScan(table2);

        Operation join1 = new NestedLoopJoin(scan1, scan2);

        join1.open();
        while (join1.hasNext()){
            Tuple r = join1.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }

    public void testMergeJoinQuery1() throws Exception {

        //tabelas criadas com 1000 registros ordenados
        Table table1 = createTable("c:\\teste\\ibd","t1.ibd",1000, false, 1);
        Table table2 = createTable("c:\\teste\\ibd","t2.ibd",1000, false, 50);

        //não é necessário ordenar as tabelas, pois os registros já foram inseridos em ordem
        Operation scan1 = new TableScan(table1);
        Operation scan2 = new TableScan(table2);

        Operation join1 = new MergeJoin(scan1, scan2);


        join1.open();
        while (join1.hasNext()){
            Tuple r = join1.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }

    public void testMergeJoinQuery2() throws Exception {

        //tabelas criadas com 1000 registros desordenados
        Table table1 = createTable("c:\\teste\\ibd","t1.ibd",1000, true, 1);
        Table table2 = createTable("c:\\teste\\ibd","t2.ibd",1000, true, 50);

        //É necessário ordenar as tabelas, pois os registros já foram inseridos em ordem
        Operation scan1 = new OrderedScan(new TableScan(table1));
        Operation scan2 = new OrderedScan(new TableScan(table2));

        Operation join1 = new MergeJoin(scan1, scan2);

        join1.open();
        while (join1.hasNext()){
            Tuple r = join1.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }

    }


    public void testMultipleMergeJoinQuery() throws Exception {
        Table table1 = createTable("c:\\teste\\ibd","table1.ibd",1000, false, 1);
        Table table2 = createTable("c:\\teste\\ibd","table2.ibd",1000, false, 50);
        Table table3 = createTable("c:\\teste\\ibd","table3.ibd",1000, false, 30);


         Operation scan1 = new TableScan(table1);
         //Operation scan1 = new OrderedSource(new TableSource(table1));
         Operation scan2 = new TableScan(table2);
         //Operation scan2 = new OrderedSource(new TableSource(table2));

         Operation scan3 = new TableScan(table3);
         //Operation scan3 = new OrderedSource(new TableSource(table3));
         Operation join1 = new MergeJoin(scan1, scan2);
        Operation join2 = new MergeJoin(scan3, join1);


        join2.open();
        while (join2.hasNext()){
            Tuple r = join2.next();
            System.out.println(r.primaryKey + " - "+r.content);
        }


    }

    /* 

    */
    public void testNestedLoopJoin() throws Exception {
        Table table1 = createTable("","t1.ibd",150, false, 1);
        Table table2 = createTable("","t2.ibd",260, false, 1);
        Table table3 = createTable("","t3.ibd",300, false, 1);
        Table table4 = createTable("","t4.ibd",150, false, 1);
        
        Operation scan1 = new TableScan(table1);
        Operation scan2 = new TableScan(table2);
        Operation scan3 = new TableScan(table3);
        Operation scan4 = new TableScan(table4);
        
        Operation join1 = new NestedLoopJoin(scan1, scan2);
        Operation join2 = new NestedLoopJoin(join1, scan3);
        Operation join = new NestedLoopJoin(join2, scan4);

        Params.BLOCKS_LOADED = 0;
        Params.BLOCKS_SAVED = 0;
        join.open();
        while (join.hasNext()){
            Tuple r = join.next();
            System.out.println(r.primaryKey + " - " + r.content);
        }
        System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
        System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);

    }

    public void testOptmizer () throws Exception {
        Table table1 = createTable("","t1.ibd",150, false, 1);
        Table table2 = createTable("","t2.ibd",260, false, 1);
        Table table3 = createTable("","t3.ibd",300, false, 1);
        Table table4 = createTable("","t4.ibd",150, false, 1);
        
        Operation scan1 = new TableScan(table1);
        Operation scan2 = new TableScan(table2);
        Operation scan3 = new TableScan(table3);
        Operation scan4 = new TableScan(table4);
        
        Operation join1 = new NestedLoopJoin(scan1, scan2);
        Operation join2 = new NestedLoopJoin(join1, scan3);
        Operation join = new NestedLoopJoin(scan4, join2);
        
        FrancielleQueryOptimizer opt = new FrancielleQueryOptimizer();
        Operation query = opt.optimizeQuery(join);
        
        Params.BLOCKS_LOADED = 0;
        Params.BLOCKS_SAVED = 0; 
        query.open();
        while (query.hasNext()){
            Tuple r = query.next();
            System.out.println(r.primaryKey + " - " + r.content);
        }
        System.out.println("blocks loaded during reorganization " + Params.BLOCKS_LOADED);
        System.out.println("blocks saved during reorganization " + Params.BLOCKS_SAVED);
    }

    public static void main(String[] args) {
        try {
            Main m = new Main();
            /*
                blocks loaded during reorganization 72
                blocks saved during reorganization 13
            */
            //m.testNestedLoopJoin();
            /*
                blocks loaded during reorganization 12861
                blocks saved during reorganization 1204
            */
            m.testOptmizer();

        } catch (Exception ex) {
            Logger.getLogger(Main.class.getName()).log(Level.SEVERE, null, ex);
        }
    }
}
