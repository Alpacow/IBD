package ibd2;

import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;

/**
 *
 * @author pccli
 */
public class Utils {

    static public Table createTable(String folder, String name, int size, boolean shuffled, int range) throws Exception{
        Table table = Directory.getTable(folder, name); 
        table.createTable();
        table.initLoad();

        long[] array1 = new long[size/range];
        for (int i = 0; i < array1.length; i++) {
            array1[i] = i*range;
        }

        if (shuffled)
            shuffleArray(array1);

        for (int i =0; i < array1.length; i++) {
            table.addRecord(array1[i], name +"("+array1[i]+")");
            //table.addRecord(array1[i], String.valueOf(array1[i]));
            //table.addRecord(array1[i], "0");
        }
        table.flushDB();
        return table;
    }

    public static void shuffleArray(long[] ar) {
        // If running on Java 6 or older, use `new Random()` on RHS here
        Random rnd = ThreadLocalRandom.current();
        for (int i = ar.length - 1; i > 0; i--) {
            int index = rnd.nextInt(i + 1);
            // Simple swap
            long a = ar[index];
            ar[index] = ar[i];
            ar[i] = a;
        }
    }

}
