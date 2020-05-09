package ibd2;

import java.util.Hashtable;

/**
 *
 * @author Sergio
 */
public class Directory {

static Hashtable<String, Table> tables = new Hashtable<>();

public static Table getTable(String folder, String name) throws Exception{
        String key = folder+"/"+name;
        Table t = tables.get(key);
        if (t!=null) return t;
        t = new HeapTable(folder, name);
        //t = new SlackedOrderedTable(folder, name);
        t.key = key;
        tables.put(key, t);
        return t;

    }
    public static Table getTable(String key) throws Exception{
        String folder = getTableFolder(key);
        String file = getTableFile(key);
        return getTable(folder, file);
    }


    public static String getTableFolder(String key){

        return key.substring(0, key.lastIndexOf("/"));

    }

    public static String getTableFile(String key){

        return key.substring(key.lastIndexOf("/"), key.length());

    }


}
