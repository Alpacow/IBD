/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ibd2.query;

/**
 *
 * @author Sergio
 */
public interface  Operation {
    public abstract void open() throws Exception;
    public abstract Tuple next() throws Exception;
    public abstract boolean hasNext() throws Exception;
    public abstract void close() throws Exception;
}
