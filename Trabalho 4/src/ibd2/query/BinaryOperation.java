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
public interface BinaryOperation extends Operation{
    public Operation getLeftOperation();
    public Operation getRigthOperation();
}
