/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Algorithms;

/**
 *
 * @author letrung
 */
public class Matrix {
	
	private long id;
	private double[] b;

	/**
            * @param id
            * @param b
	 */
	public Matrix(long id, double[] b) {
		super();
		this.id = id;
		this.b = b;
	}
	/**
	 * @return the id
	 */
	public long getId() {
		return id;
	}
	/**
	 * @param id the id to set
	 */
	public void setId(long id) {
		this.id = id;
	}
	/**
	 * @return the firstName
	 */
	public double[] getArray() {
		return b;
	}
	/**
	 * @param b the firstName to set
	 */
	public void setArray(double[] b) {
		this.b = b;
	}
	
	
	@Override
	public String toString() {
		return ", Array=" + "Matrix [id=" + id;
	}
}
