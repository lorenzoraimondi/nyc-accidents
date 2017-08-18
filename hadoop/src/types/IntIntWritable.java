package src.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Defines a Writable type composed by a couple of integer numbers.
 * 
 * @author loren
 *
 */
public class IntIntWritable  implements Writable{

	private IntWritable int1;
	private IntWritable int2;
	
	/**
	 * 
	 * @param int1 first IntWritable to be wrapped in the object
	 * @param int2 second IntWritable to be wrapped in the object
	 */
	public IntIntWritable(IntWritable int1, IntWritable int2){
		this.int1 = int1;
		this.int2 = int2;
		
	}
	
	/**
	 * 
	 * @param int1 first integer to be wrapped in the object
	 * @param int2 second integer to be wrapped in the object
	 */
	public IntIntWritable(int int1, int int2){
		this.int1 = new IntWritable(int1);
		this.int2 = new IntWritable(int2);
		
	}
	
	/**
	 * Default constructor that creates an empty object. Values can than be set using setter methods.
	 */
	public IntIntWritable() {
		this.int1 = new IntWritable();
		this.int2 = new IntWritable();

	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value first integer to be set.
	 */
	public void setInt1(int value){
		this.int1 = new IntWritable(value);
	
	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value second integer to be set.
	 */
	public void setInt2(int value){
		this.int2 = new IntWritable(value);
	
	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value first integer to be set.
	 */
	public void setInt1(IntWritable value){
		this.int1 = value;
	
	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value second integer to be set.
	 */
	public void setInt2(IntWritable value){
		this.int2 = value;
	
	}
	
	/**
	 * @return first wrapped IntWritable object. 
	 */
	public IntWritable getInt1(){
		return int1;
	 
	}
	
	/**
	 * @return second wrapped IntWritable object. 
	 */
	public IntWritable getInt2(){
		return int2;
	 
	}
	
	@Override
	/**
	 * Overrides default readFields method.
	 * It de-serializes the byte stream data.
	 */
	public void readFields(DataInput in) throws IOException {
		int1.readFields(in);
		int2.readFields(in);
		
		
	}
	
	@Override
	/**
	 * Overrides default write method.
	 * It serializes object data into byte stream data.
	 */
	public void write(DataOutput out) throws IOException {
		int1.write(out);
		int2.write(out);
		
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IntIntWritable){
			IntIntWritable other = (IntIntWritable) o;
			
			return int1.equals(other.getInt1()) && int2.equals(other.getInt2());
		}
		
		return false;
	
	}
	
	@Override
	public int hashCode(){
		return Objects.hash(this.int1, this.int2);
		
	}
	
	@Override
	public String toString(){
		
		return String.format("%d\t%d", int1.get(), int2.get());
		
	}


}