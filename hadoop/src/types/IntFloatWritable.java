package src.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Defines a Writable type composed by an integer and a float number.
 * 
 * @author loren
 *
 */
public class IntFloatWritable  implements Writable{

	private IntWritable i;
	private FloatWritable f;
	
	/**
	 * @param i IntWritable to be wrapped in the object
	 * @param f FloatWritable to be wrapped in the object
	 */
	public IntFloatWritable(IntWritable i, FloatWritable f){
		this.i = i;
		this.f = f;
		
	}
	
	/**
	 * @param i integer to be wrapped in the object
	 * @param f float to be wrapped in the object
	 */
	public IntFloatWritable(int i, float f){
		this.i = new IntWritable(i);
		this.f = new FloatWritable(f);
		
	}
	
	/**
	 * Default constructor that creates an empty object. Values can than be set using setter methods.
	 */
	public IntFloatWritable() {
		this.i = new IntWritable();
		this.f = new FloatWritable();

	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value integer to be set.
	 */
	public void setInt(int value){
		this.i = new IntWritable(value);
	
	}
	
	/**
	 * Sets the wrapped float to the given value.
	 * 
	 * @param value float to be set.
	 */
	public void setFloat(float value){
		this.f = new FloatWritable(value);
	
	}
	
	/**
	 * Sets the wrapped integer to the given value.
	 * 
	 * @param value integer to be set.
	 */
	public void setInt(IntWritable value){
		this.i = value;
	
	}
	
	/**
	 * Sets the wrapped float to the given value.
	 * 
	 * @param value float to be set.
	 */
	public void setFloat(FloatWritable value){
		this.f = value;
	
	}
	
	/**
	 * @return wrapped IntWritable object. 
	 */
	public IntWritable getInt(){
		return i;
	 
	}
	
	/**
	 * @return wrapped FloatWritable object. 
	 */
	public FloatWritable getFloat(){
		return f;
	 
	}
	
	@Override
	/**
	 * Overrides default readFields method.
	 * It de-serializes the byte stream data.
	 */
	public void readFields(DataInput in) throws IOException {
		i.readFields(in);
		f.readFields(in);
		
		
	}
	
	@Override
	/**
	 * Overrides default write method.
	 * It serializes object data into byte stream data.
	 */
	public void write(DataOutput out) throws IOException {
		i.write(out);
		f.write(out);
		
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof IntFloatWritable){
			IntFloatWritable other = (IntFloatWritable) o;
			
			return i.equals(other.getInt()) && f.equals(other.getFloat());
		}
		
		return false;
	
	}
	
	@Override
	public int hashCode(){
		return Objects.hash(this.i, this.f);
		
	}
	
	@Override
	public String toString(){
		
		return String.format("%d\t%.2f", i.get(), f.get());
		
	}


}