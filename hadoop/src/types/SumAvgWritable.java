package src.types;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Writable;

/**
 * Defines a Writable type representing a sum and a percentage.
 * 
 * @author loren
 *
 */
public class SumAvgWritable extends IntFloatWritable implements Writable{

	/**
	 * 
	 * @param sum IntWritable to be wrapped in the object
	 * @param avg FloatWritable to be wrapped in the object
	 */
	public SumAvgWritable(IntWritable sum, FloatWritable avg){
		super(sum,avg);
	}
	
	/**
	 * 
	 * @param sum i integer to be wrapped in the object
	 * @param avg f float to be wrapped in the object
	 */
	public SumAvgWritable(int sum, float avg){
		super(sum,avg);
		
	}
	
	/**
	 * Default constructor that creates an empty object. Values can than be set using setter methods.
	 */
	public SumAvgWritable() {
		super();

	}
	
	/**
	 * Sets the wrapped sum to the given value.
	 * 
	 * @param value sum value to be set.
	 */
	public void setSum(int value){
		super.setInt(value);
	
	}
	
	/**
	 * Sets the wrapped percentage to the given value.
	 * 
	 * @param value float value to be set.
	 */
	public void setAvg(float value){
		super.setFloat(value);
	
	}
	
	/**
	 * @return wrapped IntWritable sum object. 
	 */
	public IntWritable getSum(){
		return super.getInt();
	 
	}
	
	/**
	 * @return wrapped FloatWritable percentage object. 
	 */
	public FloatWritable getAvg(){
		return super.getFloat();
	 
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof SumAvgWritable){
			SumAvgWritable other = (SumAvgWritable) o;
			
			return super.getInt().equals(other.getSum()) && super.getFloat().equals(other.getAvg());
		}
		
		return false;
	
	}
	
	@Override
	public String toString(){
		
		return String.format("%d\t%.2f%%", super.getInt().get(), super.getFloat().get());
		
	}


}