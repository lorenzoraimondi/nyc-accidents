package src.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.text.ParseException;
import java.util.Objects;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.WritableComparable;

import src.utils.YearWeekCalculator;

/**
 * Defines a wrapper object between the couple of attributes Year and Week. 
 * 
 * @author loren
 *
 */
public class YearWeekWritable implements WritableComparable<YearWeekWritable>{

	private IntWritable week;
	private IntWritable year;
	
	/**
	 * 
	 * @param year Year to be set to the wrapper object
	 * @param week Week to be set to the wrapper object
	 */
	public YearWeekWritable(IntWritable year, IntWritable week){
		this.year = year;
		this.week = week;
		
	}
	
	/**
	 * 
	 * @param year Year to be set to the wrapper object
	 * @param week Week to be set to the wrapper object
	 */
	public YearWeekWritable(int year, int week){
		this.year = new IntWritable(year);
		this.week = new IntWritable(week);
		
	}
	
	/**
	 * Default constructor that creates an empty object. Values can than be set using setter methods.
	 */
	public YearWeekWritable() {
		this.year = new IntWritable();
		this.week = new IntWritable();

	}
	
	/**
	 * Performs the year and week setting directly calculating them from a date, in the format "MM/DD/YYYY" 
	 * 
	 * @param inputDate
	 * @throws ParseException
	 */
	public void setFromDate(String inputDate) throws ParseException{
		YearWeekCalculator calculator = new YearWeekCalculator(inputDate);

		this.week = new IntWritable(calculator.getWeek());
		this.year = new IntWritable(calculator.getYear());
		
	
	}
	
	/**
	 * Sets the wrapped week to the given one.
	 * 
	 * @param value week value to be wrapped.
	 */
	public void setWeek(int value){
		this.week.set(value);
	
	}
	
	/**
	 * Sets the wrapped year to the given one.
	 * 
	 * @param value year value to be wrapped.
	 */
	public void setYear(int value){
		this.year.set(value);
	
	}
	
	/**
	 * @return the week object wrapped in this object. 
	 */
	public IntWritable getWeek(){
		return week;
	 
	}
	 
	/**
	 * @return the year object wrapped in this object. 
	 */
	public IntWritable getYear(){
		return year;
	 
	}
	
	@Override
	/**
	 * Overrides default readFields method.
	 * It de-serializes the byte stream data.
	 */
	public void readFields(DataInput in) throws IOException {
		year.readFields(in);
		week.readFields(in);
		
	}
	
	@Override
	/**
	 * Overrides default write method.
	 * It serializes object data into byte stream data.
	 */
	public void write(DataOutput out) throws IOException {
		year.write(out);
		week.write(out);
		
	}
	 
	@Override
	public int compareTo(YearWeekWritable yw) {
		if (year.compareTo(yw.getYear())==0){
			return (week.compareTo(yw.getWeek()));
		}
		else return (year.compareTo(yw.getYear()));
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof YearWeekWritable){
			YearWeekWritable other = (YearWeekWritable) o;
			
			return week.equals(other.getWeek()) && year.equals(other.getYear());
		}
		
		return false;
	
	}
	
	@Override
	public int hashCode(){
		return Objects.hash(this.year, this.week);
		
	}
	
	@Override
	public String toString(){
		return year.toString()+ "\t" + week.toString();
		
	}


}