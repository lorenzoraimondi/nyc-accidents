package src.types;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Objects;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;

/**
 * Defines a wrapper object between the couple of attributes Year-Week and Borough. 
 * 
 * @author loren
 *
 */
public class WeekBoroughKey implements WritableComparable<WeekBoroughKey> {
	
	YearWeekWritable yearWeek;
	Text borough;
	
	/**
	 * @param yearWeek Year-Week to be set to the wrapper object 
	 * @param borough Borough to be set to the wrapper object
	 */
	public WeekBoroughKey(YearWeekWritable yearWeek, Text borough){
		this.yearWeek = yearWeek;
		this.borough = borough;
		
	}
	
	/**
	 * 
	 * @param year Year to be set to the wrapper object
	 * @param week Week to be set to the wrapper object
	 * @param borough Borough to be set to the wrapper object
	 */
	public WeekBoroughKey(int year, int week, String borough){
		this.yearWeek = new YearWeekWritable(year,week);
		this.borough = new Text(borough);
		
	}
	
	/**
	 * Default constructor that creates an empty object. Values can than be set using setter methods.
	 */
	public WeekBoroughKey(){
		this.yearWeek = new YearWeekWritable();
		this.borough = new Text();
		
	}
	
	/**
	 * Sets the wrapped YearWeekWritable to the given one.
	 * 
	 * @param value object to be wrapped.
	 */
	public void setYearWeek(YearWeekWritable yearWeek){
		this.yearWeek = yearWeek;
		
	}
	
	/**
	 * Sets the wrapped Borough to the given one.
	 * 
	 * @param value object to be wrapped.
	 */
	public void setBorough(Text borough){
		this.borough = borough;
		
	}
	
	/**
	 * @return YearWeek object wrapped in this object. 
	 */
	public YearWeekWritable getYearWeek(){
		return this.yearWeek;
		
	}
	
	/**
	 * @return Borough object wrapped in this object. 
	 */
	public Text getBorough(){
		return this.borough;
		
	}

	@Override
	/**
	 * Overrides default readFields method.
	 * It de-serializes the byte stream data.
	 */
	public void readFields(DataInput in) throws IOException {
		yearWeek.readFields(in);
		borough.readFields(in);
		
	}

	@Override
	/**
	 * Overrides default write method.
	 * It serializes object data into byte stream data.
	 */
	public void write(DataOutput out) throws IOException {
		yearWeek.write(out);
		borough.write(out);
		
	}

	@Override
	public int compareTo(WeekBoroughKey o) {
		if (yearWeek.compareTo(o.getYearWeek())==0){
			return (borough.compareTo(o.getBorough()));
		}
		else return (yearWeek.compareTo(o.getYearWeek()));
	}
	
	@Override
	public boolean equals(Object o) {
		if (o instanceof WeekBoroughKey){
			WeekBoroughKey other = (WeekBoroughKey) o;
			
			return yearWeek.equals(other.getYearWeek()) && borough.equals(other.getBorough());
		}
		
		return false;
	
	}
	
	@Override
	public int hashCode(){
		return Objects.hash(this.yearWeek, this.borough);
		
	}
	
	@Override
	public String toString(){
		return yearWeek.toString()+ "\t" + borough.toString();
		
	}

}
