package src.lethal_accidents;

import src.types.*;
import src.utils.*;
import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

/**
 * Mapper class for the 1st request.
 * 
 * @author loren
 *
 */
public class LethalAccidentsMap extends MapReduceBase implements Mapper<LongWritable, Text, YearWeekWritable, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	
	@Override
	/**
	 * Starting from couple <line_number, line> this methods emits a couple <yearweek,[0|1]>, 
	 * depending whether the accidents was mortal or not.
	 */
	public void map(LongWritable key, Text value, OutputCollector<YearWeekWritable, IntWritable> output, Reporter reporter) throws IOException {
		YearWeekWritable yw = new YearWeekWritable();
		int killed;
		
		LineParser lp = new LineParser();
		Map<Attribute, String> map = Attribute.getMapping(lp.parse(value.toString()));
		
		if(key.get() > 0){ //to skip first line!
			try {
				yw.setFromDate(map.get(Attribute.DATE));
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			//If the number is a missing value, I can assume it is a 0.
			try{
				killed = Integer.parseInt(map.get(Attribute.NUMBER_OF_PERSONS_KILLED));
			} catch (NumberFormatException e){
				killed = 0;
			}
			
			if(killed > 0){
				output.collect(yw, one);
			} else {
				output.collect(yw, zero);
			}	
		}
	}
}
