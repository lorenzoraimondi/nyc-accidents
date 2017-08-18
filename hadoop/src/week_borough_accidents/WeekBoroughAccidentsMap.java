package src.week_borough_accidents;

import java.io.IOException;
import java.text.ParseException;
import java.util.Map;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import src.types.WeekBoroughKey;
import src.types.YearWeekWritable;
import src.utils.Attribute;
import src.utils.LineParser;

/**
 * First Mapper class for the 3rd request.
 * 
 * @author loren
 *
 */

public class WeekBoroughAccidentsMap extends Mapper<LongWritable, Text, WeekBoroughKey, IntWritable> {
	
	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	//private final static Text missingBorough = new Text("0");
	private final static Text missingBorough = new Text("MV");
	
	@Override
	public void cleanup(Context context) throws IOException {
		//Default 
	}
	
	@Override
	/**
	 * Starting from couple <line_number, line> this methods emits a couple <yearweek_borough,[0|1]>, 
	 * depending whether the accidents was mortal or not.
	 */
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	
		YearWeekWritable yw = new YearWeekWritable();
		Text borough = new Text();
		WeekBoroughKey wb = new WeekBoroughKey();
		int killed;
		
		LineParser lp = new LineParser();
		Map<Attribute, String> map = Attribute.getMapping(lp.parse(value.toString()));
		
		borough.set(map.get(Attribute.BOROUGH));
		
		//TODO https://hadoop.apache.org/docs/r1.2.1/mapred_tutorial.html#Skipping+Bad+Records
		if(borough.equals(missingBorough)){
			//System.out.println("Bad record, skip it");
			return;
		} else {
			wb.setBorough(borough);
			
			if(key.get() > 0){ //to skip first line!
				try {
					yw.setFromDate(map.get(Attribute.DATE));
					
				} catch (ParseException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
					
				}
				
				wb.setYearWeek(yw);
				
				//If the number is a missing value, I can assume it is a 0.
				try{
					killed = Integer.parseInt(map.get(Attribute.NUMBER_OF_PERSONS_KILLED));
				} catch (NumberFormatException e){
					killed = 0;
				}
				
				if(killed > 0){
					context.write(wb, one);
					
				} else {
					context.write(wb, zero);
					
				}	
			}	
		}
			
		
	}
}

