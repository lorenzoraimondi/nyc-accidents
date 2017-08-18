package src.week_borough_accidents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import src.types.IntIntWritable;

/**
 * Second Reducer class for the 3rd request.
 * 
 * @author loren
 *
 */
public class BoroughLethalAccidentsReduce extends Reducer<Text, IntIntWritable, Text, FloatWritable>  {

	@Override
	public void cleanup(Context context) throws IOException {
		//Default
	}

	@Override
	/**
	 * Compute the lethal accidents per week average value. 
	 */
	public void reduce(Text key, Iterable<IntIntWritable> values, Context context) throws IOException, InterruptedException {
		
		float lethalAccidents = 0;
		
		Configuration conf = context.getConfiguration();
		int numberOfWeeks = conf.getInt("num_weeks", -1);  
		
		Iterator<IntIntWritable> valuesIterator = values.iterator();
		
		while (valuesIterator.hasNext()) {
			lethalAccidents += valuesIterator.next().getInt1().get();
	
		}
		
		FloatWritable weeklyAverage = new FloatWritable(lethalAccidents/numberOfWeeks);

		context.write(key, weeklyAverage);
	}

}