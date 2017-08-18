package src.week_borough_accidents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapreduce.Reducer;

import src.types.IntIntWritable;
import src.types.WeekBoroughKey;

/**
 * First Reducer class for the 3rd request.
 * 
 * @author loren
 *
 */
public class WeekBoroughAccidentsReduce extends Reducer<WeekBoroughKey, IntWritable, WeekBoroughKey, IntIntWritable>  {

	@Override
	public void cleanup(Context context) throws IOException {
		//Default 
	}

	@Override
	/**
	 * This method computes the total number accidents and the number of lethal ones,
	 * for a year-week-borough trio. 
	 * Starting from a list of couples <yearweek_borough, [0|1]> it sums up ones to get final result.
	 */
	public void reduce(WeekBoroughKey key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
		
		int lethalAccidents = 0;
		int accidents = 0;		
		
		
		Iterator<IntWritable> valuesIterator = values.iterator();
		
		while (valuesIterator.hasNext()) {
			lethalAccidents += valuesIterator.next().get();
			accidents++;
		}
		
		context.write(key, new IntIntWritable(lethalAccidents, accidents));
	}

}
