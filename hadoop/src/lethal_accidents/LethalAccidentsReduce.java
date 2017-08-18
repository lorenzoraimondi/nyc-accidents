package src.lethal_accidents;

import java.io.IOException;
import java.util.Iterator;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import src.types.YearWeekWritable;

/**
 * Reducer class for the 1st request.
 * 
 * @author loren
 *
 */
public class LethalAccidentsReduce extends MapReduceBase implements Reducer<YearWeekWritable, IntWritable, YearWeekWritable, IntWritable>  {


	@Override
	/**
	 * This method computes the total number of lethal accidents for a year-week couple. 
	 * Starting from a list of couples <yearweek, [0|1]> it sums up ones to get final result.
	 */
	public void reduce(YearWeekWritable key, Iterator<IntWritable> values, OutputCollector<YearWeekWritable, IntWritable> output, Reporter reporter) throws IOException {
		int sum = 0;
		while (values.hasNext()) {
			sum += values.next().get();
		}
		output.collect(key, new IntWritable(sum));
	}

}
