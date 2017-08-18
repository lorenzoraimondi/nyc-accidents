package src.contributing_factor_accidents;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

import src.types.SumAvgWritable;

/**
 * Reducer class for the 2nd request.
 * 
 * @author loren
 *
 */
public class ContributingFactorAccidentsReduce extends MapReduceBase implements Reducer<Text, IntWritable, Text, SumAvgWritable>  {
	
	@Override
	/**
	 * This method computes the total number of lethal accidents 
	 * and their percentage on the overall accidents, for a contributing factor. 
	 * 
	 */
	public void reduce(Text key, Iterator<IntWritable> values, OutputCollector<Text, SumAvgWritable> output, Reporter reporter) throws IOException {
		
		int sum = 0;
		int count = 0;
		
		while (values.hasNext()) {
			sum += values.next().get();
			count++;
		}
		
		IntWritable sumWritable = new IntWritable(count);
		FloatWritable avgWritable = new FloatWritable(100*(float)sum/count);
		
		output.collect(key, new SumAvgWritable(sumWritable, avgWritable));
	}

}
