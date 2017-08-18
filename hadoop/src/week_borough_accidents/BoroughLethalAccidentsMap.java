package src.week_borough_accidents;

import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import src.types.*;

/**
 * Second Mapper class for the 3rd request.
 * 
 * @author loren
 *
 */
public class BoroughLethalAccidentsMap extends Mapper<WeekBoroughKey, IntIntWritable, Text, IntIntWritable> {	 
	
	@Override
	public void cleanup(Context context) throws IOException {
		//Default 
	}
	
	@Override
	/**
	 * Emits a token of type <borough,v> starting from <yearweek_borough, v> tokens.
	 */
	public void map(WeekBoroughKey key, IntIntWritable value, Context context) throws IOException, InterruptedException {
	
		Text borough = key.getBorough();
		
		context.write(borough, value);
	}
}

