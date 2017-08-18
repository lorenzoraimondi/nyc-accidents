package src.contributing_factor_accidents;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

import src.utils.Attribute;
import src.utils.LineParser;

/**
 * Mapper class for the 2nd request.
 * 
 * @author loren
 *
 */
public class ContributingFactorAccidentsMap extends MapReduceBase implements Mapper<LongWritable, Text, Text, IntWritable> {

	private final static IntWritable one = new IntWritable(1);
	private final static IntWritable zero = new IntWritable(0);
	//private final static Text missingFactor = new Text("0");
	private final static Text missingFactor = new Text("MV");
	private final static Text unspecifiedFactor = new Text("Unspecified");
	
	@Override
	/**
	 * Starting from couple <line_number, line> this methods emits a couple <contributing_factor,[0|1]>, 
	 * depending whether the accidents was mortal or not.
	 */
	public void map(LongWritable key, Text value, OutputCollector<Text, IntWritable> output, Reporter reporter) throws IOException {
		int killed;
		Set<Text> contributingFactors= new HashSet<Text>();
		
		LineParser lp = new LineParser();
		Map<Attribute, String> map = Attribute.getMapping(lp.parse(value.toString()));
		
		
		if(key.get() > 0){ //to skip first line!
			contributingFactors.add(new Text(map.get(Attribute.CONTRIBUTING_FACTOR_VEHICLE_1)));
			contributingFactors.add(new Text(map.get(Attribute.CONTRIBUTING_FACTOR_VEHICLE_2)));
			contributingFactors.add(new Text(map.get(Attribute.CONTRIBUTING_FACTOR_VEHICLE_3)));
			contributingFactors.add(new Text(map.get(Attribute.CONTRIBUTING_FACTOR_VEHICLE_4)));
			contributingFactors.add(new Text(map.get(Attribute.CONTRIBUTING_FACTOR_VEHICLE_5)));
			
			//If the number is a missing value, I can assume it is a 0.
			try{
				killed = Integer.parseInt(map.get(Attribute.NUMBER_OF_PERSONS_KILLED));
			} catch (NumberFormatException e){
				killed = 0;
			}						
			
			for(Text cf : contributingFactors){
				if(!cf.equals(missingFactor) && !cf.equals(unspecifiedFactor)){
					if(killed > 0){
						output.collect(cf, one);
					} else if(killed == 0){
						output.collect(cf, zero);
					}	
				}
				
			}
		}
	}
}
