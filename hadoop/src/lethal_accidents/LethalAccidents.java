package src.lethal_accidents;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import src.types.YearWeekWritable;

/**
 * Driver Class for first request: number of lethal accidents per week throughout the entire dataset.
 * This class runs the first request job, given the input and output paths passed as main arguments.
 * 
 * @author loren
 *
 */
public class LethalAccidents extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String inPath = args[0];
		String outPath = args[1];
		
		Configuration conf = getConf();
		
        JobConf job = new JobConf(conf, LethalAccidents.class);
        job.setJobName("letal_accidents");
		
        job.setOutputKeyClass(YearWeekWritable.class);
        job.setOutputValueClass(IntWritable.class);
			
        job.setMapperClass(LethalAccidentsMap.class);
        job.setCombinerClass(LethalAccidentsReduce.class);
        job.setReducerClass(LethalAccidentsReduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new LethalAccidents(), args);
        System.exit(res);
		
	}
}
