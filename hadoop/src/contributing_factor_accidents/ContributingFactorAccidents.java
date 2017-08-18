package src.contributing_factor_accidents;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import src.types.SumAvgWritable;

/**
 * Driver Class for 2nd request: number of accidents and percentage of number of deaths per contributing factor in the dataset.
 * This class runs the second request job, given the input and output paths passed as main arguments.
 * 
 * @author loren
 *
 */
public class ContributingFactorAccidents extends Configured implements Tool {

	@Override
	public int run(String[] args) throws Exception {
		String inPath = args[0];
		String outPath = args[1];
		
		Configuration conf = getConf();
		
        JobConf job = new JobConf(conf, ContributingFactorAccidents.class);
        job.setJobName("contributing_factor_accidents");
	
        job.setMapOutputValueClass(IntWritable.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(SumAvgWritable.class);
			
        job.setMapperClass(ContributingFactorAccidentsMap.class);
        job.setReducerClass(ContributingFactorAccidentsReduce.class);
			
        job.setInputFormat(TextInputFormat.class);
        job.setOutputFormat(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job, new Path(inPath));
		FileOutputFormat.setOutputPath(job, new Path(outPath));
		
		JobClient.runJob(job);

		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		int res = ToolRunner.run(new Configuration(), new ContributingFactorAccidents(), args);
        System.exit(res);
		
	}
}
