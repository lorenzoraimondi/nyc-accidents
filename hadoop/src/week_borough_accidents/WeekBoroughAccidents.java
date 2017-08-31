package src.week_borough_accidents;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import src.types.IntIntWritable;
import src.types.WeekBoroughKey;
import src.utils.NumWeeksCalculator;

/**
 * Driver Class for 3rd request: number of accidents and average number of lethal accidents per week per borough.
 * This class runs the third request jobs, given the input and output paths passed as main arguments.
 * This class performs chaining between the three jobs required to accomplish the request.
 * 
 * @author loren
 *
 */
public class WeekBoroughAccidents extends Configured implements Tool {
	
	private static String intermediatePath;
	private static String outPath1;
	private static String outPath2;
	private static String filePath;
	private static NumWeeksCalculator weeksCalc;

	@Override
	public int run(String[] args) throws Exception {
		
		Configuration conf = getConf();
		
        weeksCalc.setConf(conf);
        weeksCalc.setPath(filePath);
        
		//System.out.println(weeksCalc.getNumWeeks());
		conf.setInt("num_weeks",weeksCalc.getNumWeeks());
		
		
		
		Job job1 = Job.getInstance(conf);
        job1.setJobName("week_borough_accidents");
        job1.setJarByClass(WeekBoroughAccidents.class);
		
        job1.setMapOutputValueClass(IntWritable.class);
        job1.setOutputKeyClass(WeekBoroughKey.class);
        job1.setOutputValueClass(IntIntWritable.class);
			
        job1.setMapperClass(WeekBoroughAccidentsMap.class);
        job1.setReducerClass(WeekBoroughAccidentsReduce.class);
			
        job1.setInputFormatClass(TextInputFormat.class);
        job1.setOutputFormatClass(SequenceFileOutputFormat.class);
		
		FileInputFormat.setInputPaths(job1, new Path(args[0]));
		FileOutputFormat.setOutputPath(job1, new Path(intermediatePath));
		
		job1.waitForCompletion(true);


		
		Job job2 = Job.getInstance(conf);
		job2.setJobName("week_borough_average");
        job2.setJarByClass(WeekBoroughAccidents.class);
		
        job2.setMapOutputValueClass(IntIntWritable.class);
        job2.setOutputKeyClass(Text.class);
        job2.setOutputValueClass(FloatWritable.class);
			
        job2.setMapperClass(BoroughLethalAccidentsMap.class);
        job2.setReducerClass(BoroughLethalAccidentsReduce.class);
			
        job2.setInputFormatClass(SequenceFileInputFormat.class);
        job2.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job2, new Path(intermediatePath));
		FileOutputFormat.setOutputPath(job2, new Path(outPath1));
		
		job2.waitForCompletion(true);
		
		
		
		Job job3 = Job.getInstance(conf);
        job3.setJobName("week_borough_average");
        job3.setJarByClass(WeekBoroughAccidents.class);
		
        job3.setMapOutputValueClass(IntIntWritable.class);
        job3.setOutputKeyClass(WeekBoroughKey.class);
        job3.setOutputValueClass(IntIntWritable.class);
			
        job3.setMapperClass(Mapper.class);
        job3.setReducerClass(Reducer.class);
			
        job3.setInputFormatClass(SequenceFileInputFormat.class);
        job3.setOutputFormatClass(TextOutputFormat.class);
		
		FileInputFormat.setInputPaths(job3, new Path(intermediatePath));
		FileOutputFormat.setOutputPath(job3, new Path(outPath2));
		
		job3.waitForCompletion(true);
		
		return 0;
	}

	public static void main(String[] args) throws Exception {
		
		intermediatePath = args[1]+"/temp";
		outPath1 = args[1]+"/out_job3_1";
		outPath2 = args[1]+"/out_job3_2";
		filePath = args[0];
		
		weeksCalc = new NumWeeksCalculator();
		
		int res = ToolRunner.run(new Configuration(), new WeekBoroughAccidents(), args);
        System.exit(res);
		
	}
}
