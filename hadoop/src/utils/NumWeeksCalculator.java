package src.utils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import src.types.YearWeekWritable;

/**
 * Calculator of the total number of weeks throughout the dataset.  
 * 
 * @author loren
 *
 */
public class NumWeeksCalculator {
	
	private static Path path;
	private static int numWeeks;
	private static Configuration conf;
		
	public NumWeeksCalculator() {
		
		NumWeeksCalculator.numWeeks = -1;
		
	}

	
	/**
	 * Performs calculation of the number of weeks  
	 * @throws IOException 
	 */
	private static void calculate() throws IOException {
		
		LineParser parser = new LineParser();
		Set<YearWeekWritable> ywSet = new HashSet<YearWeekWritable>();
		
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        
        BufferedReader reader = new BufferedReader(new InputStreamReader(inputStream));
        
        String buffer = reader.readLine();
        
        while (buffer != null) {
        	
        	Map<Attribute, String> map = Attribute.getMapping(parser.parse(buffer));
        	String date = map.get(Attribute.DATE);
  
        	YearWeekWritable yearWeek = new YearWeekWritable();
        	
			try {
				yearWeek.setFromDate(date);
			} catch (ParseException e) {
				//Skips the line in case of bad record.
				buffer = reader.readLine();
				continue;
			}
			
        	ywSet.add(yearWeek);
        	
        	buffer = reader.readLine();
        }
        
        numWeeks = ywSet.size();
        
        reader.close();
        fs.close();
        
	}
	
	/**
	 * Returns the number of weeks, calculating it if not calculated yet.
	 * 
	 * @return number of weeks.
	 * @throws IOException 
	 */
	public int getNumWeeks() throws IOException{
		
		if(numWeeks == -1)
			calculate();
		
		return numWeeks;
		
	}

	/**
	 * Sets the Job Configuration in order to make the class access HDFS.
	 * 
	 * @param conf
	 */
	public void setConf(Configuration conf) {
		
		NumWeeksCalculator.conf = conf;
		
	}

	/**
	 * Sets the dataset HDFS path.
	 * 
	 * @param filePath
	 */
	public void setPath(String filePath) {
		
		NumWeeksCalculator.path = new Path(filePath);
		
	}
	

}
