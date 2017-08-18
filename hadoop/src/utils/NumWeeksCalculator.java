package src.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.text.ParseException;
import java.util.HashSet;
import java.util.Map;
import java.util.Scanner;
import java.util.Set;

import src.types.YearWeekWritable;

/**
 * Calculator of the total number of weeks throughout the dataset.  
 * 
 * @author loren
 *
 */
public class NumWeeksCalculator {
	
	private String path;
	private int numWeeks;
	
	public NumWeeksCalculator(String path) throws FileNotFoundException, ParseException{
		this.path = path;
		
		calculate();
	}
	
	/**
	 * Performs calculation of the number of weeks  
	 * 
	 * @throws FileNotFoundException
	 */
	private void calculate() throws FileNotFoundException {
		LineParser parser = null;
		Set<YearWeekWritable> ywSet = new HashSet<YearWeekWritable>();
        Scanner scanner = new Scanner(new File(path));
        
        while (scanner.hasNext()) {
        	String line = scanner.nextLine();
        	parser = new LineParser();
        	Map<Attribute, String> map = Attribute.getMapping(parser.parse(line));
        	String date = map.get(Attribute.DATE);
  
        	YearWeekWritable yearWeek = new YearWeekWritable();
			try {
				yearWeek.setFromDate(date);
			} catch (ParseException e) {
				//Skips the line in case of bad record.
				continue;
			}
			
        	ywSet.add(yearWeek);
        }
        
        numWeeks = ywSet.size();
        scanner.close();
	}
	
	/**
	 * Returns the number of weeks.
	 * 
	 * @return number of weeks.
	 */
	public int getNumWeeks(){
		return numWeeks;
	}
	

}
