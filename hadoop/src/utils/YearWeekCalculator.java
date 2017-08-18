package src.utils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Calculator of year and week number of a date. 
 * 
 * @author loren
 *
 */
public class YearWeekCalculator {

	private int week;
	private int year;
	private static final String dateFormat = "MM/dd/yyyy";
	
	/**
	 * Calculates of year and week number of a date. Date needs to be in "MM/DD/YYYY" format.
	 *   
	 * @param inputDate date from which calculate year and week.
	 * @throws ParseException 
	 */
	public YearWeekCalculator(String inputDate) throws ParseException{

		SimpleDateFormat df = new SimpleDateFormat(dateFormat);
		Date date = df.parse(inputDate);
		
		Calendar cal = Calendar.getInstance();
		cal.setTime(date);
		
		int year = cal.get(Calendar.YEAR);
		int week = cal.get(Calendar.WEEK_OF_YEAR);
		int month = cal.get(Calendar.MONTH);
		
		if(month==0 && (week==53 || week==52))
			year--;			
		
		if(month==11 && week==1)
			year++;
	
		this.week = week;
		this.year = year;	
		
	}
	
	/**
	 * @return date week number.
	 */
	public int getWeek(){
		return week;
		
	}
	
	/**
	 * @return date year.
	 */
	public int getYear(){
		return year;
		
	}
}
