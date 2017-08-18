package src.utils;


import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

/**
 * A Parser for getting single values from an entire dataset line.
 * 
 * @author loren
 *
 */
public class LineParser {
	
	List<String> tokens;
	
	public LineParser(){
		tokens = new ArrayList<String>();
		
	}
	
	/**
	 * Performs line parsing. Given a dataset row the methods splits it in single values.
	 * 
	 * @param row The row to be parsed.
	 * @return The List of parsed values.
	 */
	public List<String> parse(String row){
		tokens.clear();
		
		String[] t = row.split(",");
		Iterator<String> iterator = Arrays.asList(t).iterator();
		
		while(iterator.hasNext()){
			String token = iterator.next();
			if(!token.startsWith("\"")){
				tokens.add(token);
				
			} else {
				token = token.substring(1).concat(", ".concat(iterator.next()));
				while(!token.endsWith("\"") && iterator.hasNext()){
					token = token.concat(iterator.next());
				
				}
				
				tokens.add(token.substring(0, token.length()-1));
			
			}	
		}
	
		return tokens;		
		
	}
}
