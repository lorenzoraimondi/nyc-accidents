package src.utils;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * List of dataset attribute fields.
 * 
 * @author loren
 *
 */
public enum Attribute {
	DATE, 
	TIME, 
	BOROUGH, 
	ZIP_CODE, 
	LATITUDE, 
	LONGITUDE, 
	LOCATION, 
	ON_STREET_NAME, 
	CROSS_STREET_NAME, 
	OFF_STREET_NAME, 
	NUMBER_OF_PERSONS_INJURED, 
	NUMBER_OF_PERSONS_KILLED, 
	NUMBER_OF_PEDESTRIANS_INJURED, 
	NUMBER_OF_PEDESTRIANS_KILLED, 
	NUMBER_OF_CYCLIST_INJURED, 
	NUMBER_OF_CYCLIST_KILLED, 
	NUMBER_OF_MOTORIST_INJURED, 
	NUMBER_OF_MOTORIST_KILLED, 
	CONTRIBUTING_FACTOR_VEHICLE_1,
	CONTRIBUTING_FACTOR_VEHICLE_2, 
	CONTRIBUTING_FACTOR_VEHICLE_3, 
	CONTRIBUTING_FACTOR_VEHICLE_4, 
	CONTRIBUTING_FACTOR_VEHICLE_5, 
	UNIQUE_KEY, 
	VEHICLE_TYPE_CODE_1, 
	VEHICLE_TYPE_CODE_2, 
	VEHICLE_TYPE_CODE_3, 
	VEHICLE_TYPE_CODE_4, 
	VEHICLE_TYPE_CODE_5;

	/**
	 * Returns the Map mapping, for a given dataset line, each attribute with its respective value.
	 * 
	 * @param values the List line's values
	 * @return the mapping attribute-value
	 */
	public static Map<Attribute, String> getMapping(List<String> values){
		
		Map<Attribute, String> attributes = new HashMap<Attribute, String>();
		Iterator<String> iterator = values.iterator();
		
		String v;
		
		for(Attribute a : Attribute.values()){
			try{
				v = iterator.next();
			
			} catch (NoSuchElementException e){
				//This means the line last value is missing, so I insert 0.
				//v = "0";
				v = "MV";
			
			}
			if(v.equals(" ") || v.equals(""))
				//v = "0";
				v = "MV";
			
			attributes.put(a, v);
			
		}
		
		return attributes;
	}
}
