package de.hpi.fgis.loducc.statistics;

import de.hpi.fgis.loducc.IntegerKey;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

public class IntegerStatistics {
	
	/**
	 *
	 */
	public HashMap<String, Double> getUniquenessDensityKeyness(Integer property, HashMap<IntegerKey, Integer> propertyValues, int entityCount) {
		HashMap<String, Double> result = new HashMap<String, Double>();
		Double uniqueness = null;
		Double density = null;
		Double overallCount = 0.0;
		Double uniqueValues = 0.0;
		Iterator<Entry<IntegerKey, Integer>> it = propertyValues.entrySet().iterator();
	    while (it.hasNext()) {
	        Entry<IntegerKey, Integer> pairs = it.next();
	        //ArrayList<String> property = (ArrayList<String>) pairs.getKey();
	        Integer count = (Integer) pairs.getValue();
	        overallCount += count;
	        if (count == 1) {
	        	uniqueValues += 1;
	        }
		}
		/*
	    if (out != null) {
	    	out.print(className + "," + propertyName + "," + entityCount + ",");
	    } else {
	    	System.out.print(className + "," + propertyName + "," + entityCount + ",");
	    }
	    */
	    if (overallCount > 0) {
		    uniqueness = uniqueValues/overallCount;
		    density = overallCount/entityCount;
			result.put("uniqueness", uniqueness);
			result.put("density", density);
			result.put("keyness", Keyness.getKeyness(uniqueness, density));
			result.put("properties", (double) propertyValues.size());
	    }
		return result;
	}
}
