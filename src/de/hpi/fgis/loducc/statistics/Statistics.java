package de.hpi.fgis.loducc.statistics;

import java.io.PrintWriter;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map.Entry;

import de.hpi.fgis.loducc.Key;

public class Statistics {
	
	/**
	 * @param className
	 * @param propertyName
	 * @param propertyValues
	 * @param entityCount
	 * @param out
	 */
	public void getUniquenessDensityKeyness(String className, String propertyName, HashMap<Key, Integer> propertyValues, int entityCount, PrintWriter out) {
		Double uniqueness = null;
		Double density = null;
		Double overallCount = 0.0;
		Double uniqueValues = 0.0;
		Iterator<Entry<Key, Integer>> it = propertyValues.entrySet().iterator();
	    while (it.hasNext()) {
	        Entry<Key, Integer> pairs = it.next();
	        //ArrayList<String> property = (ArrayList<String>) pairs.getKey();
	        Integer count = (Integer) pairs.getValue();
	        overallCount += count;
	        if (count == 1) {
	        	uniqueValues += 1;
	        }
		}
	    if (out != null) {
	    	out.print(className + "," + propertyName + "," + entityCount + ",");
	    } else {
	    	System.out.print(className + "," + propertyName + "," + entityCount + ",");
	    }
	    if (overallCount > 0) {
		    uniqueness = uniqueValues/overallCount;
		    density = overallCount/entityCount;
		    if (out != null) {
		    	out.println(uniqueness + "," + density + "," + Keyness.getKeyness(uniqueness, density));
		    } else {
		    	System.out.println(uniqueness + "," + density + "," + Keyness.getKeyness(uniqueness, density));
		    }
	    } else {
		    if (out != null) {
		    	out.println(",");
		    } else {
		    	System.out.println(",");
		    }
	    }
	}
}
