package de.hpi.fgis.loducc;

import de.hpi.fgis.loducc.statistics.IntegerStatistics;

import java.util.*;

/*
    Keyness for ProLOD
 */

public class Keyness {

    public Keyness() {

    }

    public HashMap<Integer, HashMap<String, Double>> getKeyness(String ontologyClass, HashMap<Integer, HashMap<Integer, Integer>> triples) {
        HashMap<Integer, HashMap<String, Double>> results = new HashMap<Integer, HashMap<String, Double>>();
        HashMap<Integer, HashMap<IntegerKey, Integer>> propertiesPropertyValueMap = new HashMap<Integer, HashMap<IntegerKey, Integer>>();

        Iterator it = triples.entrySet().iterator();
        while (it.hasNext()) {
            Map.Entry pair = (Map.Entry) it.next();
            Integer subject = (Integer) pair.getKey();

            boolean firstProperty = true;
            Integer currentProperty = null;
            List<Integer> currentPropertyValues = new ArrayList<Integer>();

            HashMap<Integer, Integer> subjectTriples = (HashMap<Integer, Integer>) pair.getValue();
            Iterator tripleIt = subjectTriples.entrySet().iterator();
            while (tripleIt.hasNext()) {
                Map.Entry triple = (Map.Entry) tripleIt.next();
                Integer property = (Integer) triple.getKey();
                Integer object = (Integer) triple.getValue();

                if (currentProperty != property) {
                    if (firstProperty) {
                        firstProperty = false;
                    } else {
                        IntegerKey keys = new IntegerKey(currentPropertyValues);
                        if (propertiesPropertyValueMap.containsKey(currentProperty)) {
                            HashMap<IntegerKey, Integer> existingPropertyValues = propertiesPropertyValueMap.get(currentProperty);
                            existingPropertyValues = addPropertyValueCount(existingPropertyValues, keys);
                            propertiesPropertyValueMap.put(currentProperty, existingPropertyValues);
                        } else {
                            HashMap<IntegerKey, Integer> propertyValues = new HashMap<IntegerKey, Integer>();
                            propertyValues = addPropertyValueCount(propertyValues, keys);
                            propertiesPropertyValueMap.put(currentProperty, propertyValues);
                        }
                    }
                    currentProperty = property;
                    currentPropertyValues = new ArrayList<Integer>();
                }
                currentPropertyValues.add(object);
            }
            // add last property
            if (currentPropertyValues.size() > 0) {
                IntegerKey keys = new IntegerKey(currentPropertyValues);
                if (propertiesPropertyValueMap.containsKey(currentProperty)) {
                    HashMap<IntegerKey, Integer> existingPropertyValues = propertiesPropertyValueMap.get(currentProperty);
                    existingPropertyValues = addPropertyValueCount(existingPropertyValues, keys);
                    propertiesPropertyValueMap.put(currentProperty, existingPropertyValues);
                } else {
                    HashMap<IntegerKey, Integer> propertyValues = new HashMap<IntegerKey, Integer>();
                    propertyValues = addPropertyValueCount(propertyValues, keys);
                    propertiesPropertyValueMap.put(currentProperty, propertyValues);
                }
            }
        }

        for (Iterator iterator = propertiesPropertyValueMap.keySet().iterator(); iterator.hasNext();) {
            Integer property = (Integer) iterator.next();
            HashMap<IntegerKey, Integer> propertyValues = propertiesPropertyValueMap.get(property);

            IntegerStatistics stats = new IntegerStatistics();
            results.put(property, stats.getUniquenessDensityKeyness(property, propertyValues, triples.size()));
        }

        return results;
    }

    private HashMap<IntegerKey, Integer> addPropertyValueCount(HashMap<IntegerKey, Integer> propertyValues, IntegerKey key) {
        if (!propertyValues.containsKey(key)) {
            propertyValues.put(key, 1);
        } else {
            propertyValues.put(key, propertyValues.get(key) + 1);
        }
        return propertyValues;
    }


    public static void main(String[] args) {
        new Keyness();
    }

}
