package de.hpi.fgis.loducc;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;

public class ClassConverter {
	ClassConverter(String className, OntModel ontology,
			final List<String> properties, final OntClass ontologyClass) {
		List<String> propertiesInClass = new ArrayList<String>();
		className = className.replace("http://dbpedia.org/ontology/", "").replace("http://www.w3.org/2002/07/owl#Thing", "Thing");
		PrintWriter out;
		int entityCount = 0;
		for (ExtendedIterator<Individual> j = ontology
				.listIndividuals(ontologyClass); j.hasNext(); ++entityCount) {
			j.next();
		}
		if (entityCount > 0) {
			try {
				out = new PrintWriter("classes_nonullcols/" + className + ".csv");
				System.out.println(className + " " + entityCount);
	
				String sparql = "SELECT ?s ?p ?o " + "WHERE { " + " ?s a <"
						+ ontologyClass.getURI() + ">; "
						+ " ?p ?o. } ORDER BY ?s ?p ";
	
				Query qry = QueryFactory.create(sparql);
				QueryExecution qe = QueryExecutionFactory.create(qry, ontology);
				ResultSet rs = qe.execSelect();
	
				HashMap<String, HashMap<String, List<String>>> entities = new HashMap<String, HashMap<String, List<String>>>();
				while (rs.hasNext()) {
					QuerySolution sol = rs.nextSolution();
					RDFNode subject = sol.get("s");
					if (!subject.toString().startsWith("http://dbpedia.org/resource")) {
						continue;
					}
					RDFNode predicate = sol.get("p");
					RDFNode object = sol.get("o");
					if (!entities.containsKey(subject.toString())) {
						HashMap<String, List<String>> predicates = new HashMap<String, List<String>>();
						List<String> objects = new ArrayList<String>();
						objects.add(object.toString());
						predicates.put(predicate.toString(), objects);
						entities.put(subject.toString(), predicates);
					} else {
						HashMap<String, List<String>> predicates = entities
								.get(subject.toString());
						List<String> objects = new ArrayList<String>();
						if (predicates.containsKey(predicate.toString())) {
							objects = predicates.get(predicate.toString());
						}
						objects.add(object.toString());
						predicates.put(predicate.toString(), objects);
						entities.put(subject.toString(), predicates);
					}
					if (!propertiesInClass.contains(predicate.toString())) {
						propertiesInClass.add(predicate.toString());
					}
				}
	
				// out.print("entity");
				int propertyCnt = 0;
				for (Iterator<String> iterator = properties.iterator(); iterator
						.hasNext();) {
					String propertyName = (String) iterator.next();
					if (propertiesInClass.contains(propertyName)) {
						if (propertyCnt > 0) {
							out.print(",");
						}
						out.print(propertyName);
						propertyCnt += 1;
					}
				}
				out.println("");
	
				Iterator<Entry<String, HashMap<String, List<String>>>> it = entities
						.entrySet().iterator();
				while (it.hasNext()) {
					Map.Entry pairs = (Map.Entry) it.next();
					String subject = (String) pairs.getKey();
					HashMap<String, List<String>> predicates = (HashMap<String, List<String>>) pairs.getValue();
	
					// out.print(subject.replaceAll(",", "--comma--"));
	
					int lineNr = 0;
					for (Iterator<String> iterator = properties.iterator(); iterator
							.hasNext();) {
						String propertyName = (String) iterator.next();
						if (!propertiesInClass.contains(propertyName)) {
							continue;
						}
						if (predicates.containsKey(propertyName)) {
							if (lineNr > 0) {
								out.print(",");
							}
							List<String> objects = predicates.get(propertyName);
							if (!objects.isEmpty()) {
								Collections.sort(objects);
								for (Iterator<String> iterator2 = objects
										.iterator(); iterator2.hasNext();) {
									String object = (String) iterator2.next();
									object = object.replaceAll("\\r?\\n",
											"--linebreak--");
									object = object
											.replaceAll(";", "--semicolon--");
									object = object.replaceAll(",", "--comma--");
									out.print(object);
									if (iterator2.hasNext()) {
										out.print(";");
									}
								}
							}
							lineNr += 1;
						} else {
							out.print(",");
						}
					}
	
					out.println("");
				}
				out.close();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
		} else {
			System.out.println("Class " + className + " has no entities.");
		}
	}
}
