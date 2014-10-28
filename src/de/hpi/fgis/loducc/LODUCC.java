package de.hpi.fgis.loducc;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.hp.hpl.jena.ontology.Individual;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.ontology.OntModelSpec;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.query.Dataset;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.Model;
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Property;
import com.hp.hpl.jena.rdf.model.RDFNode;
import com.hp.hpl.jena.rdf.model.ResIterator;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory;
import com.hp.hpl.jena.rdf.model.Statement;
import com.hp.hpl.jena.rdf.model.StmtIterator;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException;

public class LODUCC {
	private static final String DBPEDIA_ONTOLOGY_NS = "http://dbpedia.org/ontology/";
	private static final String DBPEDIA_RESOURCE_NS = "http://dbpedia.org/resource/";
	private Model dataset = ModelFactory.createDefaultModel();
	private static final Logger logger = Logger.getLogger(LODUCC.class);
	public static final String JENA = "jena";
	public static final String TDB = "tdb";
	private boolean all = false;
	private boolean onlyPerson = false;
	private boolean onlyAthlete = false;
	private String ns = null;
	private String onlyProperty = null;
	private String ontns = null;
	PrintWriter out;
	private boolean jena;
	private List<String> classes = new ArrayList<String>();
	private boolean tdb = false;
	private OntModel ontology;
	
	public LODUCC(String[] files, String mode) throws IllegalArgumentException, IllegalStateException, Exception {
		PropertyConfigurator.configure("log4j.properties");
		logger.setLevel(Level.INFO);

		if (mode.equals(JENA)) {
			logger.info("Jena version");
			this.jena  = true;
		} else if (mode.equals(TDB)) {
			logger.info("TDB version");
			this.tdb  = true;
		}

		long t1 = System.currentTimeMillis();
		
		int loadedDatasets = 0;
		
		for (int i = 0; i < files.length; i++) {
			if (files[i].equals("resume")) {
				readCsv("dbpedia_uniqueness.csv");
			} else if (files[i].equals("owl:Thing")) {
				this.all = true;
			} else if (files[i].equals("onlyAthlete")) {
				this.onlyAthlete = true;
			} else if (files[i].equals("onlyPerson")) {
				this.onlyPerson = true;
			} else if(files[i].startsWith("-ontology=")) {
				String ontologyLocation = files[i].replaceFirst("-ontology=", "");
				Model ontologyModel = FileManager.get().loadModel(ontologyLocation);
				this.ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, ontologyModel);
			} else if(files[i].startsWith("-ontns=")) {
				this.ontns = files[i].replaceFirst("-ontns=", "");
			} else if(files[i].startsWith("-ns=")) {
				this.ns = files[i].replaceFirst("-ns=", "");
			} else if(files[i].startsWith("-property=")) {
				this.onlyProperty = files[i].replaceFirst("-property=", "");
			} else if (files[i].startsWith("-out=")) {
				this.out = new PrintWriter(files[i].replaceFirst("-out=", ""));
			} else {
				if (this.tdb) {
					/*
					if (loadedDatasets == 1) {
						throw new WrongNumberArgsException("Please provide only one TDB folder. ("+files[i]+")");
					}
					*/
					logger.warn("trying to load '" + files[i] + "'");
					String tbdDirectory = files[i];
					Dataset ds = TDBFactory.createDataset(tbdDirectory) ;
					this.dataset = ds.getDefaultModel() ;
					loadedDatasets++;
				} else if (this.jena) {
					loadDump(files[i]);
		            logger.warn("Loaded " + files[i]);
					loadedDatasets++;
				}
			}
		}
		
		if (this.ns == null) {
			this.ns = DBPEDIA_RESOURCE_NS;
		}
		if (this.ontns == null) {
			this.ontns = DBPEDIA_ONTOLOGY_NS;
		}

		if (loadedDatasets > 0) {
			long t2 = System.currentTimeMillis();
			logger.warn("Loading the dataset took " + (t2 - t1) + " milliseconds to execute.");
			
			if (this.onlyAthlete) {
				getUniqueness("Athlete");
			} else if (this.onlyPerson) {
				getUniqueness("Person");
			} else {
				getUniqueness();
			}
		}
		this.dataset.close();
	}
	
	private Boolean getUniqueness(String className) throws FileNotFoundException {
		OntModel ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, dataset);

		long t1 = System.currentTimeMillis();

		if (all) {
			calculcateUniquenessAll();
		} else {
			for (Iterator<OntClass> i = ontology.listClasses(); i.hasNext();) {
				OntClass ontologyClass = i.next();
	            if (!ontologyClass.getURI().equals(ontns+className)) {
	            	continue;
	            }
	
	            if (onlyPerson || onlyAthlete) {
	            	List<String> subjects = selectSubjects(ontologyClass);
	        		logger.warn("Found "+subjects.size() + " entities of type "+ className);
		            calculcateUniqueness(ontologyClass, subjects);
	            }
			}
		}
		

		long t2 = System.currentTimeMillis();
		logger.warn("Getting the uniqueness and density took " + (t2 - t1) + " milliseconds to execute.");
		if (out != null) {
			out.close();
		}
		return true;
	}

	private Boolean getUniqueness() throws FileNotFoundException {
		long t1 = System.currentTimeMillis();
		if (all) {
			calculcateUniquenessAll();
		} else {
			OntModel ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, dataset);

			for (Iterator<OntClass> i = ontology.listClasses(); i.hasNext();) {
				OntClass ontologyClass = i.next();
	            if (!ontologyClass.getURI().toString().startsWith(ontns) && !ontologyClass.getURI().toString().equals("http://www.w3.org/2002/07/owl#Thing")) {
	            	continue;
	            }

	            logger.warn("Fetching entities for ontology class " + ontologyClass.getURI().toString());

	            int entityCount = 0;
	            for (ExtendedIterator<Individual> j = ontology.listIndividuals(ontologyClass); j.hasNext(); ++entityCount ) j.next();
	            if (entityCount > 0) {
		            logger.warn("Found " + entityCount + " entities of type "+ ontologyClass.getURI());
	            	calculcateUniqueness(ontologyClass, entityCount);
	            }
			}
		}
		

		long t2 = System.currentTimeMillis();
		logger.warn("Getting the uniqueness and density took " + (t2 - t1) + " milliseconds to execute.");
		if (out != null) {
			out.close();
		}
		return true;
	}
	
	private void getUniquenessValue(String className, String propertyName, HashMap<Key, Integer> propertyValues, int entityCount) {
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
	    System.out.print(className + "," + propertyName + "," + entityCount + ",");
	    if (out != null) {
	    	out.print(className + "," + propertyName + "," + entityCount + ",");
	    }
	    if (overallCount > 0) {
		    uniqueness = uniqueValues/overallCount;
		    density = overallCount/entityCount;
		    System.out.println(uniqueness + "," + density + "," + Keyness.getKeyness(uniqueness, density));
		    if (out != null) {
		    	out.println(uniqueness + "," + density + "," + Keyness.getKeyness(uniqueness, density));
		    }
	    } else {
	    	System.out.println(",");
		    if (out != null) {
		    	out.println(",");
		    }
	    }
	}

	private void getCLasses() {
		OntModel ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_DL_MEM_RDFS_INF, dataset);
	        
        // TODO: only classes in ontology namespace?
		// setNumClusters(ontology.listClasses().toList().size());
        
        for (Iterator<OntClass> i = ontology.listHierarchyRootClasses(); i.hasNext(); ) {
            OntClass hierarchyRoot = i.next();
            System.out.println(hierarchyRoot);
            for (Iterator<OntClass> j = hierarchyRoot.listSubClasses(); j.hasNext(); ) {
                OntClass hierarchysubClass = j.next();
                System.out.println("  " + hierarchysubClass);
            }
            
        }
	
	}
	
	private void calculcateUniquenessAll() {
		int entityCountThing = 0;
		/*
		ResIterator it = dataset.listSubjects();
		// alternative: listIndividuals on OntModel
		List<Integer> subjects = new ArrayList<Integer>();
		while (it.hasNext()) {
			Resource subject = (Resource) it.next();
			String subjectUri = subject.getURI();
			if (!subjectUri.startsWith(ns)) continue;
			if (subjects.contains(subjectUri.hashCode())) continue;
			subjects.add(subjectUri.hashCode());
			entityCountThing++;
			if (entityCountThing%10000 == 0) {
				logger.info(entityCountThing);
			}
		}
		logger.info("Found "+entityCountThing+" entities of type owl:Thing");
		subjects = null;
		*/
		
		//HashSet<String> subjects = new HashSet<String>();
		String sparql_s = "SELECT DISTINCT ?s " + "WHERE { ";
		if (this.ns.equals(DBPEDIA_RESOURCE_NS)) {
			sparql_s += "?s a <http://www.w3.org/2002/07/owl#Thing>. } ";
		} else {
			sparql_s += "?s a ?t. } ";
		}
		Query qry_s = QueryFactory.create(sparql_s);
		QueryExecution qe_s = QueryExecutionFactory.create(qry_s, dataset);
		ResultSet rs_s = qe_s.execSelect();
		while (rs_s.hasNext()) {
			QuerySolution sol = rs_s.nextSolution();
			RDFNode subject = sol.get("s");
			String subjectUri = subject.toString();
			if (!subjectUri.startsWith(ns)) continue;
			/*
			if (subjects.contains(subjectUri)) continue;
			subjects.add(subjectUri);
			*/
			entityCountThing++;
			/*
			if (entityCountThing%200000 == 0) {
				logger.info(entityCountThing);
			}
			*/
		}
		qe_s.close();
		logger.info("Found "+entityCountThing+" entities of type owl:Thing");
		//subjects = null;
		

		
		String sparql = "SELECT DISTINCT ?p " + "WHERE { "
				+ "?s ?p ?o. } ";
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, dataset);
		ResultSet rs = qe.execSelect();
		while (rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode property = sol.get("p");
			calculateUniquenessPerProperty(entityCountThing, property);
		}
		qe.close();
		
		/*
		//ResIterator objectProperties = dataset.listSubjectsWithProperty(ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ResourceFactory.createResource("http://www.w3.org/2002/07/owl#ObjectProperty"));
		ExtendedIterator<OntProperty> objectProperties = ontology.listAllOntProperties();
		while (objectProperties.hasNext()) {
			RDFNode property = objectProperties.next();
			logger.info(property.toString());
			calculateUniquenessPerProperty(entityCountThing, property);
		}

		ResIterator datatypeProperties = dataset.listSubjectsWithProperty(ResourceFactory.createProperty("http://www.w3.org/1999/02/22-rdf-syntax-ns#type"), ResourceFactory.createResource("http://www.w3.org/2002/07/owl#DatatypeProperty"));
		while (datatypeProperties.hasNext()) {
			RDFNode property = datatypeProperties.next();

			calculateUniquenessPerProperty(entityCountThing, property);

		}
		*/
		//calculcateUniqueness(null, 0);
	}

	private void calculateUniquenessPerProperty(int entityCountThing,
			RDFNode property) {
		//int entitiesWithProperty = 0;
		HashMap<String, HashMap<String, List<String>>> entities = new HashMap<String, HashMap<String, List<String>>>();

		Property currentProperty = ResourceFactory.createProperty(property.toString());
		ResIterator subjects = dataset.listResourcesWithProperty(currentProperty);
		while (subjects.hasNext()) {
			Resource subject = (Resource) subjects.next();
			if (!subject.toString().startsWith(ns)) continue;
			//entitiesWithProperty++;
			
			StmtIterator statements = subject.listProperties(currentProperty);
			while (statements.hasNext()) {
				Statement statement = statements.next();
				RDFNode object = statement.getObject();
				String objectName = "";
				if (object.isLiteral()) {
					objectName= object.toString();
				} else if (object.isURIResource()) {
					objectName = object.asResource().getURI().toString();
				}
				
				if (!entities.containsKey(subject.toString())) {
					HashMap<String, List<String>> predicates = new HashMap<String, List<String>>();
					List<String> objects = new ArrayList<String>();
					objects.add(objectName);
					predicates.put(property.toString(), objects);
					entities.put(subject.toString(), predicates);
				} else {
					HashMap<String, List<String>> predicates = entities.get(subject.toString());
					List<String> objects = new ArrayList<String>();
					if (predicates.containsKey(property.toString())) {
						objects = predicates.get(property.toString());
					}
					objects.add(objectName);
					predicates.put(property.toString(), objects);
					entities.put(subject.toString(), predicates);
				}
				
			}

		}
		
		calculateUniquenessInternal(null, entityCountThing, entityCountThing, entities);
	}
	
	private List<String> selectSubjects(OntClass ontologyClass) {
		List<String> entityURLs = new ArrayList<String>();
		String sparql = "SELECT ?s ?t " + "WHERE { ";
		if (onlyPerson || onlyAthlete) {
			sparql += " ?s a <" + ontologyClass.getURI() + ">; "
					+ " a ?t. } ORDER BY ?s ?t ";
		}
		
		// get subclasses
		List<String> subclasses = new ArrayList<String>();
		for (Iterator<OntClass> i = ontologyClass.listSubClasses(); i.hasNext(); ) {
		  OntClass c = i.next();
		  subclasses.add(c.getURI());
		}
		
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, this.dataset);
		ResultSet rs = qe.execSelect();

		List<String> toDelete = new ArrayList<String>();
		while (rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode subject = sol.get("s");
			RDFNode type = sol.get("t");
			if (!subject.toString().startsWith(this.ns)) {
				continue;
			}
			if (!entityURLs.contains(subject.toString()) && !toDelete.contains(subject.toString())) {
				entityURLs.add(subject.toString());
			}
			if (this.onlyPerson  || this.onlyAthlete) {
				//logger.warn("subclasses of Person: " + StringUtils.join(subclasses,","));
				if (subclasses.contains(type.toString())) {
					if (!toDelete.contains(subject.toString())) {
						toDelete.add(subject.toString());
					}
				}
			}
		}
		qe.close();
		for (Iterator<String> iterator = toDelete.iterator(); iterator.hasNext();) {
			String delete = iterator.next();
			entityURLs.remove(delete.toString());
			
		}
		return entityURLs;
	}
	
	private void calculcateUniqueness(OntClass ontologyClass, List<String> subjects) {
		int entityCountThing = 0;
		HashMap<String, HashMap<String, List<String>>> entities = new HashMap<String, HashMap<String, List<String>>>();
		for (Iterator<String> iterator = subjects.iterator(); iterator.hasNext();) {
			String subject = iterator.next();
			
			String sparql = "SELECT ?p ?o " + "WHERE { "
				+ "<"+subject + "> ?p ?o. } ORDER BY ?p ";
			
			Query qry = QueryFactory.create(sparql);
			QueryExecution qe = QueryExecutionFactory.create(qry, dataset);
			ResultSet rs = qe.execSelect();

			while (rs.hasNext()) {
				QuerySolution sol = rs.nextSolution();
				RDFNode predicate = sol.get("p");
				RDFNode object = sol.get("o");
				if (!entities.containsKey(subject)) {
					HashMap<String, List<String>> predicates = new HashMap<String, List<String>>();
					List<String> objects = new ArrayList<String>();
					objects.add(object.toString());
					predicates.put(predicate.toString(), objects);
					entities.put(subject.toString(), predicates);
				} else {
					HashMap<String, List<String>> predicates = entities.get(subject);
					List<String> objects = new ArrayList<String>();
					if (predicates.containsKey(predicate.toString())) {
						objects = predicates.get(predicate.toString());
					}
					objects.add(object.toString());
					predicates.put(predicate.toString(), objects);
					entities.put(subject.toString(), predicates);
				}
			}
			qe.close();
		}
		calculateUniquenessInternal(ontologyClass, subjects.size(),
				entityCountThing, entities);
	}
	
	private void calculcateUniqueness(OntClass ontologyClass, int entityCount) {
		//get entity count for owl:Thing
		int entityCountThing = 0;
		if (this.all) {
            logger.warn("Starting entity count.");
			String entityCountAllSparql = "SELECT DISTINCT ?s WHERE { "
					+ " ?s a ?t. } ";

			/*
			String entityCountAllSparql = "SELECT (COUNT(DISTINCT ?s) AS ?count) WHERE { ?s ?p ?t. "
					+ "FILTER regex(str(?s), \"^"+ ns + "\")"
					+ "}";
			*/
			Query qryAll = QueryFactory.create(entityCountAllSparql );
			QueryExecution qeAll = QueryExecutionFactory.create(qryAll, dataset);
			ResultSet rsAll = qeAll.execSelect();
			while (rsAll.hasNext()) {
				QuerySolution sol = rsAll.nextSolution();
				RDFNode subject = sol.get("s");
				if (!subject.toString().startsWith(ns)) {
					continue;
				}

				entityCountThing += 1;
			}
/*			while (rsAll.hasNext()) {
				QuerySolution sol = rsAll.nextSolution();
				entityCountThing = sol.getLiteral("count").getInt();
			}
			*/
			qeAll.close();
            logger.warn("Found " + entityCountThing + " entities of type owl:Thing");
		}
		
		// get Uniqueness
		String sparql = "";
		/*
		if (this.all) {
			
		} else (this.onlyProperty != null) {
			
		} else {
			
		}
		*/

		if (onlyProperty != null) {
			sparql = "SELECT ?s ?o WHERE { ";
		} else {
			sparql = "SELECT ?s ?p ?o " + "WHERE { ";
		}
		if (!this.all) {
			sparql += " ?s a <" + ontologyClass.getURI() + ">; "
					+ " ?p ?o. } ORDER BY ?s ?p ";
		} else {
			if (onlyProperty != null) {
				sparql += " ?s <" + onlyProperty + "> ?o. } ORDER BY ?s ";
			} else {
				sparql += " ?s ?p ?o. } ORDER BY ?s ?p ";
			}
		}
		
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, dataset);
		ResultSet rs = qe.execSelect();

		HashMap<String, HashMap<String, List<String>>> entities = new HashMap<String, HashMap<String, List<String>>>();
		while (rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode subject = sol.get("s");
			if (!subject.toString().startsWith(ns)) {
				continue;
			}
			String predicate = null;
			if (onlyProperty != null) {
				predicate = onlyProperty;
			} else {
				RDFNode predicateNode = sol.get("p");
				predicate = predicateNode.toString();
			}
			RDFNode object = sol.get("o");
			if (!entities.containsKey(subject.toString())) {
				HashMap<String, List<String>> predicates = new HashMap<String, List<String>>();
				List<String> objects = new ArrayList<String>();
				objects.add(object.toString());
				predicates.put(predicate, objects);
				entities.put(subject.toString(), predicates);
			} else {
				HashMap<String, List<String>> predicates = entities.get(subject
						.toString());
				List<String> objects = new ArrayList<String>();
				if (predicates.containsKey(predicate)) {
					objects = predicates.get(predicate);
				}
				objects.add(object.toString());
				predicates.put(predicate, objects);
				entities.put(subject.toString(), predicates);
			}
		}
		qe.close();
		logger.warn("Done building property map");

		calculateUniquenessInternal(ontologyClass, entityCount, entityCountThing, entities);
	}

	private void calculateUniquenessInternal(OntClass ontologyClass,
			int entityCount, int entityCountThing,
			HashMap<String, HashMap<String, List<String>>> entities) {
		HashMap<String, List<List<String>>> propertyValueMap = new HashMap<String, List<List<String>>>();

		Iterator<Entry<String, HashMap<String, List<String>>>> it = entities
				.entrySet().iterator();
		while (it.hasNext()) {
			Entry<String, HashMap<String, List<String>>> pairs = it.next();
			pairs.getKey();
			HashMap<String, List<String>> predicates = pairs
					.getValue();
			Iterator<Entry<String, List<String>>> predIt = predicates
					.entrySet().iterator();
			while (predIt.hasNext()) {
				Entry<String, List<String>> predPairs = predIt.next();
				String predicate = predPairs.getKey();
				List<String> objects = predPairs.getValue();
				if (!objects.isEmpty()) {
					List<List<String>> objectLists = new ArrayList<List<String>>();
					if (propertyValueMap.containsKey(predicate)) {
						objectLists = propertyValueMap.get(predicate);
					}
					objectLists.add(objects);
					propertyValueMap.put(predicate, objectLists);
				}
			}
		}

		//logger.warn("Done building property value sets");

		Iterator<Entry<String, List<List<String>>>> propertyIt = propertyValueMap
				.entrySet().iterator();
		while (propertyIt.hasNext()) {
			Entry<String, List<List<String>>> pairs = propertyIt.next();
			String property = pairs.getKey();
			List<List<String>> objectLists = pairs
					.getValue();

			HashMap<Key, Integer> propertyValues = new HashMap<Key, Integer>();
			for (Iterator<List<String>> iterator = objectLists.iterator(); iterator.hasNext();) {
				List<String> objects = iterator.next();
				Key keys = new Key(objects);
				if (!propertyValues.containsKey(keys)) {
					propertyValues.put(keys, 1);
				} else {
					propertyValues.put(keys, propertyValues.get(keys) + 1);
				}
			}
			if (all) {
				getUniquenessValue("http://www.w3.org/2002/07/owl#Thing",
						property, propertyValues, entityCountThing);
			} else {
				getUniquenessValue(ontologyClass.getURI(), property,
						propertyValues, entityCount);
			}
		}
	}
	
	private void getProperties() {
		String sparql = "SELECT DISTINCT ?p " +
				"WHERE { " +
				"?s ?p ?o. }";
		//+ 
		//		"FILTER (regex(?p, 'http://dbpedia.org')). }";
		
		List<String> subjects = new ArrayList<String>();
		
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, dataset);
		ResultSet rs = qe.execSelect();
		
		while(rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode str = sol.get("p"); 
			System.out.println(str);
			subjects.add(str.toString());
		}
		
		qe.close(); 
	}
	
	private void loadDump(String file) throws Exception {
		try {
			Model dataset1 = FileManager.get().loadModel(file);
			dataset.add(dataset1);
		} catch (Exception e) {
			logger.error("Error loading file: "+file);
			e.printStackTrace(System.out);
			System.exit(0);
		}
	}
	
	public HashMap<String, String> readCsv(String csvFileToRead) {
		HashMap<String, String> csv = new HashMap<String, String>();
		BufferedReader br = null;
		String line = "";
		String splitBy = ",";

		try {
			br = new BufferedReader(new FileReader(csvFileToRead));
			while ((line = br.readLine()) != null) {
				String[] csvLine = line.split(splitBy);
				if (csvLine.length == 3) {
					csv.put(csvLine[0], csvLine[1]);
				}
			}
		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			if (br != null) {
				try {
					br.close();
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		return csv;
	}

	public static void main(String[] args) {
		try {
			new LODUCC(args, "jena");
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IllegalStateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}
