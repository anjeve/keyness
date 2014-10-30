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

import org.apache.commons.lang3.StringUtils;
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
import com.hp.hpl.jena.sdb.assembler.MissingException;
import com.hp.hpl.jena.tdb.TDBFactory;
import com.hp.hpl.jena.util.FileManager;
import com.hp.hpl.jena.util.iterator.ExtendedIterator;
import com.sun.org.apache.xpath.internal.functions.WrongNumberArgsException;

import de.hpi.fgis.loducc.statistics.Keyness;
import de.hpi.fgis.loducc.statistics.Statistics;

public class LODUCC {
	private static final String DBPEDIA_ONTOLOGY_NS = "http://dbpedia.org/ontology/";
	private static final String DBPEDIA_RESOURCE_NS = "http://dbpedia.org/resource/";
	private Model dataset = ModelFactory.createDefaultModel();
	private static final Logger logger = Logger.getLogger(LODUCC.class);
	public static final String JENA = "jena";
	public static final String TDB = "tdb";
	private boolean all = false;
	private String ns = null;
	private String onlyProperty = null;
	private String ontns = null;
	private PrintWriter out;
	private boolean jena;
	private List<String> classes = new ArrayList<String>();
	private boolean tdb = false;
	private OntModel ontology;
	private String only;
	
	/**
	 * @param files
	 * @param mode
	 * @throws IllegalArgumentException
	 * @throws IllegalStateException
	 * @throws Exception
	 */
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
			if (files[i].equals("debug")) {
				logger.setLevel(Level.DEBUG);
			} else  if (files[i].equals("resume")) {
				readCsv("dbpedia_uniqueness.csv");
			} else if (files[i].equals("owl:Thing")) {
				this.all = true;
			} else if (files[i].startsWith("-only=")) {
				this.only = files[i].replaceFirst("-only=", "");
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
					if (loadedDatasets == 1) {
						throw new IllegalArgumentException("Please provide only one TDB folder ("+files[i]+").");
					}
					String tbdDirectory = files[i];
					Dataset ds = TDBFactory.createDataset(tbdDirectory) ;
					this.dataset = ds.getDefaultModel() ;
				} else if (this.jena) {
					loadDump(files[i]);
				}
	            logger.info("Dataset loaded: " + files[i]);
				loadedDatasets++;
			}
		}
		
		if (this.ns == null) {
			this.ns = DBPEDIA_RESOURCE_NS;
		}
		if (this.ontns == null) {
			this.ontns = DBPEDIA_ONTOLOGY_NS;
		}

		if (this.ontology == null) {
			this.ontology = ModelFactory.createOntologyModel(OntModelSpec.OWL_MEM, this.dataset);
		}
		
		if (loadedDatasets > 0) {
			long t2 = System.currentTimeMillis();
			logger.info("Loading the dataset took " + (t2 - t1) + " ms.");
			
			long t3 = System.currentTimeMillis();
			if (this.only != null) {
				if (this.ontology == null) throw new IllegalArgumentException("Missing ontology.");
				logger.info("Analyzing only entities of type " + this.ontns + this.only);
				getUniqueness(this.ontns+this.only);
			} else {
				getUniqueness();
			}
			long t4 = System.currentTimeMillis();
			logger.info("Getting the uniqueness, density, and keyness took " + (t4 - t3) + " ms.");

		}
		this.dataset.close();
		if (this.out != null) {
			this.out.close();
		}
	}
	
	/**
	 * @return
	 * @throws FileNotFoundException
	 */
	private void getUniqueness() throws FileNotFoundException {
		if (this.all) {
			calculcateUniquenessAll();
		} else {
			for (Iterator<OntClass> i = this.ontology.listClasses(); i.hasNext();) {
				OntClass ontologyClass = i.next();
				String classUri = ontologyClass.getURI();
				if (!classUri.startsWith(this.ontns)) continue;
				getUniqueness(classUri.toString());
				
				/*
	            if (!ontologyClass.getURI().toString().startsWith(this.ontns) && !ontologyClass.getURI().toString().equals("http://www.w3.org/2002/07/owl#Thing")) continue;

	            logger.info("Fetching entities for ontology class " + ontologyClass.getURI().toString());

	            int entityCount = 0;
	            for (ExtendedIterator<Individual> j = ontology.listIndividuals(ontologyClass); j.hasNext(); ++entityCount ) j.next();
	            if (entityCount > 0) {
		            logger.info("Found " + entityCount + " entities of type "+ ontologyClass.getURI());
		            calculcateUniqueness(ontologyClass, entityCount);
	            	
	            }
	            */
			}
		}
	}
	
	/**
	 * 
	 * @param classUri
	 * @throws FileNotFoundException
	 */
	private void getUniqueness(String classUri) throws FileNotFoundException {
		if (all) {
			calculcateUniquenessAll();
		} else {
			Resource ontologyClass = dataset.getResource(classUri);
			OntClass ontologyClassObject = this.ontology.getOntClass(ontologyClass.getURI());
			List<String> subjects = getEntityUris(ontologyClassObject);
            calculcateUniqueness(ontologyClassObject, subjects);
        } 
	}
	


	/**
	 * 
	 */
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
			if (!subjectUri.startsWith(this.ns)) continue;
			if (subjectUri.startsWith(this.ontns)) continue;
			/*
			if (subjects.contains(subjectUri)) continue;
			subjects.add(subjectUri);
			*/
			entityCountThing++;
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

	/**
	 * @param entityCountThing
	 * @param property
	 */
	private void calculateUniquenessPerProperty(int entityCountThing,
			RDFNode property) {
		HashMap<String, HashMap<String, List<String>>> entities = new HashMap<String, HashMap<String, List<String>>>();
		
		Property currentProperty = property.as(Property.class);
		ResIterator subjects = dataset.listResourcesWithProperty(currentProperty);
		while (subjects.hasNext()) {
			Resource subject = (Resource) subjects.next();
			if (!subject.toString().startsWith(ns)) continue;
			
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
	
	/**
	 * @param ontologyClass
	 * @return
	 */
	private List<String> getEntityUris(OntClass ontologyClass) {
		String ontologyClassUri = ontologyClass.getURI();
		List<String> entityURLs = new ArrayList<String>();
		String sparql = "SELECT ?s ?t " + "WHERE { ";
		// if DBpedia, get subclasses already from SPARQL
		if (this.ns.equals(DBPEDIA_RESOURCE_NS)) {
			sparql += " ?s a <" + ontologyClassUri + ">; ";
			sparql += " a ?t. } ";
		} else {
			sparql += " ?s a <" + ontologyClassUri + ">. } ";
		}
		// get subclasses
		List<String> subclasses = new ArrayList<String>();
		ExtendedIterator<OntClass> subclassIterator;
		if (this.only != null) {
			subclassIterator = ontologyClass.listSubClasses(true);
		} else {
			subclassIterator = ontologyClass.listSubClasses();
		}
		while (subclassIterator.hasNext()) {
		  OntClass subclass = subclassIterator.next();
		  if (!subclass.toString().startsWith(this.ontns)) continue;
		  subclasses.add(subclass.getURI());
		}
		if (subclasses.size() > 0) {
			logger.debug("  Subclasses: " + StringUtils.join(subclasses,","));
		}
		
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, this.dataset);
		ResultSet rs = qe.execSelect();

		List<String> toDelete = new ArrayList<String>();
		while (rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode subject = sol.get("s");
			RDFNode type = sol.get("t");
			if (!subject.toString().startsWith(this.ns)) continue;
			if (!entityURLs.contains(subject.toString()) && !toDelete.contains(subject.toString())) {
				entityURLs.add(subject.toString());
			}
			if (this.only != null) {
				if (subclasses.contains(type.toString())) {
					if (!toDelete.contains(subject.toString())) {
						toDelete.add(subject.toString());
					}
				}
			}
		}
		qe.close();
		
		// if -only parameter given, exclude entities of subclasses
		if (this.only != null) {
			if (!this.ns.equals(DBPEDIA_RESOURCE_NS)) {
				for (Iterator<String> iterator = subclasses.iterator(); iterator.hasNext();) {
					String subclass = (String) iterator.next();
					logger.debug("Excluding entities of type " + subclass);
					
					String sparql_subclass = "SELECT ?s ?t " + "WHERE { "
							+ " ?s a <" + subclass + ">. }";
					Query qry_subclass = QueryFactory.create(sparql_subclass);
					QueryExecution qe_subclass = QueryExecutionFactory.create(qry_subclass, this.dataset);
					ResultSet rs_subclass = qe_subclass.execSelect();
					while (rs_subclass.hasNext()) {
						QuerySolution sol_subclass = rs_subclass.nextSolution();
						RDFNode subject_subclass = sol_subclass.get("s");
						if (!subject_subclass.toString().startsWith(this.ns)) continue;
						if (!toDelete.contains(subject_subclass.toString())) {
							toDelete.add(subject_subclass.toString());
						}
	
					}
					qe_subclass.close();
				}
			}
			
		} else {	// add entities of subclasses (if not DBpedia and all classes given already)
			if (entityURLs.size() > 0) {
				logger.info("Found " + entityURLs.size() + " entities of type " + ontologyClassUri);
			}
			if (!this.ns.equals(DBPEDIA_RESOURCE_NS)) {
				for (Iterator<String> iterator = subclasses.iterator(); iterator.hasNext();) {
					String subclass = (String) iterator.next();
					
					String sparql_subclass = "SELECT ?s " + "WHERE { "
							+ " ?s a <" + subclass + ">. }";
					Query qry_subclass = QueryFactory.create(sparql_subclass);
					QueryExecution qe_subclass = QueryExecutionFactory.create(qry_subclass, this.dataset);
					ResultSet rs_subclass = qe_subclass.execSelect();
					int subclassEntityCount = 0;
					while (rs_subclass.hasNext()) {
						QuerySolution sol_subclass = rs_subclass.nextSolution();
						RDFNode subject_subclass = sol_subclass.get("s");
						if (!subject_subclass.toString().startsWith(this.ns)) continue;
						if (!entityURLs.contains(subject_subclass.toString()) && !toDelete.contains(subject_subclass.toString())) {
							entityURLs.add(subject_subclass.toString());
							subclassEntityCount++;
						}
					}
					qe_subclass.close();
					if (subclassEntityCount > 0) {
						logger.debug("  Added " + subclassEntityCount + " entities of type " + subclass + " to " + ontologyClassUri);
					}
				}
			}
		}

		for (Iterator<String> iterator = toDelete.iterator(); iterator.hasNext();) {
			String delete = iterator.next();
			entityURLs.remove(delete.toString());
			
		}
		return entityURLs;
	}
	
	/**
	 * @param ontologyClass
	 * @param subjects
	 */
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

	/**
	 * @param ontologyClass
	 * @param entityCount
	 * @param entityCountThing
	 * @param entities
	 */
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
			Statistics stats = new Statistics();
			if (this.all) {
				stats.getUniquenessDensityKeyness("http://www.w3.org/2002/07/owl#Thing",
						property, propertyValues, entityCountThing, this.out);
			} else {
				stats.getUniquenessDensityKeyness(ontologyClass.getURI(), property,
						propertyValues, entityCount, this.out);
			}
		}
	}
	

	/**
	 * @param file
	 * @throws Exception
	 */
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
	
	/**
	 * @param csvFileToRead
	 * @return
	 */
	private HashMap<String, String> readCsv(String csvFileToRead) {
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
			e.printStackTrace();
		} catch (IllegalStateException e) {
			e.printStackTrace();
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
