package de.hpi.fgis.loducc.dataset;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.log4j.Logger;
import com.hp.hpl.jena.ontology.OntClass;
import com.hp.hpl.jena.ontology.OntModel;
import com.hp.hpl.jena.query.Query;
import com.hp.hpl.jena.query.QueryExecution;
import com.hp.hpl.jena.query.QueryExecutionFactory;
import com.hp.hpl.jena.query.QueryFactory;
import com.hp.hpl.jena.query.QuerySolution;
import com.hp.hpl.jena.query.ResultSet;
import com.hp.hpl.jena.rdf.model.RDFNode;

import de.hpi.fgis.loducc.LODUCC;

public class Dataset {
	private static final Logger logger = Logger.getLogger(LODUCC.class);
	
	/**
	 * 
	 */
	private void getClasses(OntModel ontology) {
        for (Iterator<OntClass> i = ontology.listHierarchyRootClasses(); i.hasNext(); ) {
            OntClass hierarchyRoot = i.next();
            logger.debug(hierarchyRoot);
            for (Iterator<OntClass> j = hierarchyRoot.listSubClasses(); j.hasNext(); ) {
                OntClass hierarchysubClass = j.next();
                logger.debug("  " + hierarchysubClass);
            }
            
        }
	
	}
	
	/**
	 * 
	 */
	private void getProperties(OntModel ontology) {
		String sparql = "SELECT DISTINCT ?p " +
				"WHERE { " +
				"?s ?p ?o. }";

		List<String> subjects = new ArrayList<String>();
		
		Query qry = QueryFactory.create(sparql);
		QueryExecution qe = QueryExecutionFactory.create(qry, ontology);
		ResultSet rs = qe.execSelect();
		
		while(rs.hasNext()) {
			QuerySolution sol = rs.nextSolution();
			RDFNode str = sol.get("p"); 
			logger.debug(str);
			subjects.add(str.toString());
		}
		
		qe.close(); 
	}
}
