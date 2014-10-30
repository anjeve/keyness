package de.hpi.fgis.loducc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class Key {

    private final List<String> list;

    public Key(List<String> list) {
        this.list = list;
    }

    public List<String> getList() {
    	return this.list;
    }

    @Override
    public int hashCode() {
    	int hashCode = 0;
    	int counter = 0;
    	for (Iterator<String> iterator = list.iterator(); iterator.hasNext();) {
			String type = (String) iterator.next();
			if (counter > 0) {
				hashCode = type.hashCode() ^ hashCode;
			} else {
				hashCode = type.hashCode();
			}
			counter += 1;
		}
        return hashCode;
    }

    @Override
    public boolean equals(Object obj) {
    	List<String> compareList = ((Key) obj).getList();
    	Collections.sort(compareList);
    	Collections.sort(this.list);
        return (obj instanceof Key) && compareList.equals(this.list);
    }
}