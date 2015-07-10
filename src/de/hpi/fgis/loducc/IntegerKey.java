package de.hpi.fgis.loducc;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public final class IntegerKey {

    private final List<Integer> list;

    public IntegerKey(List<Integer> list) {
        this.list = list;
    }

    public List<Integer> getList() {
    	return this.list;
    }

    @Override
    public int hashCode() {
    	int hashCode = 0;
    	int counter = 0;
    	for (Iterator<Integer> iterator = list.iterator(); iterator.hasNext();) {
			Integer type = (Integer) iterator.next();
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
    	List<Integer> compareList = ((IntegerKey) obj).getList();
    	Collections.sort(compareList);
    	Collections.sort(this.list);
        return (obj instanceof IntegerKey) && compareList.equals(this.list);
    }
}