package simpleDB;

import java.io.Serializable;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class Tuple implements Serializable {

	private int pageID;
	private int rowID;
	private String[] val;

	public Tuple(Hashtable<String, String> htblColNameValue) {
		int cols = htblColNameValue.size();
		val = new String[cols];
		Iterator<Entry<String, String>> colNameValue = htblColNameValue
				.entrySet().iterator();
		for (int i = 0; colNameValue.hasNext(); ++i) {
			Entry<String, String> col = colNameValue.next();
			val[i] = col.getValue();
		}

	}

	public int getRowID() {
		return rowID;
	}

	public void setRowID(int rowID) {
		this.rowID = rowID;
	}

	public int getPageID() {
		return pageID;
	}

	public void setPageID(int pageID) {
		this.pageID = pageID;
	}

	public String[] getVal() {
		return val;
	}

	public String toString() {
		String res = "";
		for (int i = 0; i < val.length; i++) {
			res += i != val.length - 1 ? val[i] + ", " : val[i];
		}
		return res;
	}
}
