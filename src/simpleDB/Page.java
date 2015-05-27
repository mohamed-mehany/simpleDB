package simpleDB;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

@SuppressWarnings("serial")
public class Page implements Serializable {
	private static int maxSize;
	private Tuple[] tuples;
	private int size;
	private int pageID;

	public Page(int pageID) {
		tuples = new Tuple[maxSize];
		size = 0;
		this.pageID = pageID;
	}

	public void insert(Hashtable<String, String> htblColNameValue) {
		Tuple t = new Tuple(htblColNameValue);
		tuples[size] = t;
		t.setPageID(pageID);
		t.setRowID(size);
		size++;

	}

	public void delete(int row) {
		//System.out.println(row + "EEH");
		tuples[row] = null;
	}

	public HashSet<Tuple> findLinear(
			Hashtable<String, String> htblColNameValue, String strOperator,
			HashMap<String, Integer> colNameID, String[] colNames,
			HashMap<String, Boolean> visited) {
		HashSet<Tuple> res = new HashSet<>();
		if (strOperator.equals("or")) {
			for (int i = 0; i < size; ++i) {
				Tuple tup = tuples[i];
				Iterator<Entry<String, String>> colNameValue = htblColNameValue
						.entrySet().iterator();
				while (colNameValue.hasNext()) {
					Entry<String, String> col = colNameValue.next();
					String colName = col.getKey();
					String colVal = col.getValue();
					if (visited.get(colName))
						continue;
					if (tup != null && colVal.equals(tup.getVal()[colNameID.get(colName)])) {
						res.add(tup);
						break;
					}
				}

			}
		} else if (strOperator.equals("and")) {
			//System.out.println();
			for (int i = 0; i < size; ++i) {
				Tuple tup = tuples[i];
				Iterator<Entry<String, String>> colNameValue = htblColNameValue
						.entrySet().iterator();
				boolean f = true;
				while (colNameValue.hasNext()) {
					Entry<String, String> col = colNameValue.next();
					String colName = col.getKey();
					String colVal = col.getValue();
					if (visited.get(colName))
						continue;
					if (tup != null && !colVal.equals(tup.getVal()[colNameID.get(colName)])) {
						f = false;
					}
				}
				if (f){
					//System.out.println("Hi123" + tup);
					res.add(tup);
				}
			}
		}
		return res;
	}

	public int getpageID() {
		return pageID;
	}

	public static void setMaxSize(File f) throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(f));
		maxSize = Integer.parseInt(br.readLine().substring(25));
		br.close();
	}

	public void setpageID(int pageID) {
		this.pageID = pageID;
	}

	public boolean checkFullPage() {
		return size >= maxSize;
	}

	public Tuple getTuple(int row) {
		return tuples[row];
	}

	public int getSize() {
		return size;
	}

	public String toString() {
		String res = "";
		for (int i = 0; i < size; i++) {
			res += tuples[i].toString() + '\n';
		}
		return res;
	}

}
