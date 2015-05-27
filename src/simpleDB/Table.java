package simpleDB;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map.Entry;

import simpleDB.Exceptions.DBAppException;

@SuppressWarnings("serial")
public class Table implements Serializable {
	private ArrayList<String> singleIndexes;
	private String[] colNames;
	private HashMap<String, Integer> colNameID;
	private int nPages;
	private String name;
	private String prefix;
	private int nKDTrees;
	private HashMap<Integer, ArrayList<String>> kdIndexCol;
	private HashMap<ArrayList<String>, Integer> kdColIndex;

	public Table(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException, IOException {
		name = strTableName;
		nPages = 0;
		nKDTrees = 0;
		prefix = DBApp.dataDir + "/" + name + "/";
		singleIndexes = new ArrayList<>();
		kdIndexCol = new HashMap<>();
		kdColIndex = new HashMap<>();
		colNames = new String[htblColNameType.size()];
		colNameID = new HashMap<>();
		createTable(strTableName, htblColNameType, htblColNameRefs,
				strKeyColName);
		createPage();
	}

	public void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException, IOException {
		FileWriter meta = new FileWriter(DBApp.dataDir + "/metadata.csv", true);
		Iterator<Entry<String, String>> colNameType = htblColNameType
				.entrySet().iterator();
		String line;
		for (int i = 0; colNameType.hasNext(); ++i) {
			Entry<String, String> col = colNameType.next();
			boolean indexed = col.getKey().equals(strKeyColName);
			line = strTableName + ", " + col.getKey() + ", " + col.getValue()
					+ ", " + indexed + ", " + false + ", "
					+ htblColNameRefs.get(col.getKey()) + "\n";
			meta.write(line);
			colNames[i] = col.getKey();
			colNameID.put(col.getKey(), i);
		}
		meta.close();
	}

	public void createIndex(String strTableName, String strColName)
			throws IOException {
		BufferedReader br = new BufferedReader(new FileReader(DBApp.dataDir
				+ "/metadata.csv"));
		String line;
		String input = "";
		while ((line = br.readLine()) != null) {
			String[] tmp = line.split(", ");
			if (tmp[0].equals(strTableName) && tmp[1].equals(strColName)) {
				String newLine = "";
				for (int i = 0; i < tmp.length; ++i) {
					if (i == 4)
						tmp[i] = "true";
					newLine += (i == tmp.length - 1) ? tmp[i] : tmp[i] + ", ";
				}
				input += newLine + "\n";
			} else
				input += line + "\n";
		}
		br.close();
		FileOutputStream fileOut = new FileOutputStream(DBApp.dataDir
				+ "/metadata.csv");
		fileOut.write(input.getBytes());
		fileOut.close();
		createHashTable(strColName);
		singleIndexes.add(strColName);
	}

	public void insertTupleToTable(Hashtable<String, String> htblColNameValue)
			throws IOException, ClassNotFoundException {
		Page p = getLastPage();
		if (p.checkFullPage()) {
			Page pNew = createPage();
			pNew.insert(htblColNameValue);
			updateIndex(htblColNameValue, pNew);
			updateMultiIndex(htblColNameValue, pNew);
			savePage(pNew);
		} else {
			p.insert(htblColNameValue);
			updateIndex(htblColNameValue, p);
			updateMultiIndex(htblColNameValue, p);
			savePage(p);
		}
	}

	public void createMultiDimIndex(ArrayList<String> htblColNames)
			throws DBAppException, IOException {
		Collections.sort(htblColNames);
		BufferedReader br = new BufferedReader(new FileReader(DBApp.dataDir
				+ "/metadata.csv"));
		String line;
		String input = "";
		while ((line = br.readLine()) != null) {
			String[] tmp = line.split(", ");
			if (tmp[0].equals(name) && htblColNames.contains(tmp[1])) {
				String newLine = "";
				for (int i = 0; i < tmp.length; ++i) {
					if (i == 4)
						tmp[i] = "true";
					newLine += (i == tmp.length - 1) ? tmp[i] : tmp[i] + ", ";
				}
				input += newLine + "\n";
			} else
				input += line + "\n";
		}
		br.close();
		FileOutputStream fileOut = new FileOutputStream(DBApp.dataDir
				+ "/metadata.csv");
		fileOut.write(input.getBytes());
		fileOut.close();
		//System.out.println("ANAJJJ" + htblColNames.size());
		kdColIndex.put(htblColNames, nKDTrees);
		//System.out.println("asdsa" + kdColIndex.size());

		kdIndexCol.put(nKDTrees, htblColNames);
		createKDTree(htblColNames);
		nKDTrees++;

	}

	public void updateIndex(Hashtable<String, String> htblColNameValue, Page p)
			throws ClassNotFoundException, IOException {
		Iterator<Entry<String, String>> colNameValue = htblColNameValue
				.entrySet().iterator();
		while (colNameValue.hasNext()) {
			Entry<String, String> col = colNameValue.next();
			String colName = col.getKey();
			String pageIDRow = p.getpageID() + "_" + (p.getSize() - 1);
			if (singleIndexes.contains(colName)) {
				ExtendibleHashtable<String, String> exh = getHashTable(colName);
				exh.put(col.getValue(), pageIDRow);
				saveHashTable(exh, colName);
			}
		}

	}

	public void updateMultiIndex(Hashtable<String, String> htblColNameValue,
			Page p) throws ClassNotFoundException, IOException {
		Iterator<Entry<Integer, ArrayList<String>>> it = kdIndexCol.entrySet()
				.iterator();
		
		//System.out.println(kdIndexCol.size()+"HI@2");

		while (it.hasNext()) {
			//System.out.println("HI@");
			ArrayList<String> toBeHashed = new ArrayList<String>();
			Entry<Integer, ArrayList<String>> pair = it.next();
			int kdNumber = pair.getKey();
			ArrayList<String> v = pair.getValue();
			KD kd = getKDTree(kdNumber);
			Iterator<Entry<String, String>> colNameValue = htblColNameValue
					.entrySet().iterator();
			String pageIDRow = p.getpageID() + "_" + (p.getSize() - 1);
			while (colNameValue.hasNext()) {
				Entry<String, String> col = colNameValue.next();
				String colName = col.getKey();
				if (v.contains(colName))
					toBeHashed.add(col.getValue());
			}
			double hash[] = hashCols(toBeHashed);

			kd.insert(hash, pageIDRow);
			saveKDTree(kd, kdNumber);
		}

	}

	public Page createPage() throws IOException {
		Page p = new Page(nPages);
		savePage(p);
		++nPages;
		return p;
	}

	public Page getLastPage() throws IOException, ClassNotFoundException {
		String pageName = name + "_" + (nPages - 1) + ".ser";
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(prefix
				+ pageName));
		Page p = (Page) in.readObject();
		in.close();
		return p;
	}

	public Page getPage(int pageID) throws IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(prefix
				+ name + "_" + pageID + ".ser"));
		Page p = (Page) in.readObject();
		in.close();
		return p;
	}

	public void savePage(Page p) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				prefix + name + "_" + p.getpageID() + ".ser"));
		out.writeObject(p);
		out.close();
	}

	public void deleteTupleFromTable(Tuple t) throws ClassNotFoundException,
			IOException {
		Page p = getPage(t.getPageID());
		p.delete(t.getRowID());
		deleteFromIndexes(t);
		deleteFromMultiIndexes(t);
		savePage(p);
	}

	public void deleteFromIndexes(Tuple t) throws ClassNotFoundException,
			IOException {
		for (String indexedColName : singleIndexes) {
			ExtendibleHashtable<String, String> exh = getHashTable(indexedColName);
			int colID = colNameID.get(indexedColName);
			String v = t.getVal()[colID];
			exh.put(v, null);
			saveHashTable(exh, indexedColName);
		}

	}

	public void deleteFromMultiIndexes(Tuple t) throws ClassNotFoundException,
			IOException {
		Hashtable<String, String> htblColNameValue = tupleToTable(t);
		Page p = getPage(t.getPageID());
		Iterator<Entry<Integer, ArrayList<String>>> it = kdIndexCol.entrySet()
				.iterator();
		while (it.hasNext()) {
			ArrayList<String> toBeHashed = new ArrayList<String>();
			Entry<Integer, ArrayList<String>> pair = it.next();
			int kdNumber = pair.getKey();
			ArrayList<String> v = pair.getValue();
			KD kd = getKDTree(kdNumber);
			Iterator<Entry<String, String>> colNameValue = htblColNameValue
					.entrySet().iterator();
			String pageIDRow = p.getpageID() + "_" + (p.getSize() - 1);
			while (colNameValue.hasNext()) {
				Entry<String, String> col = colNameValue.next();
				String colName = col.getKey();
				if (v.contains(colName))
					toBeHashed.add(col.getValue());
			}
			double hash[] = hashCols(toBeHashed);
			kd.insert(hash, pageIDRow);
			saveKDTree(kd, kdNumber);
		}

	}
	public Hashtable<String, String> tupleToTable(Tuple t) throws ClassNotFoundException, IOException{
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		String val[] = t.getVal();
		for(int i = 0; i < val.length; ++i)
			htblColNameValue.put(colNames[i], val[i]);
		return htblColNameValue;
	}
	public ExtendibleHashtable<String, String> getHashTable(String colName)
			throws IOException, ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(prefix
				+ name + "_" + colName + ".ser"));
		@SuppressWarnings("unchecked")
		ExtendibleHashtable<String, String> exh = (ExtendibleHashtable<String, String>) in
				.readObject();
		in.close();
		return exh;
	}

	public ExtendibleHashtable<String, String> createHashTable(String colName)
			throws IOException {
		ExtendibleHashtable<String, String> exh = new ExtendibleHashtable<>();
		String hashTableName = name + "_" + colName + ".ser";
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				prefix + hashTableName));
		out.writeObject(exh);
		out.close();
		return exh;
	}

	public KD createKDTree(ArrayList<String> colNames) throws IOException {
		KD kd = new KD(colNames.size());
		String kdName = name + "_kd_" + nKDTrees + ".ser";
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				prefix + kdName));
		out.writeObject(kd);
		out.close();
		return kd;
	}

	public void saveHashTable(ExtendibleHashtable<String, String> exh,
			String colName) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				prefix + name + "_" + colName + ".ser"));
		out.writeObject(exh);
		out.close();
	}

	public void saveKDTree(KD kd, int kdNumber) throws IOException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				prefix + name + "_kd_" + kdNumber + ".ser"));
		out.writeObject(kd);
		out.close();
	}

	public KD getKDTree(int kdNumber) throws IOException,
			ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(prefix
				+ name + "_kd_" + kdNumber + ".ser"));
		KD kd = (KD) in.readObject();
		in.close();
		return kd;

	}

	public Iterator<Tuple> findInTable(
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws ClassNotFoundException, IOException {
		// Init visited array
		HashMap<String, Boolean> visited = new HashMap<>();
		Iterator<Entry<String, String>> colNameValue = htblColNameValue
				.entrySet().iterator();
		ArrayList<String> checkCols = new ArrayList<String>();
		while (colNameValue.hasNext()) {
			Entry<String, String> col = colNameValue.next();
			checkCols.add(col.getKey());
			visited.put(col.getKey(), false);
		}
		Collections.sort(checkCols);
		boolean multi = false;
		int kdIndex = -1;
		if(kdColIndex.containsKey(checkCols)){
			multi = true;
			kdIndex = kdColIndex.get(checkCols);
		}
		// Init result
		HashSet<Tuple> res = new HashSet<>();
		// Check Singleindexed cols
		ArrayList<String> indexedColNames = checkSingle(htblColNameValue);
		// System.out.println("hi");
		if(multi){
			//System.out.println("h2i");
			Tuple tup = selectMultiIndex(htblColNameValue, kdIndex);		
			res.add(tup);
			
		}
		else if (indexedColNames.size() > 0) {
			// System.out.println("oops");
			for (String colName : indexedColNames) {
				visited.put(colName, true);
				Tuple tup = selectSingleIndex(htblColNameValue, colName);
				if (tup != null)
					res.add(tup);
				else if (tup == null && strOperator.equals("and"))
					return null;
			}
		} else {
			// Search linearly for the rest
			Iterator<Tuple> resCols = linearSearch(htblColNameValue,
					strOperator, visited);
			// System.out.println(resCols.hasNext());
			while (resCols.hasNext()) {
				Tuple t = resCols.next();
				// System.out.println("NO"+t);
				if (t != null)
					res.add(t);
				else if (t == null && strOperator.equals("and"))
					return null;
			}

		}

		return res.iterator();
	}

	public Iterator<Tuple> linearSearch(
			Hashtable<String, String> htblColNameValue, String strOperator,
			HashMap<String, Boolean> visited) throws ClassNotFoundException,
			IOException {
		HashSet<Tuple> res = new HashSet<Tuple>();
		for (int i = 0; i < nPages; ++i) {
			Page p = getPage(i);
			// System.out.println("Page" + i);
			res.addAll(p.findLinear(htblColNameValue, strOperator, colNameID,
					colNames, visited));
		}
		return res.iterator();
	}

	public Tuple selectSingleIndex(Hashtable<String, String> htblColNameValue,
			String indexedColName) throws ClassNotFoundException, IOException {
		ExtendibleHashtable<String, String> exh = getHashTable(indexedColName);
		String k = htblColNameValue.get(indexedColName);
		String pageIDRow = exh.get(k);
		if (pageIDRow == null)
			return null;
		String pageArr[] = pageIDRow.split("_");
		Page p = getPage(Integer.parseInt(pageArr[0]));
		return p.getTuple(Integer.parseInt(pageArr[1]));
	}

	public Tuple selectMultiIndex(Hashtable<String, String> htblColNameValue,
			int indexedColsID) throws ClassNotFoundException, IOException {
		KD kd = getKDTree(indexedColsID);
		ArrayList<String> indexedCols = kdIndexCol.get(indexedColsID);
		Iterator<Entry<String, String>> colNameValue = htblColNameValue
				.entrySet().iterator();
		ArrayList<String> toBeHashed = new ArrayList<String>();
		while (colNameValue.hasNext()) {
			Entry<String, String> col = colNameValue.next();
			String colName = col.getKey();
			if (indexedCols.contains(colName))
				toBeHashed.add(col.getValue());
		}
		double hash[] = hashCols(toBeHashed);
		String pageIDRow = (String) kd.search(hash);
		//System.out.println(pageIDRow);
		if (pageIDRow == null)
			return null;
		String pageArr[] = pageIDRow.split("_");
		Page p = getPage(Integer.parseInt(pageArr[0]));
		return p.getTuple(Integer.parseInt(pageArr[1]));
	}

	public ArrayList<String> checkSingle(
			Hashtable<String, String> htblColNameValue) {
		ArrayList<String> res = new ArrayList<>();
		Iterator<Entry<String, String>> colNameValue = htblColNameValue
				.entrySet().iterator();
		while (colNameValue.hasNext()) {
			Entry<String, String> col = colNameValue.next();
			String colName = col.getKey();
			if (singleIndexes.contains(colName))
				res.add(colName);
		}
		return res;
	}

	public String getName() {
		return name;
	}

	private double[] hashCols(ArrayList<String> cols) {
		double res[] = new double[cols.size()];
		for (int i = 0; i < cols.size(); ++i)
			res[i] = hash(cols.get(i));
		return res;
	}

	private double hash(String value) {
		int hash = 7;
		for (int i = 0; i < value.length(); i++) {
			hash = hash * 31 + value.charAt(i);
		}
		return hash;
	}

	public String getPrefix() {
		return prefix;
	}
}
