package simpleDB;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import simpleDB.Exceptions.DBAppException;
import simpleDB.Exceptions.DBEngineException;

public class DBApp {
	public final static String currentDir = System.getProperty("user.dir");
	public final static String dataDir = currentDir + "/data/";
	public final static String name = "db1";
	public ArrayList<String> tables;

	public void init() throws IOException {
		File f = new File(dataDir + name);
		f.mkdirs();
		File conf = new File(currentDir + "/config/DBApp.config");
		Page.setMaxSize(conf);
		tables = new ArrayList<>();
	}

	public void createTable(String strTableName,
			Hashtable<String, String> htblColNameType,
			Hashtable<String, String> htblColNameRefs, String strKeyColName)
			throws DBAppException, IOException, ClassNotFoundException {
		File f = new File(DBApp.dataDir + strTableName);
		f.mkdirs();
		Table t = new Table(strTableName, htblColNameType, htblColNameRefs,
				strKeyColName);
		saveTable(t);
		tables.add(strTableName);
		createIndex(strTableName, strKeyColName);

	}

	public void createIndex(String strTableName, String strColName)
			throws DBAppException, ClassNotFoundException, IOException {
		Table t = getTable(strTableName);
		t.createIndex(strTableName, strColName);
		saveTable(t);
	}

	public void createMultiDimIndex(String strTableName,
			ArrayList<String> htblColNames) throws DBAppException, ClassNotFoundException, IOException {
		Table t = getTable(strTableName);
		t.createMultiDimIndex(htblColNames);
		saveTable(t);
		
	}

	void insertIntoTable(String strTableName,
			Hashtable<String, String> htblColNameValue) throws DBAppException,
			ClassNotFoundException, IOException {
		Table t = getTable(strTableName);
		t.insertTupleToTable(htblColNameValue);
		saveTable(t);

	}

	public void deleteFromTable(String strTableName,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		Table t = getTable(strTableName);
		Iterator<Tuple> res = t.findInTable(htblColNameValue, strOperator.toLowerCase());
		while (res.hasNext()) {
			Tuple tup = res.next();
			//System.out.println("HI123" + tup);
			t.deleteTupleFromTable(tup);
		}
		saveTable(t);

	}

	public Iterator<Tuple> selectFromTable(String strTable,
			Hashtable<String, String> htblColNameValue, String strOperator)
			throws DBEngineException, ClassNotFoundException, IOException {
		Table t = getTable(strTable);
		return t.findInTable(htblColNameValue, strOperator.toLowerCase());
	}

	public void saveAll() throws DBEngineException {

	}

	public Table getTable(String strTable) throws IOException,
			ClassNotFoundException {
		ObjectInputStream in = new ObjectInputStream(new FileInputStream(
				dataDir + strTable + ".ser"));
		Table t = (Table) in.readObject();
		in.close();
		return t;
	}

	public void saveTable(Table t) throws IOException, ClassNotFoundException {
		ObjectOutputStream out = new ObjectOutputStream(new FileOutputStream(
				dataDir + t.getName() + ".ser"));
		out.writeObject(t);
		out.close();
	}
}
