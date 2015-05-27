package simpleDB;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

import simpleDB.Exceptions.DBAppException;

public class DBAppTest {

	// You must run this test at the beginning of testing
	// It creates two tables "Employee" & "Department"
	// It appends their data in the metafile as well as creates all required
	// .ser files and directories
	// It also creates the .ser files for the specified keys
	public static void testDBApp_Meta(DBApp dbEngine) {

		try {

			Hashtable<String, String> htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("ID", "java.lang.Integer");
			htblColNameType.put("Name", "java.lang.String");
			htblColNameType.put("Location", "java.lang.String");

			Hashtable<String, String> htblColNameRefs = new Hashtable<String, String>();
			htblColNameRefs.put("ID", "null");
			htblColNameRefs.put("Name", "null");
			htblColNameRefs.put("Location", "null");

			dbEngine.createTable("Department", htblColNameType,
					htblColNameRefs, "ID");

			String tableName = "Employee";
			htblColNameType = new Hashtable<String, String>();
			htblColNameType.put("ID", "java.lang.Integer");
			htblColNameType.put("Name", "java.lang.String");
			htblColNameType.put("Dept", "java.lang.String");
			htblColNameType.put("Start_Date", "java.util.Date");

			htblColNameRefs = new Hashtable<String, String>();
			htblColNameRefs.put("ID", "null");
			htblColNameRefs.put("Name", "null");
			htblColNameRefs.put("Dept", "Department.ID");
			htblColNameRefs.put("Start_Date", "null");

			dbEngine.createTable(tableName, htblColNameType, htblColNameRefs,
					"ID");

			dbEngine.saveAll();

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// After running the testDBAPP_Meta test to create the metadata for the
	// tables as well as the
	// ser files, this test fills those tables with some rows to be used later
	// on for testing
	public static void testDBApp_Insert(DBApp dbEngine) {

		try {
			String tableName = "Employee";

			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("ID", (new Integer(1)).toString());
			htblColNameValue.put("Name", "Mohamed Mehany");
			htblColNameValue.put("Dept", "IT");
			htblColNameValue.put("Start_Date", "12-12-2030");
			dbEngine.insertIntoTable(tableName, htblColNameValue);

			htblColNameValue.clear();

			htblColNameValue.put("ID", (new Integer(2)).toString());
			htblColNameValue.put("Name", "Mark Morcos");
			htblColNameValue.put("Dept", "Accounting");
			htblColNameValue.put("Start_Date", "12-08-2030");

			dbEngine.insertIntoTable(tableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("ID", (new Integer(3)).toString());
			htblColNameValue.put("Name", "Mayar Bassel");
			htblColNameValue.put("Dept", "IT");
			htblColNameValue.put("Start_Date", "12-12-2030");

			dbEngine.insertIntoTable(tableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("ID", (new Integer(4)).toString());
			htblColNameValue.put("Name", "Ebram Sherif");
			htblColNameValue.put("Dept", "Accounting");
			htblColNameValue.put("Start_Date", "12-09-2010");

			dbEngine.insertIntoTable(tableName, htblColNameValue);

			htblColNameValue.clear();
			htblColNameValue.put("ID", (new Integer(5)).toString());
			htblColNameValue.put("Name", "Adam Mathew");
			htblColNameValue.put("Dept", "HR");
			htblColNameValue.put("Start_Date", "2-09-2020");
			dbEngine.insertIntoTable(tableName, htblColNameValue);
			htblColNameValue.clear();
			htblColNameValue.put("ID", (new Integer(6)).toString());
			htblColNameValue.put("Name", "Andrea Willson");
			htblColNameValue.put("Dept", "IT");
			htblColNameValue.put("Start_Date", "4-10-2013");

			dbEngine.insertIntoTable(tableName, htblColNameValue);

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//creates a multi index on ID and Dept for table Employee
	public static void testDBApp_CreateMultiDimIndex(DBApp dbEngine){
		ArrayList<String> htblColNames = new ArrayList<String>();
		htblColNames.add("ID");
		htblColNames.add("Dept");
		try {
			dbEngine.createMultiDimIndex("Employee", htblColNames);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//select * From Employee where ID = 2 OR Dept = 'Accounting'
	//This is a test to test selecting using both indexed attributes
	public static void testDBApp_Select_Params_Indexed_OR(DBApp dbEngine){
		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("ID", "2");
			htblColNameValue.put("Dept", "Accounting");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "OR");
			while (iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//select * From Employee where ID = 2 AND Dept = 'Accounting'
		//This is a test to test selecting using both indexed attributes
	public static void testDBApp_Select_Params_Indexed_AND(DBApp dbEngine){
		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("ID", "2");
			htblColNameValue.put("Dept", "Accounting");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "AND");
			while (iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//Selects using only one attribute that is indexed
	public static void testDBApp_Select_OneParm_Indexed(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("ID", "2");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "OR");
			while (iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	

	// This test selects with one condition specified, and the condition is an
	// index
	// SELECT * FROM Employee WHERE ID = 2
	public static void testDBApp_Select_OneParm_NonIndexed(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("Dept", "IT");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "OR");
			while (iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Tests selecting with more than one condition specified using the OR
	// operator
	// SELECT * FROM Employee WHERE Dept = 'Accounting' OR Name = 'Mohamed
	// Mehany'
	public static void testDBApp_Select_Parms_OR(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("Dept", "Accounting");
			htblColNameValue.put("Name", "Mohamed Mehany");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "OR");
			while (iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	// Tests selecting with more than one condition specified using the AND
	// operator
	// SELECT * FROM Employee WHERE Dept = 'IT' AND Start_Date = '12-12-2030'
	public static void testDBApp_Select_Parms_AND(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("Name", "Mayar Bassel");
			htblColNameValue.put("Dept", "IT");

			Iterator<?> iter = dbEngine.selectFromTable(tableName,
					htblColNameValue, "and");
			while (iter != null && iter.hasNext()) {
				// keep printing rows numbers
				System.out.println(iter.next());
			}

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//Deletes using OR
	public static void testDBApp_DeleteOR(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("Name", "Mohamed Mehany");
			htblColNameValue.put("Dept", "Accounting");

			dbEngine.deleteFromTable(tableName, htblColNameValue, "OR");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * This method should result on the deletions of all the record that has
	 * this two conditions satisfied and this record should have a deletion
	 * marker.
	 */
	public static void testDBApp_DeleteAND(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("Name", "Mayar Bassel");
			htblColNameValue.put("Dept", "IT");

			dbEngine.deleteFromTable(tableName, htblColNameValue, "AND");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//Deletes using one specified attribute that is indexed
	public static void testDBApp_Delete_One_Indexed(DBApp dbEngine) {

		try {
			String tableName = "Employee";
			Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
			htblColNameValue.put("ID", "2");
			dbEngine.deleteFromTable(tableName, htblColNameValue, "OR");

		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	//inserts one record that is using one specified attribute that ins indexed
	public static void testDBApp_Insert_One_Indexed(DBApp dbEngine) {
		String tableName = "Employee";
		Hashtable<String, String> htblColNameValue = new Hashtable<String, String>();
		htblColNameValue.put("ID", (new Integer(2)).toString());
		htblColNameValue.put("Name", "Mark Morcos");
		htblColNameValue.put("Dept", "Accounting");
		htblColNameValue.put("Start_Date", "12-08-2030");
		try {
			dbEngine.insertIntoTable(tableName, htblColNameValue);
		} catch (ClassNotFoundException | DBAppException | IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	//selects using some specified attributes then deletes using the same attributes then tries to
	//reselect them but they should be deleted so it displays nothing then re inserts them 
	//and re selects them and they should appear again now
	public static void testDBApp_mix1(DBApp dbEngine) {
		System.out.println("First select:");
		testDBApp_Select_OneParm_Indexed(dbEngine);
		testDBApp_Delete_One_Indexed(dbEngine);
		System.out.println("Second Select:");
		testDBApp_Select_OneParm_Indexed(dbEngine);
		testDBApp_Insert_One_Indexed(dbEngine);
		System.out.println("Third Select:");
		testDBApp_Select_OneParm_Indexed(dbEngine);
	}

	public static void main(String[] args) throws IOException {
		DBApp dbEngine = new DBApp();
		dbEngine.init();
		testDBApp_Meta(dbEngine);
		testDBApp_Insert(dbEngine);
		//testDBApp_CreateMultiDimIndex(dbEngine);
		//testDBApp_Insert(dbEngine);
		testDBApp_Select_Params_Indexed_AND(dbEngine);
		//testDBApp_mix1(dbEngine);
		// Insert extra tests here
	}
}