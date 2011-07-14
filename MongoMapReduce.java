/**
 * @author	Vaibhav Bhembre <vaibhav@bhembre.com>
 * @version 1.0
 * @since 2011.0714
 */

package com.bhembre.vaibhav.algorithms;

import com.mongodb.Mongo;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.MapReduceOutput;
import com.mongodb.util.JSON;
import java.util.Iterator;
import java.util.List;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.UnknownHostException;
import java.io.IOException;
import com.google.gson.Gson;

public class MongoMapReduce {
	
	/**
	 * Store the MongoDB database name in a class variable.
	 */
	private DB db = null;

	/**
	 * Store Collection name in a class variable.
	 */
	private DBCollection dbcoll = null;

	/**
	 * Mongo Driver
	 */
	private static Mongo m = null;

	public MongoMapReduce(DB db, DBCollection dbcoll) {
		this.db = db;
		this.dbcoll = dbcoll;
	}

	public DBCursor find(String collectionName) {
		DBCollection dbcollOut = db.getCollection(collectionName);
		return dbcollOut.find(); 
	}

	public ArrayList<Collection> runMR() {
		DBObject dbo = new BasicDBObject();
		ArrayList<Collection> arrColl = new ArrayList<Collection>();
		String mapper = "function(){this.tags.forEach(function(z){emit(z,{count:1});});};"; // Simple word-counting javascript mapper/reducer
		String reducer = "function(key,values){var total=0;for(var i=0; i<values.length; i++)total+=values[i].count; return {count: total};};";
		String output = "myout";
		DBCursor dbcur = dbcoll.mapReduce(mapper, reducer, output, null).getOutputCollection().find();
		while (dbcur.hasNext()) {
			dbo = dbcur.next();
			arrColl.add(new Gson().fromJson(dbo.toString(), Collection.class));
		}
		return arrColl;

	}

	public static void printResults(Result results) {
		ArrayList<Collection> arrColl = results.getArrColl();
		System.out.println("Displaying results:");
		for (Collection coll : arrColl) {
			System.out.println("The word \'" + coll.get_id() + "\' occurred " + coll.getValue().getCount() + " times.");
		}
	}

	public void randomInsert() {
		String[] args = null;
		args = new String[] { "dogs", "cats" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		args = new String[] { "dogs", "cats", "rats" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		args = new String[] { "dogs", "elephants" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		args = new String[] { "velociraptors", "t-rex" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		args = new String[] { "cthulhus", "griffins" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		args = new String[] { "griffins", "cats" };
		dbcoll.insert(new BasicDBObject().append("tags", args)); 
		System.out.println("Insertion complete.");
	}

	public static void main(String[] args) throws UnknownHostException, IOException {

		if (args.length < 1) {
			System.out.println("Incorrent usage.");
			System.out.println("Argument 0 missing. Database name required.");
			System.exit(2);
		}
		if (args.length < 2) {
			System.out.println("Incorrent usage.");
			System.out.println("Argument 1 missing. Collection name required.");
			System.exit(2);
		}

		String dbName = args[0];
		String collName = args[1];
		m = new Mongo(); // Initiate connection using default parameters. i.e. ("localhost", 27017)
		
		DB db = m.getDB(dbName);
		
		Set<String> collNames = new HashSet<String>(); 
		collNames = db.getCollectionNames();
		if (!collNames.contains(collName)) {
			System.out.print("Collection not found. Create a new one? [Y/N]: ");
			BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
			if (br.readLine().toUpperCase().contains("N")) {
				System.out.println("Collection not created. Exiting.");
				System.exit(2);
			}
		}

		DBCollection dbcoll = db.getCollection(collName);

		MongoMapReduce mmapred = new MongoMapReduce(db, dbcoll);
		mmapred.randomInsert();

		Result results = new Result();
		results.setArrColl(mmapred.runMR());
		
		MongoMapReduce.printResults(results);
	}

	class Collection {
		private String _id;
		private Count value;
		public String get_id() { return _id; }
		public Count getValue() { return value; }
		public void set_id(String _id) { this._id = _id; }
		public void setValue(Count value) { this.value = value; }
		public String toString() {
			return String.format("_id:%s,value:%s", _id, value);
		}
	}

	class Count {
		private Integer count;
		public Integer getCount() { return count; }
		public void setCount(Integer count) { this.count = count; }
		public String toString() {
			return String.format("count:%d", count);
		}
	}

	public static class Result {
		private ArrayList<Collection> arrColl;
		public ArrayList<Collection> getArrColl() { return arrColl; }
		public void setArrColl(ArrayList<Collection> arrColl) { this.arrColl = arrColl; }
	}
}
