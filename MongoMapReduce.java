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
import java.util.HashSet;
import java.util.Set;
import java.io.BufferedReader;
import java.io.InputStreamReader;

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

	public void runMR() {
		String mapper = "function(){this.tags.forEach(function(z){emit(z,{count:1});});};";
		String reducer = "function(key,values){var total=0;for(var i=0; i<values.length; i++)total+=values[i].count; return {count: total};};";
		String output = "myout";
		dbcoll.mapReduce(mapper, reducer, output, null);
		System.out.println("Ran Mapreduce. Check the output in Mongo CLI using: db." + output + ".find();");
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

	public static void main(String[] args) throws java.net.UnknownHostException, java.io.IOException {

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
		mmapred.runMR();
	}
}
