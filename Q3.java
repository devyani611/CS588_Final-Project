import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintStream;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.List;

import org.bson.Document;

import com.mongodb.BasicDBObject;
import com.mongodb.Block;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;
import com.mongodb.client.model.Sorts;
import com.sun.corba.se.spi.orbutil.fsm.Guard.Result;

public class Q3 {
	public static void main(String[] args) {       
		long start=System.currentTimeMillis();
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://34.82.105.119:27017"));
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection<Document> collection = database.getCollection("stations");
		FindIterable<Document> fit = collection.find(Filters.eq("locationtext", "Foster NB"));
		/**Fetching stationid from stations**/
		int stationid = fit.first().getInteger("stationid");
		/**Fetching length of station from stations**/
		double length = fit.first().getDouble("length");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date date1 = formatter.parse("2011-09-22 00:00:00");
			Date date2 = formatter.parse("2011-09-23 00:00:20");
			MongoCollection<Document> collection2 = database.getCollection("loopdata");
			/**Aggregating over  freeway loopdata*/
			/**Aggregation Pipeline
			 *1. Project: retain only stationid,starttime and speed further
			 *2. Match station id of Foster NB
			 *3. Retain only 22nd Sept 2011 data
			 *4. Since there are 3 detectors for Foster NB, group data by time and take average speed over all 3 detectors for 20 second interval
			 *5. Sort the starttime in ascending order **/
			AggregateIterable<Document> documents = collection2.aggregate(Arrays.asList(
					Aggregates.project(Projections.fields(Projections.include("stationid", "starttime", "speed"))),
					Aggregates.match(Filters.eq("stationid", stationid)),
					Aggregates.match(Filters.and(Filters.gte("starttime", date1), Filters.lt("starttime", date2))),
					Aggregates.group("$starttime", Accumulators.avg("speed", "$speed")),
					Aggregates.sort(Sorts.ascending("_id"))
					)
					).allowDiskUse(true);
			int nowithvalues = 0;double sum = 0;int minutes = 5;int seconds = 0;
			/** Iterating for 20 second interval. If data for that interval is missing then go into the while loop till we get a 
			 * time interval which matches the document time interval*/
			/**For every 5 minutes output the travel time.
			 * If interval is missing then output travel time as "not available" */
			Date secondinterval = new Date(date2.getYear(), date2.getMonth(), 22, 00, 00, seconds);
			for (Document doc : documents) {			
				Date doc_date = doc.getDate("_id");
				Date minuteinterval = new Date(date2.getYear(), 9, 22, 00, minutes, 00);
				/**If calculated time is same as document timestamp**/
				if (doc_date.getHours() == secondinterval.getHours()
						&& doc_date.getMinutes() == secondinterval.getMinutes()
						&& doc_date.getSeconds() == secondinterval.getSeconds()) {
					if (doc.getDouble("speed") != null) {
						sum = sum + doc.getDouble("speed");
						nowithvalues++; }
					/**For every 5 minutes output the calculated travel time. If the speed was null then output 0*/
					if (secondinterval.getHours() == minuteinterval.getHours()
							&& secondinterval.getMinutes() == minuteinterval.getMinutes()
							&& secondinterval.getSeconds() == minuteinterval.getSeconds()) {						
							double averagespeed = sum / nowithvalues;
							double traveltime = (length / averagespeed) * 3600;
							System.out.println((new Document("timestamp", secondinterval))
									.append("traveltime", traveltime));
							sum = 0;nowithvalues = 0;minutes = minutes + 5;	}}
					else {
					/**If calculated time is not the same as document timestamp. That is data is missing**/
					/**Iterate the while loop till we get the calculated time matching the document time**/
					Date minute_interval = new Date(date2.getYear(), date2.getMonth(), 22, 00, minutes, 00);
					while (doc_date.getHours() != secondinterval.getHours()
							|| doc_date.getMinutes() != secondinterval.getMinutes()
							|| doc_date.getSeconds() != secondinterval.getSeconds()) {
						/**For every 5 minutes output the travel time to be "not available"**/
						if (minute_interval.getHours() == secondinterval.getHours()
								&& minute_interval.getMinutes() == secondinterval.getMinutes()
								&& minute_interval.getSeconds() == secondinterval.getSeconds()) {
							minutes = minutes + 5;
							System.out.println(
									(new Document("timestamp", minute_interval)).append("traveltime", "not available"));}
						minute_interval = new Date(date2.getYear(), date2.getMonth(), 22, 00, minutes, 00);
						seconds = seconds + 20;
						secondinterval = new Date(date2.getYear(), date2.getMonth(), 22, 00, 00, seconds);}}				
				seconds = seconds + 20;
				secondinterval = new Date(date2.getYear(), date2.getMonth(), 22, 00, 00, seconds);
				}
		} catch (ParseException e) {
			System.out.println(e);
			}
		long stop=System.currentTimeMillis();
		System.out.println("Time Taken to execute the query in ms :"+(stop-start));
		}
	}
