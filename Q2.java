import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

import org.bson.Document;

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

public class Q2 {
	public static void main(String[] args) {
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://34.82.105.119:27017"));
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection<Document> collection = database.getCollection("stations");
		FindIterable<Document> fit = collection.find(Filters.eq("locationtext", "Foster NB"));
		/**Fetching stationid from stations**/
		int stationid = fit.first().getInteger("stationid");
		SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		try {
			Date date1 = formatter.parse("2011-09-21 00:00:00");
			Date date2 = formatter.parse("2011-09-22 00:00:00");
		MongoCollection<Document> collection2 = database.getCollection("loopdata");
		/**Aggregating over  freeway loopdata*/
		/**Aggregation Pipeline
		 *1. Project: retain only stationid,starttime and volume further
		 *2. Match station id of Foster NB
		 *3. Retain only 21nd Sept 2011 data
		 *4. Group by stationid and find the total sum of volume **/
		AggregateIterable<Document> documents = collection2.aggregate(Arrays.asList(
				Aggregates.project(
						Projections.fields(Projections.include("stationid", "starttime","volume"))),
				Aggregates.match(Filters.eq("stationid", stationid)),
				Aggregates.match(Filters.and(Filters.gte("starttime", date1), Filters.lt("starttime", date2))),
			    Aggregates.group("$stationid", Accumulators.sum("Total volume", "$volume"))
				)
				).allowDiskUse(true);
		for(Document doc:documents) {
			System.out.println(doc);
		}
		}catch (Exception e) {
			System.out.println(e);
		}			
	}
}
