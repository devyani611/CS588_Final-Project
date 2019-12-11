import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Aggregates;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Projections;

public class Q6 {
	public static void main(String[] args) {
		/*List to store the routes*/
		long start=System.currentTimeMillis();
		List<Document> result=new ArrayList<Document>();
		MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://localhost:27017"));
		MongoDatabase database = mongoClient.getDatabase("test");
		MongoCollection<Document> collection = database.getCollection("stations");
		String locationtext="";	
		/*Taking downstream ID of Johnson Cr*/
		 AggregateIterable<Document> documents=collection.aggregate(
			      Arrays.asList(Aggregates.match(Filters.eq("highwayname","I-205")),
			              Aggregates.match(Filters.eq("highway_shortdirection", "N")),
			              Aggregates.match(Filters.eq("locationtext", "Johnson Cr NB")),
			              Aggregates.project(Projections.fields(
			            		  Projections.include("stationid","locationtext","upstream","downstream")))));
		 int downstream=documents.first().getInteger("downstream");
		 result.add(documents.first());		 
		 /*Finding route using the downstream fields*/
		 /*while locationtext is not equal to Columnbia to I-205 iterate finding the downstream station*/
		 while(!(locationtext.equals("Columbia to I-205 NB"))) {
			 AggregateIterable<Document> document=collection.aggregate(
				      Arrays.asList(Aggregates.project(Projections.fields(
				    		  Projections.include("stationid","locationtext","upstream","downstream"))),
				              Aggregates.match(Filters.eq("stationid", downstream))));
			 /*Adding the downstream document to result*/
				       result.add(document.first());     
				       downstream=document.first().getInteger("downstream");
				       locationtext=document.first().getString("locationtext");}
		 for(Document doc:result) {
			 System.out.println(doc);
		 }
		 long stop=System.currentTimeMillis();
		 System.out.println("Time Taken in ms :"+(stop-start));
		 }}
