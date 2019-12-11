import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.AggregateIterable;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.*;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;

public class Q5 {
   public static void main(String[] args)
   {
       long starttime=System.currentTimeMillis();
       MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://34.82.105.119:27017"));
       MongoDatabase database = mongoClient.getDatabase("test");
       MongoCollection<Document> collection = database.getCollection("stations");
       MongoCollection<Document> collection2 = database.getCollection("loopdata");

       Document filterDoc = new Document();

       filterDoc.put("highwayname", "I-205");

       filterDoc.append("highway_direction", "NORTH");

       FindIterable<Document> fit = collection.find(filterDoc);

       double totalTravelTime=0;

       SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

       try
       {
           Date date1 = formatter.parse("2011-09-22 07:00:00" );
           Date date2 = formatter.parse("2011-09-22 09:00:00" );
           Date date3 = formatter.parse("2011-09-22 16:00:00" );
           Date date4 = formatter.parse("2011-09-22 18:00:00" );

           for(Document document:fit)
           {
               int stationid= document.getInteger("stationid");
               double length= document.getDouble("length");

               AggregateIterable<Document> documents=collection2.aggregate(
                       Arrays.asList(Aggregates.project(Projections.fields(Projections.include("stationid","starttime","speed"))),
                               Aggregates.match(
                                       Filters.and
                                               (
                                                       Filters.eq("stationid",stationid),
                                                       Filters.or(
                                                               Filters.and
                                                                       (
                                                                               Filters.gte("starttime",  date1),Filters.lt("starttime", date2)
                                                                       ),
                                                               Filters.and
                                                                       (
                                                                               Filters.gte("starttime", date3),Filters.lt("starttime", date4)
                                                                       )
                                                       )
                                               )
                               ),
                               Aggregates.group("$starttime", Accumulators.avg("speed", "$speed")),
                               Aggregates.sort(Sorts.ascending("_id"))

                       )
               ).allowDiskUse(true);

               double totalSpeed=0;
               int count=0;

               for(Document doc:documents)
               {
                   Double speed=doc.getDouble("speed");
                   if(speed!=null)
                   {
                       totalSpeed = totalSpeed + speed;
                       count++;
                   }
               }

               double avgSpeed=0;
               double travelTime=0;
               if(count!=0)
               {
                   avgSpeed= totalSpeed/count;
               }

               if (avgSpeed!=0)
               {
                   travelTime = (length/avgSpeed)*60;
               }

               totalTravelTime=totalTravelTime+travelTime;

           }
           
           System.out.println("Travel time in minutes = "+totalTravelTime);
       }
       catch (Exception e)
       {
           e.printStackTrace();
       }
       long stop=System.currentTimeMillis();
       System.out.println("Time taken in ms: "+(stop-starttime));
       
   }
}
