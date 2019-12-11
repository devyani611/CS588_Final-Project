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

public class Q4
{
   public static void main(String[] args)
   {
       long starttime=System.currentTimeMillis();

       MongoClient mongoClient = new MongoClient(new MongoClientURI("mongodb://34.82.105.119:27017"));
       MongoDatabase database = mongoClient.getDatabase("test");
       MongoCollection<Document> collection = database.getCollection("stations");

       FindIterable<Document> fit = collection.find(Filters.eq("locationtext","Foster NB"));
       int stationid= fit.first().getInteger("stationid");
       double length= fit.first().getDouble("length");

       MongoCollection<Document> collection2 = database.getCollection("loopdata");


       SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

       try {

           Date date1 = formatter.parse("2011-09-22 07:00:00" );
           Date date2 = formatter.parse("2011-09-22 09:00:00" );
           Date date3 = formatter.parse("2011-09-22 16:00:00" );
           Date date4 = formatter.parse("2011-09-22 18:00:00" );
          
           AggregateIterable<Document> documents=collection2.aggregate(
                   Arrays.asList(Aggregates.project(Projections.fields(Projections.include("stationid","starttime","speed"))),

                           Aggregates.match(
                                   Filters.and
                                           (
                                                   Filters.eq("stationid",stationid),
                                                   Filters.or(
                                                           Filters.and
                                                                   (
                                                                           Filters.gte("starttime", date1),Filters.lt("starttime",date2 )
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
               travelTime = (length/avgSpeed)*3600;
           }

           
           System.out.println("travel time in seconds = "+travelTime);

       } catch (Exception e) {
           e.printStackTrace();
       }
       long stop=System.currentTimeMillis();
       System.out.println("Time taken in ms: "+(stop-starttime));
   }
}
