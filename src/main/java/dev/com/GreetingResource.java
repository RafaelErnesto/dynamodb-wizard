package dev.com;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.*;
import com.amazonaws.services.dynamodbv2.document.spec.QuerySpec;
import com.amazonaws.services.dynamodbv2.document.utils.ValueMap;

import javax.inject.Inject;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.Iterator;

@Path("/dynamo-wizard")
public class GreetingResource {

   // @Inject
   // AmazonDynamoDBClient client;

    @POST
    @Path("/insert")
    @Produces(MediaType.TEXT_PLAIN)
    public PutItemOutcome insert() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("users-table");

        Item item = new Item()
                .withPrimaryKey("id", "3")
                .withString("cpf", "00000000000")
                .withString("orderDate", "2022-08-11");
        return table.putItem(item);
    }

    @GET
    @Path("/item-by-pk")
    @Produces(MediaType.APPLICATION_JSON)
    public Item getItem() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("users-table");

        Item item = table.getItem("id", "1");
        return item;
    }

    @GET
    @Path("/item-by-index")
    @Produces(MediaType.APPLICATION_JSON)
    public void getItemByIndex() {
        AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().build();
        DynamoDB dynamoDB = new DynamoDB(client);
        Table table = dynamoDB.getTable("users-table");
        Index index = table.getIndex("cpf-orderDate-index");

        QuerySpec spec = new QuerySpec()
                .withKeyConditionExpression("cpf = :v_cpf")
                .withValueMap(new ValueMap()
                        .withString(":v_cpf","04928416163"))
                .withScanIndexForward(false);


        ItemCollection<QueryOutcome> items = index.query(spec);
        Iterator<Item> iter = items.iterator();
        while(iter.hasNext()) {
            System.out.println(iter.next().toJSONPretty());
        }
    }
}
