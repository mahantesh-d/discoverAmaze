package com.icici;

import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.query.Criteria;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TestCodes {	
	
	public static void main(String args[]) {
		//Get Dashboard Service Request
		Criteria c1 = new Criteria("USER_ID").is("ABC");
		Criteria c2 = new Criteria("DATE_CREATED").gte("2018-12-01 00:00:00.000");
		Criteria c3 = new Criteria("DATE_CREATED").lt("2018-12-07 00:00:00.000");
		Criteria andOperator = c1.andOperator(c2, c3);
		
		MatchOperation matchstage = Aggregation.match(andOperator);
		
		BasicDBList basicDBList=new BasicDBList();
		basicDBList.add("$STATUS");
		basicDBList.add("OPEN");
		
		DBObject mygroup = (DBObject) new BasicDBObject("$group", 
				new BasicDBObject("_id",
				new BasicDBObject("STATUS", 
						new BasicDBObject("$cond",
						new BasicDBObject("if", 
								new BasicDBObject("$ne", basicDBList))
								.append("then", "OPEN")
								.append("else", "CLOSED"))))
				.append("COUNT", new BasicDBObject("$sum", 1)));
		
		Aggregation aggregation = Aggregation.newAggregation(matchstage, new CustomGroupOperation(mygroup));
		//System.out.println("Query----> " + aggregation);
		
		//TransactionTimeline del_flag 
		
		
	}
	
}