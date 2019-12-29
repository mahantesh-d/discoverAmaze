package com.icici;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.data.mongodb.core.aggregation.LimitOperation;
import org.springframework.data.mongodb.core.aggregation.MatchOperation;
import org.springframework.data.mongodb.core.aggregation.SkipOperation;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.stereotype.Service;

import com.icici.model.CallCenterResponse;
import com.icici.model.TransactionTimeline;
import com.icici.model.UpdateCategory;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.WriteResult;

@Service
public class AmazeService {

	@Autowired
	private MongoTemplate mongoTemplate;

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"dd MMM yy");

	private static final Logger logger = Logger.getLogger(AmazeService.class);

	public Date getLatestRecordDate(String collectionName) {
		Date latestDate = null;
		Statistics getLatestRecordDateStatistics = new Statistics();

		Criteria c1 = new Criteria("STATUS").is("ENDED");
		Criteria c2 = new Criteria("COLLECTION_NAME").is(collectionName);

		Criteria andOperator = c1.andOperator(c2);

		MatchOperation matchStage = Aggregation.match(andOperator);
		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("CURRENT_TS", -1));

		Aggregation aggregation = null;

		LimitOperation limitOperation = Aggregation.limit(1);
		aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(sortCondition), limitOperation);

		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_JOB_AUDIT", DBObject.class);
		// logger.info("AMAZE- Total time taken for getLatestRecordDate::"
		// + getLatestRecordDateStatistics.totalTimeRequired());
		DBObject dbObject = output.getMappedResults().size() > 0 ? output
				.getMappedResults().get(0) : null;

		// logger.info("dbObject::" + dbObject);
		if (dbObject.get("JOB_DATE") instanceof Date) {
			latestDate = dbObject != null ? (Date) dbObject.get("JOB_DATE")
					: null;
		} else if (dbObject.get("JOB_DATE") instanceof String) {
			Object object = dbObject.get("JOB_DATE");
			if (object != null) {
				try {
					latestDate = new SimpleDateFormat("dd-MM-yyyy")
							.parse((String) object);
				} catch (ParseException e) {
					logger.warn(
							"Please check AMAZE_JOB_AUDIT table, not able to parse date."
									+ "Format should be dd-MM-yyyy or it should be date object.",
							e);
				}
			}
		}
		if (latestDate == null) {
			logger.warn("Please check AMAZE_JOB_AUDIT. "
					+ "AMAZE_JOB_AUDIT should have ENDED date for job. "
					+ "Considering current date minus 2 days for collection "
					+ collectionName);
			Calendar calendar = Calendar.getInstance();
			calendar.add(Calendar.DAY_OF_MONTH, -2);
			calendar.set(Calendar.HOUR, 00);
			calendar.set(Calendar.MINUTE, 00);
			calendar.set(Calendar.SECOND, 00);
			calendar.set(Calendar.MILLISECOND, 00);
			latestDate = calendar.getTime();
		}
		return latestDate;
	}

	public List<String> getAccounts(String userId) {
		Statistics getUserIdStatistics = new Statistics();
		List<String> accountList = new ArrayList<String>();

		Date latestRecordDate = getLatestRecordDate("AMAZE_USERID_ACCOUNT");

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);
		// logger.info("before adding day::" + calendar.getTime());
		calendar.add(Calendar.DAY_OF_MONTH, 1);
		// logger.info("after adding day::" + calendar.getTime());

		Criteria c1 = new Criteria("USER_ID").is(userId);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		Aggregation aggregation = Aggregation.newAggregation(matchStage);

		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_USERID_ACCOUNT", DBObject.class);
		// logger.info(" USERID "+userId+" getLatestRecord "
		// + getUserIdStatistics.totalTimeRequired());

		for (DBObject dbObject : output) {
			accountList.add((String) dbObject.get("SOURCE_ACCOUNT_NBR"));
		}
		// logger.info("accountList"+accountList);
		return accountList;
	}

	public List<String> getAccountsCC(String userId, String SOURCE_SYSTEM_CODE) {
		Statistics getUserIdStatistics = new Statistics();
		List<String> accountList = new ArrayList<String>();
		Date latestRecordDate = getLatestRecordDate("AMAZE_USERID_ACCOUNT");
		// logger.info("latest record date is" +latestRecordDate);
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);
		// logger.info("before adding day::" + calendar.getTime());
		calendar.add(Calendar.DAY_OF_MONTH, 1);
		// logger.info("after adding day::" + calendar.getTime());

		Criteria c1 = new Criteria("USER_ID").is(userId);
		Criteria c2 = new Criteria("SOURCE_SYSTEM_CODE").is(SOURCE_SYSTEM_CODE);
		Criteria c3 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c4 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3, c4);
		MatchOperation matchStage = Aggregation.match(andOperator);

		Aggregation aggregation = Aggregation.newAggregation(matchStage);

		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_USERID_ACCOUNT", DBObject.class);
		// logger.info(" USERID "+userId+" getLatestRecord "
		// + getUserIdStatistics.totalTimeRequired());

		for (DBObject dbObject : output) {
			accountList.add((String) dbObject.get("SOURCE_ACCOUNT_NBR"));
		}
		// logger.info("accountList"+accountList);
		return accountList;
	}

	public DBObject getTillDate(String USERID) {

		Statistics getOldestDateStatistics = new Statistics();
		Date latestRecordDate = getLatestRecordDate("AMAZE_TRANS_PARAM");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);
		// logger.info("before adding day::" + calendar.getTime());
		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Criteria c1 = new Criteria("USER_ID").is(USERID);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		MatchOperation matchStage = Aggregation.match(c1.andOperator(c2, c3));

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("MAX_TRAN_DATE", -1));

		LimitOperation limitOperation = Aggregation.limit(1);
		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(sortCondition), limitOperation);

		AggregationResults<DBObject> Output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANS_PARAM", DBObject.class);

		// logger.info(" USERID "+USERID+" getTillDate "
		// + getOldestDateStatistics.totalTimeRequired());

		DBObject dbObject = Output.getMappedResults().size() > 0 ? Output
				.getMappedResults().get(0) : null;

		BasicDBObject basicDBObject = (BasicDBObject) dbObject;
		return dbObject;

	}

	public Double getSpendingSumForCurrentMonth1(String userId) {

		Statistics getSpendingSumForCurrentMonthStatistics = new Statistics();

		Double spendingSumForCurrentMonth = 0.0;
		SimpleDateFormat monFormat = new SimpleDateFormat("MMM");
		SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
		Calendar calendar = Calendar.getInstance();
		Date time = calendar.getTime();
		String MMM = monFormat.format(time);
		String yyyy = yearFormat.format(time);
		// logger.info("MMM::" + MMM);
		List<String> accountList = getAccounts(userId);
		Criteria c1 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);
		Criteria c2 = new Criteria("TRAN_MON").is(MMM.toUpperCase());
		Criteria c3 = new Criteria("TRAN_YEAR").is(yyyy);
		Criteria c4 = new Criteria("INCOME_EXPENSE").is("EXPENSE");
		Criteria c1Andc2Criteria = c1.andOperator(c2, c3, c4);

		MatchOperation matchStage = Aggregation.match(c1Andc2Criteria);

		DBObject myGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("_id", 1)
						.append("AMOUNT", 1)
						.append("TRAN_MON", 1)
						.append("EXPENSE",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))

		);
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("MONTH", "$TRAN_MON"

				)).append("spends", new BasicDBObject("$sum", "$EXPENSE")));

		// logger.info("aggregation1:" + myGroup);
		// logger.info("aggregation2:" + myGroup1);
		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup), new CustomGroupOperation(
						myGroup1));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		List<DBObject> dbObjects = output.getMappedResults();
		if (dbObjects.size() >= 1) {
			Object object = dbObjects.get(0).get("spends");
			if (object != null) {
				spendingSumForCurrentMonth = Double.valueOf(object.toString());
			}
		}

		// logger.info(" AMAZE- Total time taken for getDashboard-getSpendingSumForCurrentMonth "
		// + getSpendingSumForCurrentMonthStatistics.totalTimeRequired());
		return spendingSumForCurrentMonth;
	}

	public Double getSpendingSumForCurrentMonth(String userId) {

		Statistics getSpendingSumForCurrentMonthStatistics = new Statistics();

		Double spendingSumForCurrentMonth = 0.0;
		SimpleDateFormat monFormat = new SimpleDateFormat("MMM");
		SimpleDateFormat yearFormat = new SimpleDateFormat("yyyy");
		Calendar calendar = Calendar.getInstance();
		Date time = calendar.getTime();
		String MMM = monFormat.format(time);
		String yyyy = yearFormat.format(time);
		Calendar now = Calendar.getInstance();
		now.add(Calendar.MONTH, 0);
		now.set(Calendar.DAY_OF_MONTH, 1);
		now.set(Calendar.HOUR_OF_DAY, 00);
		now.set(Calendar.MINUTE, 00);
		now.set(Calendar.SECOND, 00);
		now.set(Calendar.MILLISECOND, 000);

		Date currentMonth = now.getTime();

		List<String> accountList = getAccounts(userId);
		Criteria c1 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);
		Criteria c2 = new Criteria("TRAN_MON").is(monFormat
				.format(currentMonth).toUpperCase());
		Criteria c3 = new Criteria("INCOME_EXPENSE").is("EXPENSE");
		Criteria c4 = new Criteria("TRAN_YEAR").is(yearFormat
				.format(currentMonth));
		Criteria c5 = new Criteria("MERCHANT").ne("ICICI BANK CREDIT CA");
		Criteria c6 = new Criteria("MERCHANT").ne("ICICI BANK CC PAYMENT");
		Criteria c1Andc2Criteria = c1.andOperator(c2, c3, c4, c5, c6);

		MatchOperation matchStage = Aggregation.match(c1Andc2Criteria);

		DBObject myGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("_id", 1)
						.append("AMOUNT", 1)
						.append("TRAN_MON", 1)
						.append("EXPENSE",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))

		);
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("MONTH", "$TRAN_MON"

				)).append("spends", new BasicDBObject("$sum", "$EXPENSE")));

		// logger.info("aggregation1:" + myGroup);
		// logger.info("aggregation2:" + myGroup1);
		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup), new CustomGroupOperation(
						myGroup1));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		List<DBObject> dbObjects = output.getMappedResults();
		if (dbObjects.size() >= 1) {
			Object object = dbObjects.get(0).get("spends");
			if (object != null) {
				spendingSumForCurrentMonth = Double.valueOf(object.toString());
			}
		}

		// logger.info(" USERID "+userId+" getDashboard-getSpendingSumForCurrentMonth "
		// + getSpendingSumForCurrentMonthStatistics.totalTimeRequired());
		return spendingSumForCurrentMonth;
	}

	public List<DBObject> getServiceRequestOverview(String userID) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_SR_DELIVERABLE_STATUS");
		// logger.info("latestRecordDate::" + latestRecordDate);

		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);
		// logger.info("before adding day::" + calendar.getTime());
		calendar.add(Calendar.DAY_OF_MONTH, 1);
		// logger.info("after adding day::" + calendar.getTime());
		Statistics getServiceRequestOverviewStatistics = new Statistics();

		Criteria c1 = new Criteria("USER_ID").is(userID);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", "$STATUS").append("count",
						new BasicDBObject("$sum", 1)));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_SR_DELIVERABLE_STATUS", DBObject.class);
		List<DBObject> serviceRequestOverview = output.getMappedResults();
		for (DBObject dbObject : serviceRequestOverview) {
			String id = (String) dbObject.get("_id");
			if (!id.equalsIgnoreCase("CLOSED")) {
				dbObject.put("_id", "OPEN");
			}
		}

		// logger.info(" USERID "+userID+" getDashboard-getServiceRequestOverview "
		// + getServiceRequestOverviewStatistics.totalTimeRequired());
		return serviceRequestOverview;
	}

	public long getRecommendationsCount(String userID) {
		Statistics getRecommendationsCountStatistics = new Statistics();

		Date latestRecordDate = getLatestRecordDate("AMAZE_RECOMMENDATION_ENGINE");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Criteria c1 = new Criteria("USER_ID").is(userID);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria msgTypeCriteria = new Criteria("MSG_TYPE").ne("DISCOVER NEW");
		Criteria andOperator = c1.andOperator(msgTypeCriteria, c2, c3);

		Query query = new Query(andOperator);

		long count = mongoTemplate.count(query, "AMAZE_RECOMMENDATION_ENGINE");

		// logger.info(" USERID "+userID+" getDashboard-getRecommendationsCount "
		// + getRecommendationsCountStatistics.totalTimeRequired());
		return count;
	}

	public List<DBObject> getRecommendationsOverview(String userID) {

		Statistics getRecommendationsOverviewStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userID);
		MatchOperation matchStage = Aggregation.match(c1);

		Aggregation aggregation = Aggregation.newAggregation(matchStage);
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_RECOMMENDATION_ENGINE", DBObject.class);
		List<DBObject> recommendationsOverview = output.getMappedResults();

		// logger.info(" USERID "+userID+" getDashboard-getRecommendationsOverview "
		// + getRecommendationsOverviewStatistics.totalTimeRequired());
		return recommendationsOverview;
	}

	public List<DBObject> getBugetMaster(String USERID) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_BUDGET_MASTER");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Statistics getBugetMasterStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(USERID);
		Criteria c2 = new Criteria("INCOME_EXPENSE").is("EXPENSE");

		Criteria c3 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c4 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria matcher2 = c1.andOperator(c2, c3, c4);

		MatchOperation matchStage2 = Aggregation.match(matcher2);

		DBObject myGroup2 = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("BUDGET_AMOUNT", 1).append(
						"BUCKET", 1));

		Aggregation aggregation1 = Aggregation.newAggregation(matchStage2,
				new CustomGroupOperation(myGroup2));
		AggregationResults<DBObject> output1 = mongoTemplate.aggregate(
				aggregation1, "AMAZE_BUDGET_MASTER", DBObject.class);
		List<DBObject> budgetList = output1.getMappedResults();
		// logger.info(" USERID "+USERID+" getOverview-getBudgetMaster "
		// + getBugetMasterStatistics.totalTimeRequired());
		return budgetList;
	}

	public List<DBObject> getCategoryList(String type) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_CATEGORY_MASTER");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Statistics getCategoryListStatistics = new Statistics();
		Criteria c1 = new Criteria("DR_CR_FLAG").is(type);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());

		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("BUCKET", 1).append(
						"CATEGORY", 1));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_CATEGORY_MASTER", DBObject.class);
		// logger.info(" AMAZE- Total time taken for getCategoryList in service "
		// + getCategoryListStatistics.totalTimeRequired());
		return output.getMappedResults();

	}

	public List<TransactionTimeline> getTransactions(String USERID, Long LIMIT,
			Long OFFSET, Date last12MonthsDate) {

		Statistics getTransactionsStatistics = new Statistics();
		List<String> accountList = getAccounts(USERID);
		
		Criteria c1 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);
		Criteria c2 = new Criteria("TRAN_DATE_TIME").gte(last12MonthsDate);
		// Criteria c3 = new Criteria("CATEGORY").ne("CC Payment Received");
		MatchOperation matchStage = Aggregation.match(c1.andOperator(c2));

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("TRAN_DATE_TIME", -1));

		DBObject projectGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("TRAN_DATE_TIME", 1)
						.append("TRAN_DAY", 1).append("TRAN_MON", 1)
						.append("TRAN_YEAR", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("SOURCE_SYSTEM_CODE", 1).append("MERCHANT", 1)
						.append("CATEGORY", 1).append("MERCHANT", 1)
						.append("CATEGORY", 1).append("DEBIT_CREDIT", 1)
						.append("AMOUNT", 1).append("BUCKET", 1)
						.append("EVENTID", 1).append("INCOME_EXPENSE", 1)
						// .append("MESSAGE", 1)
						.append("LINK_FLAG", 1).append("DEL_FLAG", 1)
						.append("CAPTION", 1).append("REDIR_FLAG", 1)
		// .append("RED_PARAM", 1)
		);

		Aggregation aggregation = null;

		if (LIMIT != null && OFFSET != null) {
			LimitOperation limitOperation = Aggregation.limit(LIMIT);
			SkipOperation skipOperation = Aggregation.skip(OFFSET);
			aggregation = Aggregation.newAggregation(matchStage,
					limitOperation, skipOperation, new CustomGroupOperation(
							sortCondition));
		} else {

			aggregation = Aggregation.newAggregation(matchStage,
					new CustomGroupOperation(projectGroup),
					new CustomGroupOperation(sortCondition));

		}
		AggregationResults<TransactionTimeline> output = mongoTemplate
				.aggregate(aggregation, "AMAZE_TRANSACTION_TIMELINE",
						TransactionTimeline.class);
		// logger.info(" USERID "+USERID+" getTransactions-getTransactionList "
		// + getTransactionsStatistics.totalTimeRequired());
		for (TransactionTimeline transactionTimeline : output) {
			String del_FLAG = transactionTimeline.getDEL_FLAG();
			/*
			 * this code convert time UTC zone (get time -5:30 hr)
			 */
			/*
			 * Date tran_DATE_TIME = transactionTimeline.getTRAN_DATE_TIME();
			 * Calendar instance = Calendar.getInstance();
			 * instance.setTime(tran_DATE_TIME); instance.add(Calendar.HOUR,
			 * -5); instance.add(Calendar.MINUTE, -30); TimeZone timeZone =
			 * TimeZone.getTimeZone("UTC"); instance.setTimeZone(timeZone); Date
			 * time = instance.getTime();
			 * transactionTimeline.setTRAN_DATE_TIME(time);
			 */
			if (del_FLAG != null && !del_FLAG.equals("N")) {
				transactionTimeline.setLINK_FLAG(null);
				transactionTimeline.setCAPTION(null);
				transactionTimeline.setREDIR_Flag(null);
			}

		}
		return output.getMappedResults();

	}

	public List<TransactionTimeline> getTransactionsV2(String USERID,
			Long LIMIT, Long OFFSET, Date last12MonthsDate) {

		Statistics getTransactionsStatistics = new Statistics();
		List<String> accountList = getAccounts(USERID);

		Criteria c1 = new Criteria("TRAN_DATE_TIME").gte(last12MonthsDate);
		Criteria c2 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);

		MatchOperation matchStage = Aggregation.match(c1.andOperator(c2));

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("TRAN_DATE_TIME", -1));

		DBObject projectGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("TRAN_DATE_TIME", 1)
						.append("TRAN_DAY", 1).append("TRAN_MON", 1)
						.append("TRAN_YEAR", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("SOURCE_SYSTEM_CODE", 1).append("MERCHANT", 1)
						.append("CATEGORY", 1).append("MERCHANT", 1)
						.append("CATEGORY", 1).append("DEBIT_CREDIT", 1)
						.append("AMOUNT", 1).append("BUCKET", 1)
						.append("EVENTID", 1).append("INCOME_EXPENSE", 1));

		Aggregation aggregation = null;

		if (LIMIT != null && OFFSET != null) {
			LimitOperation limitOperation = Aggregation.limit(LIMIT);
			SkipOperation skipOperation = Aggregation.skip(OFFSET);
			aggregation = Aggregation.newAggregation(matchStage,
					limitOperation, skipOperation, new CustomGroupOperation(
							sortCondition));
		} else {

			aggregation = Aggregation.newAggregation(matchStage,
					new CustomGroupOperation(projectGroup),
					new CustomGroupOperation(sortCondition));

		}
		AggregationResults<TransactionTimeline> output = mongoTemplate
				.aggregate(aggregation, "AMAZE_TRANSACTION_TIMELINE_SAMPLE",
						TransactionTimeline.class);
		// logger.info(" USERID "+USERID+" getTransactions-getTransactionList "
		// + getTransactionsStatistics.totalTimeRequired());
		return output.getMappedResults();

	}

	public List<DBObject> getRecommendationList(String userId) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_RECOMMENDATION_ENGINE");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Statistics getRecommendationListStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userId);

		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());

		Criteria andOperator = c1.andOperator(c2, c3);

		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("MSG_Type", "$MSG_TYPE").append("_id", 0)
						.append("Com_Text", "$COM_TEXT")
						.append("Source_account_nbr", "$SOURCE_ACCOUNT_NBR")
						.append("category", "$CATEGORY")
						.append("CTA_HASHTAG", 1).append("PARAM_1", 1));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup));

		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_RECOMMENDATION_ENGINE", DBObject.class);
		// logger.info(output);
		// logger.info(" USERID "+userId+" getRecommendation-getRecommendationList "
		// + getRecommendationListStatistics.totalTimeRequired());
		return output.getMappedResults();

	}

	// getRecommendationCorpList

	public List<DBObject> getRecommendationCorpList(String userId) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_RECOMMENDATION_ENGINE_CORP");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Statistics getRecommendationListStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userId);

		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());

		Criteria andOperator = c1.andOperator(c2, c3);

		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("MSG_Type", "$MSG_TYPE").append("_id", 0)
						.append("Com_Text", "$COM_TEXT")
						.append("Source_account_nbr", "$SOURCE_ACCOUNT_NBR")
						.append("category", "$CATEGORY")
						.append("CTA_HASHTAG", 1).append("PARAM_1", 1));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup));

		AggregationResults<DBObject> output = mongoTemplate
				.aggregate(aggregation, "AMAZE_RECOMMENDATION_ENGINE_CORP",
						DBObject.class);
		// logger.info(output);
		// logger.info(" USERID "+userId+" getRecommendation-getRecommendationList "
		// + getRecommendationListStatistics.totalTimeRequired());
		return output.getMappedResults();

	}

	public List<DBObject> getUpcomingTransaction(String userID) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_UPCOMING_TRANSACTION");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("DUE_DATE", 1));
		Statistics getUpcomingTransactionStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userID);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("CATEGORY", 1).append("_id", 0)
						.append("src_account_no", "$SRC_ACCOUNT_NO")
						.append("MERCHANT", 1).append("DUE_AMT", 1)
						.append("DUE_DATE", 1).append("RECOMMENDATION", 1)
						.append("cta_flag", "$CTA_FLAG"));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_UPCOMING_TRANSACTION", DBObject.class);
		// logger.info(" USERID "+userID+" getUpcomingTransactions-getUpcomingTransactionsList "
		// + getUpcomingTransactionStatistics.totalTimeRequired());
		return output.getMappedResults();
	}

	public List<DBObject> getUpcomingTransactionV2(String userID) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_UPCOMING_TRANSACTION");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("DUE_DATE", 1));
		Statistics getUpcomingTransactionStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userID);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("CATEGORY", 1).append("_id", 0)
						.append("src_account_no", "$SRC_ACCOUNT_NO")
						.append("MERCHANT", 1).append("DUE_AMT", 1)
						.append("DUE_DATE", 1).append("RECOMMENDATION", 1)
						.append("cta_flag", "$CTA_FLAG"));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_UPCOMING_TRANSACTION_TEST", DBObject.class);
		// logger.info(" USERID "+userID+" getUpcomingTransactions-getUpcomingTransactionsList "
		// + getUpcomingTransactionStatistics.totalTimeRequired());
		return output.getMappedResults();
	}

	public List<DBObject> getSRStatus(String userId) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_SR_DELIVERABLE_STATUS");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Statistics getSRStatusStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userId);
		Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c3 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria andOperator = c1.andOperator(c2, c3);
		MatchOperation matchStage = Aggregation.match(andOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("FLAG", 1)
						.append("TRACKING_NUMBER", 1)
						.append("SOURCE_ACCOUNT_NBR", 1)
						.append("REQUEST_DESC", 1)
						.append("TARGET_CLOSE_DATE", 1)
						.append("ACTUAL_CLOSE_DATE", 1)
						.append("REQUEST_DATE", 1).append("STATUS", 1)
						.append("STAGE", 1));
		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("REQUEST_DATE", -1));

		// logger.info("srStatus count::" + myGroup);
		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(myGroup), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_SR_DELIVERABLE_STATUS", DBObject.class);
		// logger.info(" USERID "+userId+" getSRStatus-getSRStatusList "
		// + getSRStatusStatistics.totalTimeRequired());
		return output.getMappedResults();
	}

	public List<DBObject> getTotalBudget(String userId) {

		Date latestRecordDate = getLatestRecordDate("AMAZE_BUDGET_MASTER");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		// System.out.println("LATEST RECORD DATE FOR GET TOTAL BUDGET IS "+latestRecordDate);
		// System.out.println("get time DATE FOR GET TOTAL BUDGET IS "+calendar.getTime());
		Statistics getTotalBudgetStatistics = new Statistics();
		Criteria c1 = new Criteria("USER_ID").is(userId);

		Criteria c2 = new Criteria("INCOME_EXPENSE").is("EXPENSE");

		Criteria c3 = new Criteria("DATE_CREATED").gte(latestRecordDate);
		Criteria c4 = new Criteria("DATE_CREATED").lt(calendar.getTime());
		Criteria matcher2 = c1.andOperator(c2, c3, c4);

		MatchOperation matchStage1 = Aggregation.match(matcher2);
		DBObject myGroup2 = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("_id", 0).append("USER_ID", 1).append(
						"BUDGET_AMOUNT", 1));
		DBObject myGroup3 = (DBObject) new BasicDBObject(
				"$group",
				new BasicDBObject("_id", new BasicDBObject("userid", "$USER_ID"

				)).append("budget", new BasicDBObject("$sum", "$BUDGET_AMOUNT")));
		Aggregation aggregation1 = Aggregation.newAggregation(matchStage1,
				new CustomGroupOperation(myGroup2), new CustomGroupOperation(
						myGroup3));
		AggregationResults<DBObject> output1 = mongoTemplate.aggregate(
				aggregation1, "AMAZE_BUDGET_MASTER", DBObject.class);
		// logger.info(" USERID "+userId+" getOverview-getTotalBudget "
		// + getTotalBudgetStatistics.totalTimeRequired());
		return output1.getMappedResults();
	}

	public List<DBObject> getIncomeAnalysisOutput(List<String> accountList,
			Date last12MonthsDate) {
		Statistics getIncomeAnalysisOutputStatistics = new Statistics();

		Criteria userIdCriteria = new Criteria("SOURCE_ACCOUNT_NUMBER")
				.in(accountList);
		Criteria incomeCriteria = new Criteria("INCOME_EXPENSE").is("INCOME");
		Criteria dateCriteria = new Criteria("TRAN_DATE_TIME")
				.gte(last12MonthsDate);
		Criteria c3 = new Criteria("CATEGORY").ne("CC Payment Received");
		// Criteria c4 = new Criteria("MERCHANT").ne("ICICI BANK CREDIT CA");
		// Criteria c5 = new Criteria("MERCHANT").ne("ICICI BANK CC PAYMENT");
		Criteria incomeAndOperator = incomeCriteria.andOperator(userIdCriteria,
				dateCriteria, c3);
		MatchOperation incomeAndUserMatch = Aggregation
				.match(incomeAndOperator);

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id.TRAN_YEAR", -1).append("_id.MONTH", -1));

		DBObject incomeAnalysisGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("BUCKET", 1)
						.append("INCOME_EXPENSE", 1)
						.append("TRAN_YEAR", 1)
						.append("CATEGORY", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("TRAN_MON", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));

		DBObject incomeAnalysisGroup2 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("CATEGORY",
						"$BUCKET"

				).append("MONTH", "$AGG_MON").append("TRAN_YEAR", "$AGG_YEAR"))
						.append("income", new BasicDBObject("$sum", "$income")));

		Aggregation incomeAnalysisAggregation = Aggregation.newAggregation(
				incomeAndUserMatch, new CustomGroupOperation(
						incomeAnalysisGroup), new CustomGroupOperation(
						incomeAnalysisGroup2), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> analysisOutput = mongoTemplate.aggregate(
				incomeAnalysisAggregation, "AMAZE_TRANSACTION_TIMELINE",
				DBObject.class);
		// logger.info(" AMAZE- Total time taken for getOverview-getIncomeAnalysisOutput "
		// + getIncomeAnalysisOutputStatistics.totalTimeRequired());
		return analysisOutput.getMappedResults();
	}

	public List<DBObject> getExpenseAnalysisOutput(List<String> accountList,
			Date last12MonthsDate) {
		Statistics getExpenseAnalysisOutputStatistics = new Statistics();

		Criteria userIdCriteria = new Criteria("SOURCE_ACCOUNT_NUMBER")
				.in(accountList);

		Criteria expenseCriteria = new Criteria("INCOME_EXPENSE").is("EXPENSE");
		Criteria dateCriteria = new Criteria("TRAN_DATE_TIME")
				.gte(last12MonthsDate);
		Criteria c3 = new Criteria("CATEGORY").ne("CC Payment Received");
		// Criteria c4 = new Criteria("MERCHANT").ne("ICICI BANK CREDIT CA");
		// Criteria c5 = new Criteria("MERCHANT").ne("ICICI BANK CC PAYMENT");
		Criteria expenseAndOperator = expenseCriteria.andOperator(
				userIdCriteria, dateCriteria, c3);
		MatchOperation expenseAndUserMatch = Aggregation
				.match(expenseAndOperator);

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id.TRAN_YEAR", -1).append("_id.MONTH", -1));

		DBObject expenseAnalysisGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("BUCKET", 1)
						.append("TRAN_YEAR", 1)
						.append("CATEGORY", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("TRAN_MON", 1)
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 })));

		DBObject expenseAnalysisGroup2 = (DBObject) new BasicDBObject(
				"$group",
				new BasicDBObject("_id", new BasicDBObject("CATEGORY",
						"$BUCKET"

				).append("MONTH", "$AGG_MON").append("TRAN_YEAR", "$AGG_YEAR"))
						.append("spends", new BasicDBObject("$sum", "$expense")));

		Aggregation expenseAnalysisAggregation = Aggregation.newAggregation(
				expenseAndUserMatch, new CustomGroupOperation(
						expenseAnalysisGroup), new CustomGroupOperation(
						expenseAnalysisGroup2), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> expenseAnalysisOutput = mongoTemplate
				.aggregate(expenseAnalysisAggregation,
						"AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		// logger.info(" AMAZE- Total time taken for getOverview-getExpenseAnalysisOutput "
		// + getExpenseAnalysisOutputStatistics.totalTimeRequired());
		return expenseAnalysisOutput.getMappedResults();
	}

	public List<DBObject> getIncomeAnalysisOutput_CC(List<String> accountList,
			Date last12MonthsDate) {
		Statistics getIncomeAnalysisOutputStatistics = new Statistics();

		Criteria userIdCriteria = new Criteria("SOURCE_ACCOUNT_NUMBER")
				.in(accountList);

		Criteria incomeCriteria = new Criteria("INCOME_EXPENSE").is("INCOME");
		Criteria dateCriteria = new Criteria("TRAN_DATE_TIME")
				.gte(last12MonthsDate);
		Criteria incomeAndOperator = incomeCriteria.andOperator(userIdCriteria,
				dateCriteria);
		MatchOperation incomeAndUserMatch = Aggregation
				.match(incomeAndOperator);

		DBObject incomeAnalysisGroup = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("INCOME_EXPENSE", 1)
						.append("BUCKET", 1)
						.append("TRAN_YEAR", 1)
						.append("CATEGORY", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("TRAN_MON", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));

		DBObject incomeAnalysisGroup2 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("CATEGORY",
						"$BUCKET"

				).append("MONTH", "$AGG_MON")
						.append("TRAN_YEAR", "$AGG_YEAR")
						.append("SOURCE_ACCOUNT_NUMBER",
								"$SOURCE_ACCOUNT_NUMBER")).append("income",
						new BasicDBObject("$sum", "$income")));

		Aggregation incomeAnalysisAggregation = Aggregation.newAggregation(
				incomeAndUserMatch, new CustomGroupOperation(
						incomeAnalysisGroup), new CustomGroupOperation(
						incomeAnalysisGroup2));
		AggregationResults<DBObject> analysisOutput = mongoTemplate.aggregate(
				incomeAnalysisAggregation, "AMAZE_TRANSACTION_TIMELINE",
				DBObject.class);
		// logger.info(" AMAZE- Total time taken for getCreditCardOverview-getIncomeAnalysisOutputCC "
		// + getIncomeAnalysisOutputStatistics.totalTimeRequired());
		return analysisOutput.getMappedResults();
	}

	public List<DBObject> getExpenseAnalysisOutput_CC(List<String> accountList,
			Date last12MonthsDate) {
		Statistics getExpenseAnalysisOutputStatistics = new Statistics();

		Criteria userIdCriteria = new Criteria("SOURCE_ACCOUNT_NUMBER")
				.in(accountList);

		Criteria expenseCriteria = new Criteria("INCOME_EXPENSE").is("EXPENSE");
		Criteria dateCriteria = new Criteria("TRAN_DATE_TIME")
				.gte(last12MonthsDate);
		Criteria expenseAndOperator = expenseCriteria.andOperator(
				userIdCriteria, dateCriteria);
		MatchOperation expenseAndUserMatch = Aggregation
				.match(expenseAndOperator);

		DBObject expenseAnalysisGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("TRAN_YEAR", 1)
						.append("CATEGORY", 1)
						.append("BUCKET", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("TRAN_MON", 1)
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 })));

		DBObject expenseAnalysisGroup2 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("CATEGORY",
						"$BUCKET"

				).append("MONTH", "$AGG_MON")
						.append("TRAN_YEAR", "$AGG_YEAR")
						.append("SOURCE_ACCOUNT_NUMBER",
								"$SOURCE_ACCOUNT_NUMBER")).append("spends",
						new BasicDBObject("$sum", "$expense")));

		Aggregation expenseAnalysisAggregation = Aggregation.newAggregation(
				expenseAndUserMatch, new CustomGroupOperation(
						expenseAnalysisGroup), new CustomGroupOperation(
						expenseAnalysisGroup2));
		AggregationResults<DBObject> expenseAnalysisOutput = mongoTemplate
				.aggregate(expenseAnalysisAggregation,
						"AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		// logger.info(" AMAZE- Total time taken for getCreditCardOverview-getExpenseAnalysisOutputCC "
		// + getExpenseAnalysisOutputStatistics.totalTimeRequired());
		return expenseAnalysisOutput.getMappedResults();
	}

	public List<DBObject> getPaymentSpendsOverviewByMonthCC(
			String SOURCE_ACCOUNT_NUMBER) {
		Statistics getPaymentSpendsOverviewByMonth = new Statistics();

		List<DBObject> outputResults = new ArrayList<DBObject>();

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id.TRAN_YEAR", -1).append("_id.MONTH", -1));

		Criteria c1 = new Criteria(SOURCE_ACCOUNT_NUMBER);

		MatchOperation matchStage = Aggregation.match(c1);
		DBObject overviewGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("TRAN_YEAR", 1)
						.append("TRAN_MON", 1)
						.append("TRAN_DAY", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("SOURCE_SYSTEM_CODE", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("MONTH", "$AGG_MON")
						.append("TRAN_YEAR", "$AGG_YEAR").append(
								"SOURCE_ACCOUNT_NUMBER",
								"$SOURCE_ACCOUNT_NUMBER")).append("spends",
						new BasicDBObject("$sum", "$expense")).append("income",
						new BasicDBObject("$sum", "$income")));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(overviewGroup),
				new CustomGroupOperation(myGroup1), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);

		// logger.info(" AMAZE- Total time taken for getIncomeExpenseOverviewByMonth "
		// + getPaymentSpendsOverviewByMonth.totalTimeRequired());
		// logger.info(output);
		// logger.info(output.getMappedResults());
		outputResults.addAll(output.getMappedResults());
		// logger.info(outputResults);

		return outputResults;
	}

	public List<DBObject> getSpendingOverviewByMonthYear(String userId) {
		Statistics getSpendingOverviewByMonthYear = new Statistics();
		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("AGG_YEAR", -1).append("AGG_MON", -1));
		Criteria c1 = new Criteria("USER_ID").is(userId);
		MatchOperation matchStage = Aggregation.match(c1);

		DBObject overviewGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("TRAN_YEAR", 1)
						.append("TRAN_MON", 1)
						.append("TRAN_DAY", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "TRAN_DATE_TIME"))
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("MONTH", "$TRAN_MON"

				).append("TRAN_YEAR", "$TRAN_YEAR")).append("spends",
						new BasicDBObject("$sum", "$expense")).append("income",
						new BasicDBObject("$sum", "$income")));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(sortCondition),
				new CustomGroupOperation(overviewGroup),
				new CustomGroupOperation(myGroup1));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		// logger.info(" USERID "+userId+" getIncomeExpenseOverviewByMonth "
		// + getSpendingOverviewByMonthYear.totalTimeRequired());

		return output.getMappedResults();
	}

	public List<DBObject> getIncomeExpenseOverviewByMonth(
			List<String> accountList, Date last12MonthsDate) {
		Statistics getIncomeExpenseOverviewByMonth = new Statistics();

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id.TRAN_YEAR", -1).append("_id.MONTH", -1));

		Criteria c1 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);
		Criteria c2 = new Criteria("TRAN_DATE_TIME").gte(last12MonthsDate);
		Criteria c3 = new Criteria("CATEGORY").ne("CC Payment Received");
		Criteria c4 = new Criteria("MERCHANT").ne("ICICI BANK CREDIT CA");
		Criteria c5 = new Criteria("MERCHANT").ne("ICICI BANK CC PAYMENT");
		MatchOperation matchStage = Aggregation.match(c1.andOperator(c2, c3,
				c4, c5));

		DBObject overviewGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("TRAN_YEAR", 1)
						.append("TRAN_MON", 1)
						.append("TRAN_DAY", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id",
						new BasicDBObject("MONTH", "$AGG_MON").append(
								"TRAN_YEAR", "$AGG_YEAR")).append("spends",
						new BasicDBObject("$sum", "$expense")).append("income",
						new BasicDBObject("$sum", "$income")));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(overviewGroup),
				new CustomGroupOperation(myGroup1), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		// logger.info(" AMAZE- Total time taken for getOverview-getIncomeExpenseOverviewByMonth "
		// + getIncomeExpenseOverviewByMonth.totalTimeRequired());
		// info(output);
		return output.getMappedResults();
	}

	public List<DBObject> getPaymentSpendsOverviewByMonth(
			List<String> accountList, Date last12MonthsDate) {
		Statistics getPaymentSpendsOverviewByMonth = new Statistics();

		List<DBObject> outputResults = new ArrayList<DBObject>();

		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id.TRAN_YEAR", -1).append("_id.MONTH", -1));

		Criteria c1 = new Criteria("SOURCE_ACCOUNT_NUMBER").in(accountList);

		Criteria c2 = new Criteria("TRAN_DATE_TIME").gte(last12MonthsDate);
		MatchOperation matchStage = Aggregation.match(c1.andOperator(c2));
		DBObject overviewGroup = (DBObject) new BasicDBObject(
				"$project",
				new BasicDBObject("AMOUNT", 1)
						.append("USER_ID", 1)
						.append("INCOME_EXPENSE", 1)
						.append("TRAN_YEAR", 1)
						.append("TRAN_MON", 1)
						.append("TRAN_DAY", 1)
						.append("SOURCE_ACCOUNT_NUMBER", 1)
						.append("SOURCE_SYSTEM_CODE", 1)
						.append("TRAN_DATE_TIME", 1)
						.append("AGG_YEAR",
								new BasicDBObject("$year", "$TRAN_DATE_TIME"))
						.append("AGG_MON",
								new BasicDBObject("$month", "$TRAN_DATE_TIME"))
						.append("expense",
								new BasicDBObject(
										"$cond",
										new Object[] {
												new BasicDBObject(
														"$eq",
														new Object[] {
																"$INCOME_EXPENSE",
																"EXPENSE" }),
												"$AMOUNT", 0 }))
						.append("income",
								new BasicDBObject("$cond", new Object[] {
										new BasicDBObject("$eq", new Object[] {
												"$INCOME_EXPENSE", "INCOME" }),
										"$AMOUNT", 0 })));
		DBObject myGroup1 = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", new BasicDBObject("MONTH", "$AGG_MON")
						.append("TRAN_YEAR", "$AGG_YEAR").append(
								"SOURCE_ACCOUNT_NUMBER",
								"$SOURCE_ACCOUNT_NUMBER")).append("spends",
						new BasicDBObject("$sum", "$expense")).append("income",
						new BasicDBObject("$sum", "$income")));

		Aggregation aggregation = Aggregation.newAggregation(matchStage,
				new CustomGroupOperation(overviewGroup),
				new CustomGroupOperation(myGroup1), new CustomGroupOperation(
						sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);

		// logger.info(" AMAZE- Total time taken for getCreditCardOverview-getPaymentSpendsOverviewByMonth "
		// + getPaymentSpendsOverviewByMonth.totalTimeRequired());
		// logger.info(output);
		// logger.info(output.getMappedResults());
		outputResults.addAll(output.getMappedResults());
		// logger.info(outputResults);
		return outputResults;
	}

	public String getMonthByInt(int monthNumber) {
		String monthArray[] = { "JAN", "FEB", "MAR", "APR", "MAY", "JUN",
				"JUL", "AUG", "SEP", "OCT", "NOV", "DEC" };
		return monthArray[monthNumber - 1];
	}

	public List<DBObject> getTransactionsByMongo(String userId) {
		Statistics getTransactionsByMongoStatistics = new Statistics();
		DBObject sortCondition = (DBObject) new BasicDBObject("$sort",
				new BasicDBObject("_id", -1));
		Criteria userIdCriteria = new Criteria("USER_ID").is(userId);
		MatchOperation userIdMatch = Aggregation.match(userIdCriteria);

		DBObject project = (DBObject) new BasicDBObject("$project",
				new BasicDBObject("DayVal", new BasicDBObject("$dateToString",
						new BasicDBObject("format", "%Y-%m-%d").append("date",
								"$TRAN_DATE_TIME")))
						.append("USER_ID", "$USER_ID")
						.append("TRAN_DATE_TIME", "$TRAN_DATE_TIME")
						.append("ACCOUNT_NO", "$ACCOUNT_NO")
						.append("MERCHANT", "$MERCHANT")
						.append("CATEGORY", "$CATEGORY")
						.append("DEBIT_CREDIT", "$DEBIT_CREDIT")
						.append("AMOUNT", "$AMOUNT")
						.append("BUCKET", "$BUCKET")
						.append("INCOME_EXPENSE", "$INCOME_EXPENSE"));

		DBObject group = (DBObject) new BasicDBObject("$group",
				new BasicDBObject("_id", "$DayVal").append(
						"TRANSACTIONS",
						new BasicDBObject("$push", new BasicDBObject("USER_ID",
								"$USER_ID")
								.append("TRAN_DATE_TIME", "$TRAN_DATE_TIME")
								.append("ACCOUNT_NO", "$ACCOUNT_NO")
								.append("MERCHANT", "$MERCHANT")
								.append("CATEGORY", "$CATEGORY")
								.append("DEBIT_CREDIT", "$DEBIT_CREDIT")
								.append("AMOUNT", "$AMOUNT")
								.append("BUCKET", "$BUCKET")
								.append("INCOME_EXPENSE", "$INCOME_EXPENSE"))));
		Aggregation aggregation = Aggregation.newAggregation(userIdMatch,
				new CustomGroupOperation(project), new CustomGroupOperation(
						group), new CustomGroupOperation(sortCondition));
		AggregationResults<DBObject> output = mongoTemplate.aggregate(
				aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		// logger.info(" AMAZE- Total time taken for getTransactionsByMongo "
		// + getTransactionsByMongoStatistics.totalTimeRequired());
		return output.getMappedResults();

	}

	public String getBucket(String category) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AMAZE_CATEGORY_MASTER");
		String bucket = null;

		BasicDBObject field = new BasicDBObject();
		field.put("CATEGORY", category);

		DBCursor cursor = collection.find(field).limit(1);
		while (cursor.hasNext()) {
			BasicDBObject obj = (BasicDBObject) cursor.next();
			bucket = obj.getString("BUCKET");
		}
		// System.out.println("BUCKET FOR CATEGORY "+category+" is"+ bucket);

		return bucket;
	}

	public int updateCategory(String userId, UpdateCategory updateCategory) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db
				.getCollection("AMAZE_TRANSACTION_TIMELINE");
		String bucket = getBucket(updateCategory.getNewCategory());
		BasicDBObject updateQuery = new BasicDBObject();
		long start = System.currentTimeMillis();

		updateQuery.append(
				"$set",
				new BasicDBObject().append("CATEGORY",
						updateCategory.getNewCategory()).append("BUCKET",
						bucket));

		BasicDBObject searchQuery = new BasicDBObject();
		// searchQuery.append("USER_ID", userId);
		searchQuery.append("EVENTID", updateCategory.getEventId());
		searchQuery.append("CATEGORY", updateCategory.getOldCategory());

		DBCursor searchResult = collection.find(searchQuery);

		if (searchResult.count() == 0
				&& collection.find(
						new BasicDBObject().append("EVENTID",
								updateCategory.getEventId()).append("CATEGORY",
								updateCategory.getNewCategory())).count() > 0) {
			return 0;
		} else {
			// logger.info(searchQuery);
			// logger.info(updateQuery);

			WriteResult wrupdate = collection.update(searchQuery, updateQuery,
					false, true);

			if (wrupdate.getN() > 0) {
				DBCollection collection1 = db
						.getCollection("AMAZE_CUST_CATEGORIZATION");
				Boolean value = false;
				// logger.info("inserting document..");
				BasicDBObject document = new BasicDBObject();
				document.put("USER_ID", userId);
				document.put("PROPOSED_CATEGORY",
						updateCategory.getNewCategory());
				document.put("EXISTING_CATEGORY",
						updateCategory.getOldCategory());
				document.put("EVENTID", updateCategory.getEventId());
				Date dateCreated = new Date();
				document.put("DATE_CREATED", dateCreated);
				document.put("PROCESSED", value);

				collection1.insert(document);
				return wrupdate.getN();

			} else {
				long end = System.currentTimeMillis();
				if (updateCategory.getOldCategory() == null
						|| updateCategory.getOldCategory() == "") {
					logger.error("EVENT ID " + updateCategory.getEventId()
							+ " USERID " + userId + " updateCategory E03 "
							+ (end - start)
							+ " old Category to be updated was not set"
							+ updateCategory.getNewCategory()
							+ " was the expected new Category");
				} else if (updateCategory.getNewCategory() == null
						|| updateCategory.getNewCategory() == "") {
					logger.error("EVENT ID "
							+ updateCategory.getEventId()
							+ " USERID "
							+ userId
							+ " updateCategory E03 "
							+ (end - start)
							+ " new Category to which to be updated was not set ; Old Category was set as "
							+ updateCategory.getOldCategory());
				} else {
					logger.error("EVENT ID " + updateCategory.getEventId()
							+ " USERID " + userId + " updateCategory E03 "
							+ (end - start)
							+ " No records updated: Tried to update the "
							+ updateCategory.getOldCategory() + " with "
							+ updateCategory.getNewCategory());
				}
				return -1;
			}
		}
	}

	public void insertIntoCachedCollection(String userId,
			Map<String, Object> dataMap) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AMAZE_CACHED_OVERVIEW");

		// logger.info("inserting document..");
		BasicDBObject document = new BasicDBObject();
		document.put("USER_ID", userId);
		document.put("CACHED_DATA", dataMap);
		Date dateCreated = new Date();
		document.put("DATE_CREATED", dateCreated);
		collection.insert(document);

	}

	public Map<String, Object> getCachedData(String userId) {

		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AMAZE_CACHED_OVERVIEW");

		Calendar c = new GregorianCalendar();
		c.set(Calendar.HOUR_OF_DAY, 0);
		c.set(Calendar.MINUTE, 0);
		c.set(Calendar.SECOND, 0);

		Date todaysDate = c.getTime();

		// System.out.println("todays date" + todaysDate);
		BasicDBObject Query = new BasicDBObject();

		Query.put("USER_ID", userId);
		// System.out.println("user ID = " + userId);
		Query.put("DATE_CREATED", new BasicDBObject("$gte", todaysDate));

		DBCursor cursor = collection.find(Query);
		// System.out.println("cursor ==== " + cursor);
		Map<String, Object> cachedData = null;
		try {
			cachedData = (Map<String, Object>) cursor.next();
		} catch (Exception e) {
			return cachedData;
		}

		return cachedData;
	}

//	public List<DBObject> getIVRMessage(String userId, String mobile, String acc) {
//		Date latestRecordDate = getLatestRecordDate("CALL_CENTER_CUST_PREDICT");
//		Calendar calendar = Calendar.getInstance();
//		calendar.setTime(latestRecordDate);
//
//		calendar.add(Calendar.DAY_OF_MONTH, 1);
//		Criteria andOperator = null;
//		// logger.info("Latest Record Date: " + latestRecordDate);
//		// logger.info("Current Date Time: " + calendar.getTime());
//		if (userId == "" && mobile == "" && acc == "") {
//			logger.info("MOBILE, USERID, ACCOUNT NUMBER not provided");
//		} else if (userId != null && !userId.isEmpty()) {
//			Criteria c1 = new Criteria("USER_ID").is(userId);
//			// Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
//			// Criteria c3 = new
//			// Criteria("DATE_CREATED").lt(calendar.getTime());
//			// andOperator = c1.andOperator(c2, c3);
//			andOperator = c1;
//
//		} else if (mobile != null && !mobile.isEmpty()) {
//			Criteria c1 = new Criteria("REG_MOBILE_NO").is(mobile);
//			// Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
//			// Criteria c3 = new
//			// Criteria("DATE_CREATED").lt(calendar.getTime());
//			// andOperator = c1.andOperator(c2, c3);
//			andOperator = c1;
//		} else if (acc != null && !acc.isEmpty()) {
//			Criteria c1 = new Criteria("ACCOUNT_NO").is(acc);
//			// Criteria c2 = new Criteria("DATE_CREATED").gte(latestRecordDate);
//			// Criteria c3 = new
//			// Criteria("DATE_CREATED").lt(calendar.getTime());
//			// andOperator = c1.andOperator(c2, c3);
//			andOperator = c1;
//		}
//
//		// Date Criteria
//
//		Calendar currentSysDate = Calendar.getInstance();
//		// calendar.add(Calendar.DAY_OF_MONTH, -2);
//		currentSysDate.set(Calendar.HOUR, 00);
//		currentSysDate.set(Calendar.MINUTE, 00);
//		currentSysDate.set(Calendar.SECOND, 00);
//		currentSysDate.set(Calendar.MILLISECOND, 00);
//		Date currentTime = currentSysDate.getTime();
//		Criteria c2 = new Criteria("MIN_EXPECTED_DATE").lte(currentTime);
//		Criteria c3 = new Criteria("MAX_EXPECTED_DATE").gte(currentTime);
//
//		// logger.info(andOperator.getClass());
////		try {
//			MatchOperation matchstage = Aggregation.match(andOperator.andOperator(c2,c3));
//
//			// {project: {"Priority": 1, "param_value": 1, "Account_No": 1,
//			// "User_ID": 1, "Scenario_No": 1, "Message_Text": 1
//			// "Scenario_Name": 1,
//			// "Expected_Date": 1, "Reg_Mobile_No": 1}}
//
//			DBObject myGroup = (DBObject) new BasicDBObject("$project",
//					new BasicDBObject("_id", 0).append("PRIORITY", 1)
//							.append("PARAM_VALUE", 1).append("ACCOUNT_NO", 1)
//							.append("USER_ID", 1).append("SCENARIO_NO", 1)
//							.append("MESSAGE_TEXT", 1)
//							.append("SCENARIO_NAME", 1)
//							.append("EXPECTED_DATE", 1)
//							.append("REG_MOBILE_NO", 1)
//							.append("MIN_EXPECTED_DATE", 1)
//							.append("MAX_EXPECTED_DATE", 1));
//			Aggregation aggregation = Aggregation.newAggregation(matchstage,
//					new CustomGroupOperation(myGroup));
//			AggregationResults<DBObject> output = mongoTemplate.aggregate(
//					aggregation, "CALL_CENTER_CUST_PREDICT", DBObject.class);
//			return output.getMappedResults();
////		} catch (Exception exception) {
////			exception.printStackTrace();
////		}
////		return null;
//	}
	
	
	
	public List<DBObject> getIVRMessage(String userId, String mobile, String acc) {
		Date latestRecordDate = getLatestRecordDate("CALL_CENTER_CUST_PREDICT");
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(latestRecordDate);

		calendar.add(Calendar.DAY_OF_MONTH, 1);

		Calendar now = Calendar.getInstance();
		now.set(Calendar.HOUR, 00);
		now.set(Calendar.MINUTE, 00);
		now.set(Calendar.SECOND, 00);
		now.set(Calendar.MILLISECOND, 000);

		Date currentDate = now.getTime();

		Criteria c1 = new Criteria("USER_ID").is(userId);
		Criteria c2 = new Criteria("REG_MOBILE_NO").is(mobile);
		Criteria c3 = new Criteria("ACCOUNT_NO").is(acc);
		Criteria c4 = new Criteria("MIN_EXPECTED_DATE").lte(currentDate);
		Criteria c5 = new Criteria("MAX_EXPECTED_DATE").gte(currentDate);

		Criteria finalOperator = c4.andOperator(c5).orOperator(c1,c2,c3);

		MatchOperation matchstage = Aggregation.match(finalOperator);

		DBObject myGroup = (DBObject) new BasicDBObject("$project", new BasicDBObject("_id", 0)
		.append("PRIORITY", 1)
		.append("PARAM_VALUE", 1)
		.append("ACCOUNT_NO", 1)
		.append("USER_ID", 1)
		.append("SCENARIO_NO", 1)
		.append("MESSAGE_TEXT", 1)
		.append("SCENARIO_NAME", 1)
		.append("EXPECTED_DATE", 1)
		.append("REG_MOBILE_NO", 1)
		.append("MIN_EXPECTED_DATE", 1)
		.append("MAX_EXPECTED_DATE", 1));
		Aggregation aggregation = Aggregation.newAggregation(matchstage, new CustomGroupOperation(myGroup));
		logger.info(aggregation.toString());
		AggregationResults<DBObject> output = mongoTemplate.aggregate(aggregation, "CALL_CENTER_CUST_PREDICT", DBObject.class);
		return output.getMappedResults();

	}

	public int insertIVRResponse(CallCenterResponse callCenterResponse) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AUDIT_CALL_CENTER_CUST_PREDICTION");
		
		Date dateCreated = new Date();
		
		BasicDBObject document = new BasicDBObject();
		document.put("SCENARIO_NO", callCenterResponse.getScenario_no());
		document.put("SCENARIO_NAME", callCenterResponse.getScenario_name());
		document.put("MOBILE", callCenterResponse.getMobile());
		document.put("ACCOUNT_NO", callCenterResponse.getAcc_no());
		document.put("STATUS", callCenterResponse.getResp_status());
		document.put("DATE_CREATED", dateCreated);
		
		WriteResult writeResult = collection.insert(document);
		
		return writeResult.getN();
		
	}
	

	private String getAccountNo(UpdateCategory updateCategory) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AMAZE_TRANSACTION_TIMELINE");
		
		Criteria c1 = new Criteria("EVENTID").is(updateCategory.getEventId());
		Criteria c2 = new Criteria("SOURCE_SYSTEM_CODE").is("30");
		
		MatchOperation matchstage = Aggregation.match(c1.andOperator(c2));
		
		DBObject project = new BasicDBObject("$project", new BasicDBObject("SOURCE_ACCOUNT_NUMBER", 1).append("_id", 0));
		
		Aggregation aggregation = Aggregation.newAggregation(matchstage, new CustomGroupOperation(project));
		
		AggregationResults<DBObject> output = mongoTemplate.aggregate(aggregation, "AMAZE_TRANSACTION_TIMELINE", DBObject.class);
		
		String source_acc_no = output.getMappedResults().get(0).get("SOURCE_ACCOUNT_NUMBER").toString();
		
		// TODO Auto-generated method stub
		return source_acc_no;
	}

	public int insertData(String uSERID, UpdateCategory updateCategory) {
		DB db = mongoTemplate.getDb();
		DBCollection collection = db.getCollection("AMAZE_CC_AUDIT");
		
		String getACCNo = getAccountNo(updateCategory);
		Date datecreated = new Date();
		
		BasicDBObject insertdata = new BasicDBObject();
		insertdata.put("USERID", uSERID);
		insertdata.put("CATEGORY", updateCategory.getNewCategory());
		insertdata.put("SOURCE_ACCOUNT_NUMBER", getACCNo);
		insertdata.put("CREATED_DATE", datecreated);
		
		WriteResult writeResult = collection.insert(insertdata);
		
		return writeResult.getN();

	}
	

}
