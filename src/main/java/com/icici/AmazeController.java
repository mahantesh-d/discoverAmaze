package com.icici;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.net.SocketTimeoutException;
import java.text.SimpleDateFormat;
import java.util.*;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.icici.model.CallCenterResponse;
import com.icici.model.TransactionTimeline;
import com.icici.model.UpdateCategory;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.MongoTimeoutException;
import com.icici.CustomException;

@RestController
@RequestMapping("/api")
public class AmazeController {

	private static final Logger logger = Logger
			.getLogger(AmazeController.class);

	@Autowired
	private AmazeService amazeService;

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"dd MMM yy");

	private int RetryCount;
	@Value("${server_ip}")
	private String serverIp;

	@RequestMapping(value = "/getDashboard", method = RequestMethod.GET)
	public Map<String, Object> getDashboard(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber) {
		Boolean ifError = false;
		Statistics dashboardStatistics = new Statistics();

		USERID = USERID.toUpperCase();
		Map<String, Object> dashboardMap = new LinkedHashMap<>();
		dashboardMap.put("USERID", USERID);
		dashboardMap.put("RRN", RRN);
		dashboardMap.put("CHANNEL", CHANNEL);
		dashboardMap.put("MOBILE", MOBILE);
		try {
			Double spendingSumForCurrentMonth = amazeService
					.getSpendingSumForCurrentMonth(USERID);

			spendingSumForCurrentMonth = BigDecimal
					.valueOf(spendingSumForCurrentMonth)
					.setScale(0, RoundingMode.HALF_UP).doubleValue();
			dashboardMap.put("spendingSumOverview",
					spendingSumForCurrentMonth.longValue());

			List<DBObject> upcomingTransactions = amazeService
					.getUpcomingTransaction(USERID);

			Double amount = 0.0;
			for (DBObject upcomingTransaction : upcomingTransactions) {
				Object dueAmount = upcomingTransaction.get("DUE_AMT");
				if (dueAmount != null) {
					amount = amount + Double.valueOf(dueAmount.toString());
				}
			}
			amount = BigDecimal.valueOf(amount)
					.setScale(0, RoundingMode.HALF_UP).doubleValue();
			dashboardMap.put("upcomingTransaction", amount.longValue());
			dashboardMap.put("upcomingTransactionCount",
					upcomingTransactions.size());

			List<DBObject> serviceRequestOverview = amazeService
					.getServiceRequestOverview(USERID);
			
			
			
			dashboardMap.put("serviceRequestOverview", serviceRequestOverview);

			long recommendationsCount = amazeService
					.getRecommendationsCount(USERID);
			dashboardMap.put("recommendationsCount", recommendationsCount);
			dashboardMap.put("STATUS", 200);

		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getDashboard E01 "
						+ dashboardStatistics.totalTimeRequired());
				ifError = true;
				dashboardMap.put("STATUS", 500);
				dashboardMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getDashboard E00 "
						+ dashboardStatistics.totalTimeRequired());
				getDashboard(USERID, RRN, MOBILE, CHANNEL, 1);
			}

		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {

					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getDashboard E01 "
								+ dashboardStatistics.totalTimeRequired());
						ifError = true;
						dashboardMap.put("STATUS", 500);
						dashboardMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getDashboard E00 "
								+ dashboardStatistics.totalTimeRequired());
						getDashboard(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {

					dashboardMap.put("STATUS", 500);
					ifError = true;
				}

			} catch (Exception e) {
				dashboardMap.put("STATUS", 500);
				ifError = true;

			}

		}
		if (ifError == false) {
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getDashboard " + "Success "
					+ dashboardStatistics.totalTimeRequired());
		}

		return dashboardMap;
	}

	@RequestMapping("/getOverviewOld")
	public Map<String, Object> getOverviewOld(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber)
			throws CustomException, SocketTimeoutException {

		Statistics overviewStatisticsList = new Statistics();

		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		String keyValueYearMonth = "";
		try {

			List<DBObject> totalBudget = amazeService.getTotalBudget(USERID);
			Map<String, Map<String, Object>> overviewFinalObj = new LinkedHashMap<>();
			dataMap.put("STATUS", 200);
			Object totalCalculatedBugetAmount = totalBudget != null
					&& totalBudget.size() > 0 ? totalBudget.get(0)
					.get("budget") : 0;
			Double totalCalculatedBugetAmountInDouble = Double
					.valueOf(totalCalculatedBugetAmount.toString());
			double totalCalculatedBugetAmountRounding = BigDecimal
					.valueOf(totalCalculatedBugetAmountInDouble)
					.setScale(3, RoundingMode.HALF_UP).doubleValue();
			dataMap.put("TotalBudgetAmount", totalCalculatedBugetAmountRounding);

			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();

			List<String> accounts = amazeService.getAccounts(USERID);
			List<DBObject> overviewList = amazeService
					.getIncomeExpenseOverviewByMonth(accounts, last12MonthsDate);
			List<DBObject> budgetList = amazeService.getBugetMaster(USERID);
			List<DBObject> expenseAnalysisOutputList = amazeService
					.getExpenseAnalysisOutput(accounts, last12MonthsDate);
			List<DBObject> incomeAnalysisDBObjects = amazeService
					.getIncomeAnalysisOutput(accounts, last12MonthsDate);

			Map<String, ArrayList<DBObject>> overviewIncomeMap = getIncomeOverviewMap(incomeAnalysisDBObjects);
			Map<String, ArrayList<DBObject>> overviewSpendMap = getExpenseOverviewMap(expenseAnalysisOutputList);

			for (DBObject overviewObject : overviewList) {
				Object yearObject = overviewObject.get("TRAN_YEAR");
				Object monthObject = overviewObject.get("MONTH");
				keyValueYearMonth = ""
						+ amazeService.getMonthByInt(Integer
								.parseInt(monthObject.toString())) + " "
						+ yearObject.toString().substring(2, 4);

				Map<String, Object> incomeAmountObj = new LinkedHashMap<>();
				Map<String, Object> spendsAmountObj = new LinkedHashMap<>();
				Map<String, Object> incomePercentObj = new LinkedHashMap<>();
				Map<String, Object> spendsPercentObj = new LinkedHashMap<>();

				Double incomeDouble = Double.valueOf(overviewObject.get(
						"income").toString());

				incomeDouble = BigDecimal.valueOf(incomeDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();
				incomeAmountObj.put("Income amount", incomeDouble);

				Double spendsDouble = Double.valueOf(overviewObject.get(
						"spends").toString());

				spendsDouble = BigDecimal.valueOf(spendsDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				Double total = incomeDouble + spendsDouble;

				Double percentSpends = (spendsDouble / total) * 100;
				Double percentIncome = (incomeDouble / total) * 100;

				percentSpends = BigDecimal.valueOf(percentSpends)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				percentIncome = BigDecimal.valueOf(percentIncome)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				incomeAmountObj.put("Income amount", incomeDouble);
				spendsAmountObj.put("Spends amount", spendsDouble);
				incomePercentObj.put("Income percentage", percentIncome);
				spendsPercentObj.put("Spends percentage", percentSpends);

				Map<String, Object> innerObjIncome = new LinkedHashMap<>();
				Map<String, Object> outerObjIncome = new LinkedHashMap<>();

				String keyValueYearMonth1 = keyValueYearMonth.toLowerCase();
				ArrayList<DBObject> incomeListObjList = overviewIncomeMap
						.get(keyValueYearMonth1);
				if (incomeListObjList != null) {
					if (incomeListObjList.size() > 0) {
						for (DBObject incomeListObj : incomeListObjList) {

							Map<String, Object> intermediate = new LinkedHashMap<>();
							intermediate.put("amount", Double
									.valueOf(incomeListObj.get("income")
											.toString()));
							innerObjIncome.put(
									(String) incomeListObj.get("CATEGORY"),
									intermediate);
						}
					}
				}

				outerObjIncome.put("income", innerObjIncome);
				Map<String, Object> innerObjSpends = new LinkedHashMap<>();
				Map<String, Object> outerObjSpends = new LinkedHashMap<>();

				ArrayList<DBObject> expenseListObjList = overviewSpendMap
						.get(keyValueYearMonth1);
				if (expenseListObjList != null) {
					if (expenseListObjList.size() > 0) {
						for (DBObject expenseListObj : expenseListObjList) {
							Double BudgetAmountValue = 0.0;
							Double spendsValue = (Double) expenseListObj
									.get("spends");
							String categoryValue = (String) expenseListObj
									.get("CATEGORY");
							for (DBObject categoryListObj : budgetList) {
								String categoryfrombudgetlist = (String) categoryListObj
										.get("BUCKET");

								if (categoryfrombudgetlist
										.equalsIgnoreCase(categoryValue)) {

									if (categoryListObj.get("BUDGET_AMOUNT") == null) {
										BudgetAmountValue = 0.0;
									} else {
										BudgetAmountValue = Double
												.valueOf(categoryListObj.get(
														"BUDGET_AMOUNT")
														.toString());
									}
								}
							}

							Boolean boolValue = true;
							if (BudgetAmountValue == null) {
								BudgetAmountValue = 0.0;
							}
							if (spendsValue > BudgetAmountValue) {
								boolValue = true;
							} else {
								boolValue = false;
							}
							Map<String, Object> amountData = new LinkedHashMap<>();
							amountData.put("amount", spendsValue);
							amountData.put("budget", BudgetAmountValue);
							amountData.put("isOverbudget", boolValue);
							innerObjSpends.put(categoryValue, amountData);
						}
					}
				}

				outerObjSpends.put("spends", innerObjSpends);
				Map<String, Object> overviewPreData = new LinkedHashMap<>();
				overviewPreData.put("Income amount", incomeDouble);
				overviewPreData.put("Income percentage", percentIncome);
				overviewPreData.put("Spends amount", spendsDouble);
				overviewPreData.put("Spends percentage", percentSpends);
				overviewPreData.put("income", innerObjIncome);
				overviewPreData.put("spends", innerObjSpends);
				overviewFinalObj.put(keyValueYearMonth, overviewPreData);
				if (overviewPreData.size() == 0)
					throw new CustomException("E02");
				dataMap.put("Spending Overview", overviewFinalObj);

			}
			DBObject object = amazeService.getTillDate(USERID);
			if (object == null)
				throw new CustomException("E02");
			String tillDate = null;
			if (object != null) {
				tillDate = simpleDateFormat.format(object.get("MAX_TRAN_DATE"));
			}
			dataMap.put("tillDate", tillDate);

		} catch (MongoTimeoutException exception) {

			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverviewOld E01 ");
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverviewOld E00 ");
				getOverviewOld(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getOverviewOld E01 ");
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getOverviewOld E00 ");
						getOverviewOld(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
			}
		}
		if ((int) dataMap.get("STATUS") == 500)
			dataMap.remove("TotalBudgetAmount");
		logger.info(" AMAZE - 1total time taken::"
				+ overviewStatisticsList.totalTimeRequired());
		return dataMap;

	}

	@RequestMapping(value = "/getOverview", method = RequestMethod.GET)
	public Map<String, Object> getOverview(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber)
			throws CustomException, SocketTimeoutException {

		Statistics overviewStatisticsList = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		String keyValueYearMonth = "";
		try {

			List<DBObject> totalBudget = amazeService.getTotalBudget(USERID);
			Map<String, Map<String, Object>> overviewFinalObj = new LinkedHashMap<>();
			dataMap.put("STATUS", 200);

			Object totalCalculatedBugetAmount = totalBudget != null
					&& totalBudget.size() > 0 ? totalBudget.get(0)
					.get("budget") : 0;
			Double totalCalculatedBugetAmountInDouble = Double
					.valueOf(totalCalculatedBugetAmount.toString());
			double totalCalculatedBugetAmountRounding = BigDecimal
					.valueOf(totalCalculatedBugetAmountInDouble)
					.setScale(3, RoundingMode.HALF_UP).doubleValue();
			dataMap.put("TotalBudgetAmount", totalCalculatedBugetAmountRounding);

			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();

			List<String> accounts = amazeService.getAccounts(USERID);
			List<DBObject> overviewList = amazeService
					.getIncomeExpenseOverviewByMonth(accounts, last12MonthsDate);
			List<DBObject> budgetList = amazeService.getBugetMaster(USERID);
			List<DBObject> expenseAnalysisOutputList = amazeService
					.getExpenseAnalysisOutput(accounts, last12MonthsDate);
			List<DBObject> incomeAnalysisDBObjects = amazeService
					.getIncomeAnalysisOutput(accounts, last12MonthsDate);

			Map<String, ArrayList<DBObject>> overviewIncomeMap = getIncomeOverviewMap(incomeAnalysisDBObjects);
			Map<String, ArrayList<DBObject>> overviewSpendMap = getExpenseOverviewMap(expenseAnalysisOutputList);
			Map<String, DBObject> budgetMap = getBudgetMap(budgetList);

			for (DBObject overviewObject : overviewList) {
				Object yearObject = overviewObject.get("TRAN_YEAR");
				Object monthObject = overviewObject.get("MONTH");
				keyValueYearMonth = ""
						+ amazeService.getMonthByInt(Integer
								.parseInt(monthObject.toString())) + " "
						+ yearObject.toString().substring(2, 4);

				Map<String, Object> incomeAmountObj = new LinkedHashMap<>();
				Map<String, Object> spendsAmountObj = new LinkedHashMap<>();
				Map<String, Object> incomePercentObj = new LinkedHashMap<>();
				Map<String, Object> spendsPercentObj = new LinkedHashMap<>();

				Double incomeDouble = Double.valueOf(overviewObject.get(
						"income").toString());

				incomeDouble = BigDecimal.valueOf(incomeDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();
				incomeAmountObj.put("Income amount", incomeDouble);

				Double spendsDouble = Double.valueOf(overviewObject.get(
						"spends").toString());

				spendsDouble = BigDecimal.valueOf(spendsDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				Double total = incomeDouble + spendsDouble;

				Double percentSpends = (spendsDouble / total) * 100;
				Double percentIncome = (incomeDouble / total) * 100;

				percentSpends = BigDecimal.valueOf(percentSpends)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				percentIncome = BigDecimal.valueOf(percentIncome)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				incomeAmountObj.put("Income amount", incomeDouble);
				spendsAmountObj.put("Spends amount", spendsDouble);
				incomePercentObj.put("Income percentage", percentIncome);
				spendsPercentObj.put("Spends percentage", percentSpends);

				Map<String, Object> innerObjIncome = new LinkedHashMap<>();
				Map<String, Object> outerObjIncome = new LinkedHashMap<>();

				String keyValueYearMonth1 = keyValueYearMonth.toLowerCase();
				ArrayList<DBObject> incomeListObjList = overviewIncomeMap
						.get(keyValueYearMonth1);
				if (incomeListObjList != null) {
					if (incomeListObjList.size() > 0) {
						for (DBObject incomeListObj : incomeListObjList) {

							Map<String, Object> intermediate = new LinkedHashMap<>();
							intermediate.put("amount", Double
									.valueOf(incomeListObj.get("income")
											.toString()));
							innerObjIncome.put(
									(String) incomeListObj.get("CATEGORY"),
									intermediate);
						}
					}
				}

				outerObjIncome.put("income", innerObjIncome);
				Map<String, Object> innerObjSpends = new LinkedHashMap<>();
				Map<String, Object> outerObjSpends = new LinkedHashMap<>();
				boolean hasNoCategory = false;
				ArrayList<DBObject> expenseListObjList = overviewSpendMap
						.get(keyValueYearMonth1);
				if (expenseListObjList != null) {
					if (expenseListObjList.size() > 0) {
						for (DBObject expenseListObj : expenseListObjList) {
							Double BudgetAmountValue = 0.0;
							Double spendsValue = (Double) expenseListObj
									.get("spends");
							String categoryValue = (String) expenseListObj
									.get("CATEGORY");
							DBObject categoryListObj = null;
							if (categoryValue != null) {
								categoryListObj = budgetMap.get(categoryValue
										.toLowerCase());
							} else {
								hasNoCategory = true;
							}
							if (categoryListObj != null) {
								if (hasNoCategory) {
									BudgetAmountValue = 0.0;
								} else {
									if (categoryListObj.get("BUDGET_AMOUNT") == null) {
										BudgetAmountValue = 0.0;
									} else {
										BudgetAmountValue = Double
												.valueOf(categoryListObj.get(
														"BUDGET_AMOUNT")
														.toString());
									}
								}
							}

							Boolean boolValue = true;
							if (BudgetAmountValue == null) {
								BudgetAmountValue = 0.0;
							}
							if (spendsValue > BudgetAmountValue) {
								boolValue = true;
							} else {
								boolValue = false;
							}
							Map<String, Object> amountData = new LinkedHashMap<>();
							amountData.put("amount", spendsValue);
							amountData.put("budget", BudgetAmountValue);
							amountData.put("isOverbudget", boolValue);
							if (categoryValue != null) {

								innerObjSpends.put(categoryValue, amountData);
							} else {

								innerObjSpends.put("No Category", amountData);
							}
						}
					}
				}

				outerObjSpends.put("spends", innerObjSpends);
				Map<String, Object> overviewPreData = new LinkedHashMap<>();
				overviewPreData.put("Income amount", incomeDouble);
				overviewPreData.put("Income percentage", percentIncome);
				overviewPreData.put("Spends amount", spendsDouble);
				overviewPreData.put("Spends percentage", percentSpends);
				overviewPreData.put("income", innerObjIncome);
				overviewPreData.put("spends", innerObjSpends);
				overviewFinalObj.put(keyValueYearMonth, overviewPreData);
				if (overviewPreData.size() == 0) {
					throw new CustomException("E02");
				}
				dataMap.put("Spending Overview", overviewFinalObj);
			}
			DBObject object = amazeService.getTillDate(USERID);
			if (object == null) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview E02 "
						+ overviewStatisticsList.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			String tillDate = null;
			if (object != null) {
				tillDate = simpleDateFormat.format(object.get("MAX_TRAN_DATE"));
			}
			dataMap.put("tillDate", tillDate);

		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview E01 "
						+ overviewStatisticsList.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview E00 ");
				getOverview(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			logger.error("Error for operation", S);
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			logger.error("Error for operation", exception);
			try {
				logger.error("getOverview EXCEPTION CAUSE"
						+ exception.getCause());
				logger.error("getOverview EXCEPTION CAUSE of CAUSE"
						+ exception.getCause().getCause());
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview E01 "
								+ overviewStatisticsList.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview E00 ");
						getOverview(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
					logger.error("Error Occured in Logs", exception.getCause());
				}

			} catch (Exception e) {
				logger.error(e);
				dataMap.put("STATUS", 500);
				ifError = true;

			}
		}
		if ((int) dataMap.get("STATUS") == 500)
			dataMap.remove("TotalBudgetAmount");
		if (ifError == false) {
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview " + "Success "
					+ overviewStatisticsList.totalTimeRequired());
		}
		return dataMap;
	}

	private Map<String, DBObject> getBudgetMap(List<DBObject> budgetList) {
		Map<String, DBObject> budgetMap = new LinkedHashMap<String, DBObject>();
		for (DBObject dbObject : budgetList) {
			String category = (String) dbObject.get("BUCKET");
			budgetMap.put(category.toLowerCase(), dbObject);
		}
		return budgetMap;
	}

	private Map<String, ArrayList<DBObject>> getIncomeOverviewMap(
			List<DBObject> incomeAnalysisDBObjects) {
		Map<String, ArrayList<DBObject>> map = new LinkedHashMap<String, ArrayList<DBObject>>();
		for (DBObject incomeListObj : incomeAnalysisDBObjects) {
			Object yearObjectinc = incomeListObj.get("TRAN_YEAR");
			Object monthObjectinc = incomeListObj.get("MONTH");
			String keyValueYearMonthinc = ""
					+ amazeService.getMonthByInt(Integer
							.parseInt(monthObjectinc.toString())) + " "
					+ yearObjectinc.toString().substring(2, 4);

			keyValueYearMonthinc = keyValueYearMonthinc.toLowerCase();
			List<DBObject> dbList = map.get(keyValueYearMonthinc);

			if (dbList != null && dbList.size() > 0) {
				map.get(keyValueYearMonthinc).add(incomeListObj);
			} else {
				map.put(keyValueYearMonthinc, new ArrayList<DBObject>());
				map.get(keyValueYearMonthinc).add(incomeListObj);
			}
		}
		return map;
	}

	private Map<String, ArrayList<DBObject>> getExpenseOverviewMap(
			List<DBObject> expenseAnalysisDBObjects) {
		Map<String, ArrayList<DBObject>> map = new LinkedHashMap<String, ArrayList<DBObject>>();
		for (DBObject expenseListObj : expenseAnalysisDBObjects) {
			Object yearObjectinc = expenseListObj.get("TRAN_YEAR");
			Object monthObjectinc = expenseListObj.get("MONTH");
			String keyValueYearMonthinc = ""
					+ amazeService.getMonthByInt(Integer
							.parseInt(monthObjectinc.toString())) + " "
					+ yearObjectinc.toString().substring(2, 4);
			keyValueYearMonthinc = keyValueYearMonthinc.toLowerCase();
			List<DBObject> dbList = map.get(keyValueYearMonthinc);

			if (dbList != null && dbList.size() > 0) {
				map.get(keyValueYearMonthinc).add(expenseListObj);
			} else {
				map.put(keyValueYearMonthinc, new ArrayList<DBObject>());
				map.get(keyValueYearMonthinc).add(expenseListObj);
			}
		}
		return map;
	}

	@RequestMapping("/getOverview1")
	@SuppressWarnings("unchecked")
	public Map<String, Object> getOverview1(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber)
			throws CustomException, SocketTimeoutException {

		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		Map<String, Object> dataMap1 = amazeService.getCachedData(USERID);
		if (dataMap1 != null && dataMap1.size() > 0) {
			return (Map<String, Object>) dataMap1.get("CACHED_DATA");
		}
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		String keyValueYearMonth = "";
		try {

			List<DBObject> totalBudget = amazeService.getTotalBudget(USERID);
			Map<String, Map<String, Object>> overviewFinalObj = new LinkedHashMap<>();
			dataMap.put("STATUS", 200);
			Object totalCalculatedBugetAmount = totalBudget != null
					&& totalBudget.size() > 0 ? totalBudget.get(0)
					.get("budget") : 0;
			Double totalCalculatedBugetAmountInDouble = Double
					.valueOf(totalCalculatedBugetAmount.toString());
			double totalCalculatedBugetAmountRounding = BigDecimal
					.valueOf(totalCalculatedBugetAmountInDouble)
					.setScale(3, RoundingMode.HALF_UP).doubleValue();
			dataMap.put("TotalBudgetAmount", totalCalculatedBugetAmountRounding);

			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();

			List<String> accounts = amazeService.getAccounts(USERID);
			List<DBObject> overviewList = amazeService
					.getIncomeExpenseOverviewByMonth(accounts, last12MonthsDate);
			List<DBObject> budgetList = amazeService.getBugetMaster(USERID);
			List<DBObject> expenseAnalysisOutputList = amazeService
					.getExpenseAnalysisOutput(accounts, last12MonthsDate);
			List<DBObject> incomeAnalysisDBObjects = amazeService
					.getIncomeAnalysisOutput(accounts, last12MonthsDate);

			Map<String, ArrayList<DBObject>> overviewIncomeMap = getIncomeOverviewMapOld(incomeAnalysisDBObjects);
			Map<String, ArrayList<DBObject>> overviewSpendMap = getExpenseOverviewMapOld(expenseAnalysisOutputList);

			for (DBObject overviewObject : overviewList) {
				Object yearObject = overviewObject.get("TRAN_YEAR");
				Object monthObject = overviewObject.get("MONTH");
				keyValueYearMonth = ""
						+ amazeService.getMonthByInt(Integer
								.parseInt(monthObject.toString())) + " "
						+ yearObject.toString().substring(2, 4);

				Map<String, Object> incomeAmountObj = new LinkedHashMap<>();
				Map<String, Object> spendsAmountObj = new LinkedHashMap<>();
				Map<String, Object> incomePercentObj = new LinkedHashMap<>();
				Map<String, Object> spendsPercentObj = new LinkedHashMap<>();

				Double incomeDouble = Double.valueOf(overviewObject.get(
						"income").toString());

				incomeDouble = BigDecimal.valueOf(incomeDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();
				incomeAmountObj.put("Income amount", incomeDouble);

				Double spendsDouble = Double.valueOf(overviewObject.get(
						"spends").toString());

				spendsDouble = BigDecimal.valueOf(spendsDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				Double total = incomeDouble + spendsDouble;

				Double percentSpends = (spendsDouble / total) * 100;
				Double percentIncome = (incomeDouble / total) * 100;

				percentSpends = BigDecimal.valueOf(percentSpends)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				percentIncome = BigDecimal.valueOf(percentIncome)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				incomeAmountObj.put("Income amount", incomeDouble);
				spendsAmountObj.put("Spends amount", spendsDouble);
				incomePercentObj.put("Income percentage", percentIncome);
				spendsPercentObj.put("Spends percentage", percentSpends);

				Map<String, Object> innerObjIncome = new LinkedHashMap<>();
				Map<String, Object> outerObjIncome = new LinkedHashMap<>();

				String keyValueYearMonth1 = keyValueYearMonth.toLowerCase();
				ArrayList<DBObject> incomeListObjList = overviewIncomeMap
						.get(keyValueYearMonth1);
				if (incomeListObjList != null) {
					if (incomeListObjList.size() > 0) {
						for (DBObject incomeListObj : incomeListObjList) {

							Map<String, Object> intermediate = new LinkedHashMap<>();
							intermediate.put("amount", Double
									.valueOf(incomeListObj.get("income")
											.toString()));
							innerObjIncome.put(
									(String) incomeListObj.get("CATEGORY"),
									intermediate);
						}
					}
				}
				outerObjIncome.put("income", innerObjIncome);
				Map<String, Object> innerObjSpends = new LinkedHashMap<>();
				Map<String, Object> outerObjSpends = new LinkedHashMap<>();

				ArrayList<DBObject> expenseListObjList = overviewSpendMap
						.get(keyValueYearMonth1);
				if (expenseListObjList != null) {
					if (expenseListObjList.size() > 0) {
						for (DBObject expenseListObj : expenseListObjList) {
							Double BudgetAmountValue = 0.0;
							Double spendsValue = (Double) expenseListObj
									.get("spends");
							String categoryValue = (String) expenseListObj
									.get("CATEGORY");
							for (DBObject categoryListObj : budgetList) {
								String categoryfrombudgetlist = (String) categoryListObj
										.get("BUCKET");
								if (categoryfrombudgetlist
										.equalsIgnoreCase(categoryValue)) {

									if (categoryListObj.get("BUDGET_AMOUNT") == null) {
										BudgetAmountValue = 0.0;
									} else {
										BudgetAmountValue = Double
												.valueOf(categoryListObj.get(
														"BUDGET_AMOUNT")
														.toString());
									}
								}
							}
							Boolean boolValue = true;
							if (BudgetAmountValue == null) {
								BudgetAmountValue = 0.0;
							}
							if (spendsValue > BudgetAmountValue) {
								boolValue = true;
							} else {
								boolValue = false;
							}
							Map<String, Object> amountData = new LinkedHashMap<>();
							amountData.put("amount", spendsValue);
							amountData.put("budget", BudgetAmountValue);
							amountData.put("isOverbudget", boolValue);
							innerObjSpends.put(categoryValue, amountData);
						}
					}
				}

				outerObjSpends.put("spends", innerObjSpends);
				Map<String, Object> overviewPreData = new LinkedHashMap<>();
				overviewPreData.put("Income amount", incomeDouble);
				overviewPreData.put("Income percentage", percentIncome);
				overviewPreData.put("Spends amount", spendsDouble);
				overviewPreData.put("Spends percentage", percentSpends);
				overviewPreData.put("income", innerObjIncome);
				overviewPreData.put("spends", innerObjSpends);
				overviewFinalObj.put(keyValueYearMonth, overviewPreData);
				if (overviewPreData.size() == 0)
					throw new CustomException("E02");
				dataMap.put("Spending Overview", overviewFinalObj);

			}

			DBObject object = amazeService.getTillDate(USERID);
			if (object == null)
				throw new CustomException("E02");
			String tillDate = null;
			if (object != null) {
				tillDate = simpleDateFormat.format(object.get("MAX_TRAN_DATE"));
			}
			dataMap.put("tillDate", tillDate);

		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview1 E01 ");
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview1 E00 ");
				getOverview1(USERID, RRN, MOBILE, CHANNEL, 1);
			}
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", "E01");
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview1 E01 ");
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getOverview1 E00 ");
						getOverview1(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
				}

			} catch (Exception e) {
				logger.error(e);
				dataMap.put("STATUS", 500);

			}
		}
		if ((int) dataMap.get("STATUS") == 500)
			dataMap.remove("TotalBudgetAmount");
		if ((int) dataMap.get("STATUS") != 500)
			amazeService.insertIntoCachedCollection(USERID, dataMap);
		return dataMap;
	}

	private Map<String, ArrayList<DBObject>> getIncomeOverviewMapOld(
			List<DBObject> incomeAnalysisDBObjects) {
		Map<String, ArrayList<DBObject>> map = new LinkedHashMap<String, ArrayList<DBObject>>();
		for (DBObject incomeListObj : incomeAnalysisDBObjects) {
			Object yearObjectinc = incomeListObj.get("TRAN_YEAR");
			Object monthObjectinc = incomeListObj.get("MONTH");
			String keyValueYearMonthinc = ""
					+ amazeService.getMonthByInt(Integer
							.parseInt(monthObjectinc.toString())) + " "
					+ yearObjectinc.toString().substring(2, 4);
			keyValueYearMonthinc = keyValueYearMonthinc.toLowerCase();
			List<DBObject> dbList = map.get(keyValueYearMonthinc);

			if (dbList != null && dbList.size() > 0) {
				map.get(keyValueYearMonthinc).add(incomeListObj);
			} else {
				map.put(keyValueYearMonthinc, new ArrayList<DBObject>());
				map.get(keyValueYearMonthinc).add(incomeListObj);
			}
		}
		return map;
	}

	private Map<String, ArrayList<DBObject>> getExpenseOverviewMapOld(
			List<DBObject> expenseAnalysisDBObjects) {
		Statistics expenseOverviewMapStats = new Statistics();
		Map<String, ArrayList<DBObject>> map = new LinkedHashMap<String, ArrayList<DBObject>>();
		for (DBObject expenseListObj : expenseAnalysisDBObjects) {
			Object yearObjectinc = expenseListObj.get("TRAN_YEAR");
			Object monthObjectinc = expenseListObj.get("MONTH");
			String keyValueYearMonthinc = ""
					+ amazeService.getMonthByInt(Integer
							.parseInt(monthObjectinc.toString())) + " "
					+ yearObjectinc.toString().substring(2, 4);
			keyValueYearMonthinc = keyValueYearMonthinc.toLowerCase();
			List<DBObject> dbList = map.get(keyValueYearMonthinc);

			if (dbList != null && dbList.size() > 0) {
				map.get(keyValueYearMonthinc).add(expenseListObj);
			} else {
				map.put(keyValueYearMonthinc, new ArrayList<DBObject>());
				map.get(keyValueYearMonthinc).add(expenseListObj);
			}
		}
		return map;
	}

	private Map<String, ArrayList<DBObject>> getIncomeOverviewMapper(
			List<DBObject> incomeAnalysisDBObjects) {
		Map<String, ArrayList<DBObject>> map = new LinkedHashMap<String, ArrayList<DBObject>>();
		for (DBObject incomeListObj : incomeAnalysisDBObjects) {
			Object yearObjectinc = incomeListObj.get("TRAN_YEAR");
			Object monthObjectinc = incomeListObj.get("MONTH");
			String keyValueYearMonthinc = ""
					+ amazeService.getMonthByInt(Integer
							.parseInt(monthObjectinc.toString())) + " "
					+ yearObjectinc.toString().substring(2, 4);
			keyValueYearMonthinc = keyValueYearMonthinc.toLowerCase();
			List<DBObject> dbList = map.get(keyValueYearMonthinc);
			Map<String, Object> intermediate = new LinkedHashMap<>();
			intermediate.put("amount",
					Double.valueOf(incomeListObj.get("income").toString()));

			Map<String, Object> test = new LinkedHashMap<>();
			test.put((String) incomeListObj.get("CATEGORY"), intermediate);

			if (dbList != null && dbList.size() > 0) {
				map.get(keyValueYearMonthinc).add((DBObject) test);
			} else {
				map.put(keyValueYearMonthinc, new ArrayList<DBObject>());
				map.get(keyValueYearMonthinc).add((DBObject) test);
			}
		}
		return map;
	}

	@RequestMapping(value = "/getCreditCardOverview", method = RequestMethod.GET)
	public Map<String, Object> getSpendAnalysisCreditCard(
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {

		Statistics ccoverviewStatistics = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		String keyValueYearMonth = "";
		try {
			dataMap.put("STATUS", 200);

			String SOURCE_SYSTEM_CODE = "20";
			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();

			List<String> accountList = amazeService.getAccountsCC(USERID,
					SOURCE_SYSTEM_CODE);
			List<DBObject> overviewList = amazeService
					.getPaymentSpendsOverviewByMonth(accountList,
							last12MonthsDate);

			List<DBObject> expenseAnalysisOutputList = amazeService
					.getExpenseAnalysisOutput_CC(accountList, last12MonthsDate);
			List<DBObject> incomeAnalysisDBObjects = amazeService
					.getIncomeAnalysisOutput_CC(accountList, last12MonthsDate);
			Map<String, Map<String, Map<String, Object>>> ccoverview = new LinkedHashMap<>();

			for (DBObject overviewObject : overviewList) {

				String accountValue = (String) overviewObject
						.get("SOURCE_ACCOUNT_NUMBER");
				Object yearObject = overviewObject.get("TRAN_YEAR");
				Object monthObject = overviewObject.get("MONTH");
				keyValueYearMonth = ""
						+ amazeService.getMonthByInt(Integer
								.parseInt(monthObject.toString())) + " "
						+ yearObject.toString().substring(2, 4);

				Map<String, Object> paymentAmountObj = new LinkedHashMap<>();
				Map<String, Object> spendsAmountObj = new LinkedHashMap<>();

				Double incomeDouble = Double.valueOf(overviewObject.get(
						"income").toString());

				incomeDouble = BigDecimal.valueOf(incomeDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				Double spendsDouble = Double.valueOf(overviewObject.get(
						"spends").toString());

				spendsDouble = BigDecimal.valueOf(spendsDouble)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				Double total = incomeDouble + spendsDouble;

				Double percentSpends = (spendsDouble / total) * 100;
				Double percentIncome = (incomeDouble / total) * 100;

				percentSpends = BigDecimal.valueOf(percentSpends)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				percentIncome = BigDecimal.valueOf(percentIncome)
						.setScale(3, RoundingMode.HALF_UP).doubleValue();

				paymentAmountObj.put("Payment amount", incomeDouble);
				spendsAmountObj.put("Spends amount", spendsDouble);

				Map<String, Object> innerObjIncome = new LinkedHashMap<>();

				for (DBObject incomeListObj : incomeAnalysisDBObjects) {
					Object yearObjectinc = incomeListObj.get("TRAN_YEAR");
					Object monthObjectinc = incomeListObj.get("MONTH");

					String accountNoObjectinc = (String) incomeListObj.get(
							"SOURCE_ACCOUNT_NUMBER").toString();
					if (accountValue.equalsIgnoreCase(accountNoObjectinc)) {

						String keyValueYearMonthinc = ""
								+ amazeService.getMonthByInt(Integer
										.parseInt(monthObjectinc.toString()))
								+ " "
								+ yearObjectinc.toString().substring(2, 4);
						if (keyValueYearMonthinc
								.equalsIgnoreCase(keyValueYearMonth)) {
							Map<String, Object> intermediate = new LinkedHashMap<>();
							intermediate.put("amount", Double
									.valueOf(incomeListObj.get("income")
											.toString()));
							innerObjIncome.put(
									(String) incomeListObj.get("CATEGORY"),
									intermediate);
						}
					}

				}

				Map<String, Object> innerObjSpends = new LinkedHashMap<>();

				for (DBObject spendsListObj : expenseAnalysisOutputList) {
					Object yearObjectexp = spendsListObj.get("TRAN_YEAR");
					Object monthObjectexp = spendsListObj.get("MONTH");
					String accountNoObjectexp = (String) spendsListObj.get(
							"SOURCE_ACCOUNT_NUMBER").toString();
					if (accountNoObjectexp.equalsIgnoreCase(accountValue)) {

						Double spendsValue = (Double) spendsListObj
								.get("spends");
						String categoryValue = (String) spendsListObj
								.get("CATEGORY");

						String keyValueYearMonthexp = ""
								+ amazeService.getMonthByInt(Integer
										.parseInt(monthObjectexp.toString()))
								+ " "
								+ yearObjectexp.toString().substring(2, 4);
						if (keyValueYearMonthexp
								.equalsIgnoreCase(keyValueYearMonth)) {
							Map<String, Object> amountData = new LinkedHashMap<>();
							amountData.put("amount", spendsValue);
							innerObjSpends.put(categoryValue, amountData);
						}
					}
				}
				Map<String, Object> overviewPreData = new LinkedHashMap<>();
				overviewPreData.put("Payment amount", incomeDouble);
				overviewPreData.put("Spends amount", spendsDouble);
				overviewPreData.put("Payment", innerObjIncome);
				overviewPreData.put("Spends", innerObjSpends);
				Map<String, Map<String, Object>> map = ccoverview
						.get(accountValue);
				if (map == null) {
					map = new LinkedHashMap<>();
					map.put(keyValueYearMonth, overviewPreData);
					ccoverview.put(accountValue, map);
				} else {
					map.put(keyValueYearMonth, overviewPreData);
					ccoverview.put(accountValue, map);
				}
				if (ccoverview.size() == 0)
					throw new CustomException("E02");

			}
			dataMap.put("SpendAnalysis", ccoverview);
			DBObject object = amazeService.getTillDate(USERID);
			if (object == null) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getCreditCardOverview E02 "
						+ ccoverviewStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			String tillDate = null;
			if (object != null) {
				tillDate = simpleDateFormat.format(object.get("MAX_TRAN_DATE"));
			}
			dataMap.put("tillDate", tillDate);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getCreditCardOverview E01 "
						+ ccoverviewStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getCreditCardOverview E00 ");
				getSpendAnalysisCreditCard(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			ifError = true;
			logger.error("Error for operation", S);
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			logger.error("Error for operation", exception);
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getCreditCardOverview E01 "
								+ ccoverviewStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getCreditCardOverview E00 ");
						getSpendAnalysisCreditCard(USERID, RRN, MOBILE,
								CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
					logger.error("Error Occured in Logs", exception.getCause());
				}
			} catch (Exception e) {
				logger.error(e);
				dataMap.put("STATUS", 500);
				ifError = true;

			}
		}
		if ((int) dataMap.get("STATUS") == 500)
			dataMap.remove("SpendAnalysis");
		if (ifError == false)
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getCreditCardOverview "
					+ "Success " + ccoverviewStatistics.totalTimeRequired());

		// logger.info("Data Map is "+dataMap);
		return dataMap;
	}

	@RequestMapping(value = "/getTransactions", method = RequestMethod.GET)
	public Map<String, Object> getTransactions(@RequestParam String USERID,
			@RequestParam Double RRN, 
			@RequestParam String MOBILE,
			@RequestParam String CHANNEL,
			@RequestParam String TEST,
			@RequestParam(required = false) Long OFFSET,
			@RequestParam(required = false) Long LIMIT, int... attemptNumber) {
		Statistics transactionStatistics = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();
			List<TransactionTimeline> transactions = amazeService
					.getTransactions(USERID, OFFSET, LIMIT, last12MonthsDate);
			if (transactions.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions E02 "
						+ transactionStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("TRANSACTIONS", transactions);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions E01 "
						+ transactionStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions E00 "
						+ transactionStatistics.totalTimeRequired());
				getTransactions(USERID, RRN, TEST, MOBILE, CHANNEL, OFFSET, LIMIT, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getTransactions E01 "
								+ transactionStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getTransactions E00 "
								+ transactionStatistics.totalTimeRequired());
						getTransactions(USERID, RRN, TEST, MOBILE, CHANNEL, OFFSET,
								LIMIT, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
					logger.error("ERROR1 :" + exception.getMessage());
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
				logger.error("ERROR2 :" + e.getMessage());
			}
		}
		if (ifError == false)
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions " + "Success "
					+ transactionStatistics.totalTimeRequired());
		return dataMap;
	}

	@RequestMapping("/getTransactionsV2")
	public Map<String, Object> getTransactionsV2(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL,
			@RequestParam(required = false) Long OFFSET,
			@RequestParam(required = false) Long LIMIT, int... attemptNumber) {
		Statistics transactionStatistics = new Statistics();
		USERID = USERID.toUpperCase();
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			Calendar now = Calendar.getInstance();
			now.add(Calendar.MONTH, -11);
			now.set(Calendar.DAY_OF_MONTH, 1);
			now.set(Calendar.HOUR, 00);
			now.set(Calendar.MINUTE, 00);
			now.set(Calendar.SECOND, 00);
			now.set(Calendar.MILLISECOND, 000);

			Date last12MonthsDate = now.getTime();
			List<TransactionTimeline> transactions = amazeService
					.getTransactionsV2(USERID, OFFSET, LIMIT, last12MonthsDate);
			if (transactions.size() == 0) {
				logger.error(" USERID " + USERID + " getTransactions E02 ");
				throw new CustomException("E02");
			}
			dataMap.put("TRANSACTIONS", transactions);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions E01 ");
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactions E00 ");
				getTransactionsV2(USERID, RRN, MOBILE, CHANNEL, OFFSET, LIMIT,
						1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getTransactions E01 ");
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getTransactions E00 ");
						getTransactionsV2(USERID, RRN, MOBILE, CHANNEL, OFFSET,
								LIMIT, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
			}
		}

		logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getTransactionsV2 "
				+ transactionStatistics.totalTimeRequired());
		return dataMap;

	}

	@RequestMapping("/getNLatestTransactionsUsingJava")
	public Map getTransactions(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL,
			@RequestParam(required = false, defaultValue = "100") Long limit) {

		Statistics nLatestTransactionsStatistics = new Statistics();
		USERID = USERID.toUpperCase();
		Map<String, Map<String, Map<String, List<TransactionTimeline>>>> yearMonthDateTree = new LinkedHashMap<>();
		Calendar now = Calendar.getInstance();
		now.add(Calendar.MONTH, -11);
		now.set(Calendar.DAY_OF_MONTH, 1);

		Date last12MonthsDate = now.getTime();
		List<TransactionTimeline> transactions = amazeService.getTransactions(
				USERID, limit, 0L, last12MonthsDate);
		for (TransactionTimeline transactionTimeline : transactions) {
			String tranYear = transactionTimeline.getTRAN_YEAR();
			String tranMon = transactionTimeline.getTRAN_MON();
			Integer tranDay = transactionTimeline.getTRAN_DAY();

			Map<String, Map<String, List<TransactionTimeline>>> monthDateMap = yearMonthDateTree
					.get(tranYear);
			if (monthDateMap == null) {
				monthDateMap = new LinkedHashMap<String, Map<String, List<TransactionTimeline>>>();
				yearMonthDateTree.put(tranYear, monthDateMap);
			}

			Map<String, List<TransactionTimeline>> dateMap = yearMonthDateTree
					.get(tranYear).get(tranMon);
			if (dateMap == null) {
				dateMap = new LinkedHashMap<String, List<TransactionTimeline>>();
				yearMonthDateTree.get(tranYear).put(tranMon, dateMap);
			}

			List<TransactionTimeline> transactionList = yearMonthDateTree
					.get(tranYear).get(tranMon).get(tranDay);
			if (transactionList == null) {
				transactionList = new LinkedList<TransactionTimeline>();
				yearMonthDateTree.get(tranYear).get(tranMon)
						.put("" + tranDay, transactionList);
			}

			yearMonthDateTree.get(tranYear).get(tranMon).get("" + tranDay)
					.add(transactionTimeline);
		}

		logger.info(" AMAZE - 1total time required for getNLatestTransactions::"
				+ nLatestTransactionsStatistics.totalTimeRequired());
		return yearMonthDateTree;
	}

	@RequestMapping("/getNLatestTransactionsByMongo")
	public List<DBObject> getTransactionsByMongo(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL,
			@RequestParam(required = false, defaultValue = "100") Long limit) {

		USERID = USERID.toUpperCase();

		List<DBObject> transactions = amazeService
				.getTransactionsByMongo(USERID);
		return transactions;
	}

	@RequestMapping(value = "/getSRStatus", method = RequestMethod.GET)
	public Map<String, Object> getSRStatus(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber) {

		Statistics srStatusStatistics = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			Date todaysDate = new Date();
			Map<String, List<DBObject>> srByCategory = new LinkedHashMap<>();
			List<DBObject> srStatus = amazeService.getSRStatus(USERID);
			if (srStatus.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getSRStatus E02 "
						+ srStatusStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			for (DBObject dbObject : srStatus) {
				Object object = dbObject.get("FLAG");
				dbObject.put("SYS_DATE", simpleDateFormat.format(todaysDate));
				if (dbObject.get("TARGET_CLOSE_DATE") != null) {
					String formattedTARGET_CLOSE_DATE = simpleDateFormat
							.format((Date) dbObject.get("TARGET_CLOSE_DATE"));
					dbObject.put("TARGET_CLOSE_DATE",
							formattedTARGET_CLOSE_DATE);
				} else {
					dbObject.put("TARGET_CLOSE_DATE", null);
				}
				if (dbObject.get("ACTUAL_CLOSE_DATE") != null) {
					String formattedACTUAL_CLOSE_DATE = simpleDateFormat
							.format((Date) dbObject.get("ACTUAL_CLOSE_DATE"));
					dbObject.put("ACTUAL_CLOSE_DATE",
							formattedACTUAL_CLOSE_DATE);
				} else {
					dbObject.put("ACTUAL_CLOSE_DATE", null);
				}
				if (dbObject.get("REQUEST_DATE") != null) {
					String formattedREQUEST_DATE = simpleDateFormat
							.format((Date) dbObject.get("REQUEST_DATE"));
					dbObject.put("REQUEST_DATE", formattedREQUEST_DATE);
				} else {
					dbObject.put("REQUEST_DATE", null);
				}
				if (object != null) {
					String key = (String) object;
					List<DBObject> srByCategoryList = srByCategory.get(key);
					if (srByCategoryList == null) {
						srByCategory.put(key, new LinkedList<>());
					}
					dbObject.removeField("FLAG");
					srByCategory.get(key).add(dbObject);
				}
			}
			dataMap.put("SR_DELIVERABLE", srByCategory);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" USERID " + USERID + " getSRStatus E01 "
						+ srStatusStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getSRStatus E00 "
						+ srStatusStatistics.totalTimeRequired());
				getSRStatus(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getSRStatus E01 "
								+ srStatusStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getSRStatus E00 "
								+ srStatusStatistics.totalTimeRequired());
						getSRStatus(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				// logger.error(e);
				dataMap.put("STATUS", 500);
				ifError = true;

			}
		}
		if (ifError == false)
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getSRStatus " + "Success "
					+ srStatusStatistics.totalTimeRequired());
		return dataMap;

	}

	@RequestMapping("/getUpcomingTransactionsV3")
	public Map<String, Object> getUpcomingTransactionsV3(
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {

		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			List<DBObject> upcomingTransactions = amazeService
					.getUpcomingTransaction(USERID);
			if (upcomingTransactions.size() == 0)
				throw new CustomException("E02");
			dataMap.put("getUpcomingTransactions",
					getUpcomingByCategoryViewV3(upcomingTransactions));
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {

			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", "E01");
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					dataMap.put("STATUS", 500);
					dataMap.put("ERROR_CODE", "E01");
				} else {
					dataMap.put("STATUS", 500);
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
			}
		}
		return dataMap;
	}

	private Map<String, List<DBObject>> getUpcomingByCategoryViewV3(
			List<DBObject> srStatus) {
		Map<String, List<DBObject>> srByCategory = new LinkedHashMap<>();
		for (DBObject dbObject : srStatus) {
			Object object = dbObject.get("CATEGORY");

			if (dbObject.get("DUE_DATE") != null) {
				String formattedDUE_DATE = simpleDateFormat
						.format((Date) dbObject.get("DUE_DATE"));
				dbObject.put("DUE_DATE", formattedDUE_DATE);
			}
			if (object != null) {
				List<DBObject> srByCategoryList = srByCategory
						.get(((String) object));
				if (srByCategoryList == null) {
					srByCategory.put(((String) object), new ArrayList<>());
				}
				dbObject.removeField("CATEGORY");
				srByCategory.get((String) object).add(dbObject);
			}
		}
		return srByCategory;
	}

	@RequestMapping(value = "/getUpcomingTransactions", method = RequestMethod.GET)
	public Map<String, Object> getUpcomingTransactions(
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {

		Statistics upcomingTransactionsStatistics = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			List<DBObject> upcomingTransactions = amazeService
					.getUpcomingTransaction(USERID);
			if (upcomingTransactions.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E02 "
						+ upcomingTransactionsStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("getUpcomingTransactions",
					getUpcomingTran(upcomingTransactions));
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E01 "
						+ upcomingTransactionsStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E00 "
						+ upcomingTransactionsStatistics.totalTimeRequired());
				getUpcomingTransactions(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getUpcomingTransactions E01 "
								+ upcomingTransactionsStatistics
										.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getUpcomingTransactions E00 "
								+ upcomingTransactionsStatistics
										.totalTimeRequired());
						getUpcomingTransactions(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;

			}
		}
		if (ifError == false)
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getUpcomingTransactions "
					+ "Success "
					+ upcomingTransactionsStatistics.totalTimeRequired());

		return dataMap;

	}

	@RequestMapping("/getUpcomingTransactionsV2")
	public Map<String, Object> getUpcomingTransactionsV2(
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {

		Statistics upcomingTransactionsStatistics = new Statistics();
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			List<DBObject> upcomingTransactions = amazeService
					.getUpcomingTransactionV2(USERID);
			if (upcomingTransactions.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E02 ");
				throw new CustomException("E02");
			}
			dataMap.put("getUpcomingTransactions",
					getUpcomingTran(upcomingTransactions));
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E01 ");
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
						+ " getUpcomingTransactions E00 ");
				getUpcomingTransactionsV2(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
							+ " getUpcomingTransactions E01 ");
					dataMap.put("STATUS", 500);
					dataMap.put("ERROR_CODE", "E01");
				} else {
					dataMap.put("STATUS", 500);
				}
			} catch (Exception e) {
				logger.error(e);
				dataMap.put("STATUS", 500);

			}
		}

		logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getUpcomingTransactions "
				+ upcomingTransactionsStatistics.totalTimeRequired());

		return dataMap;

	}

	private List<DBObject> getUpcomingTran(List<DBObject> srStatus) {

		List<DBObject> upcomingTransactions = new ArrayList<DBObject>();

		for (DBObject dbObject : srStatus) {

			if (dbObject.get("DUE_DATE") != null) {
				String formattedDUE_DATE = simpleDateFormat
						.format((Date) dbObject.get("DUE_DATE"));
				dbObject.put("DUE_DATE", formattedDUE_DATE);
			}
			upcomingTransactions.add(dbObject);
		}
		return upcomingTransactions;
	}

	@RequestMapping(value = "/getCategoryList", method = RequestMethod.GET)
	public Map<String, Object> getCategaory(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber) {

		Statistics categoryListStatistics = new Statistics();
		Boolean ifError = false;
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {

			Map<String, List<DBObject>> categoryMap = new LinkedHashMap<>();
			categoryMap.put("CR", amazeService.getCategoryList("CR"));
			categoryMap.put("DR", amazeService.getCategoryList("DR"));
			if (categoryMap.size() == 0) {
				logger.error(" USERID " + USERID + " getCategoryList E02 "
						+ categoryListStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("categoryList", categoryMap);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getCategoryList E01 "
						+ categoryListStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getCategoryList E00 "
						+ categoryListStatistics.totalTimeRequired());
				getCategaory(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getCategoryList E01 "
								+ categoryListStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " getCategoryList E00 "
								+ categoryListStatistics.totalTimeRequired());
						getCategaory(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;

			}
		}
		if (ifError == false)
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getCategoryList " + "Success "
					+ categoryListStatistics.totalTimeRequired());

		return dataMap;

	}

	@RequestMapping(value = "/getRecommendation", method = RequestMethod.GET)
	public Map<String, Object> getRecommendation(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber) {
		Boolean ifError = false;
		Statistics getRecommendationStatistics = new Statistics();
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {
			List<DBObject> recommendationList = amazeService
					.getRecommendationList(USERID);
			if (recommendationList.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendation E02 "
						+ getRecommendationStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("recommendations", recommendationList);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendation E01 "
						+ getRecommendationStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendation E00 "
						+ getRecommendationStatistics.totalTimeRequired());
				getRecommendation(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getRecommendation E01 "
								+ getRecommendationStatistics
										.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getRecommendation E00 "
								+ getRecommendationStatistics
										.totalTimeRequired());
						getRecommendation(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
			}
		}
		if (ifError == false) {
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendation "
					+ "Success "
					+ getRecommendationStatistics.totalTimeRequired());
		}
		return dataMap;

	}
	
	// getRecommendationcorporate
	@RequestMapping(value = "/getRecommendationCorp", method = RequestMethod.GET)
	public Map<String, Object> getRecommendationCorp(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL, int... attemptNumber) {
		Boolean ifError = false;
		Statistics getRecommendationStatistics = new Statistics();
		USERID = USERID.toUpperCase();

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {
			List<DBObject> recommendationList = amazeService
					.getRecommendationCorpList(USERID);
			if (recommendationList.size() == 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendationCorp E02 "
						+ getRecommendationStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("recommendations", recommendationList);
			dataMap.put("STATUS", 200);
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendationCorp E01 "
						+ getRecommendationStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendationCorp E00 "
						+ getRecommendationStatistics.totalTimeRequired());
				getRecommendationCorp(USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getRecommendationCorp E01 "
								+ getRecommendationStatistics
										.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID "
								+ USERID
								+ " getRecommendationCorp E00 "
								+ getRecommendationStatistics
										.totalTimeRequired());
						getRecommendationCorp(USERID, RRN, MOBILE, CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
			}
		}
		if (ifError == false) {
			logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " getRecommendationCorp "
					+ "Success "
					+ getRecommendationStatistics.totalTimeRequired());
		}
		return dataMap;

	}	
	

	@RequestMapping(value = "/updateCategory", method = RequestMethod.POST)
	public Map<String, Object> updateCategory(
			@RequestBody UpdateCategory updateCategory,
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {
		long start = System.currentTimeMillis();
		Statistics updateCategoryStatistics = new Statistics();
		Boolean ifError = false;
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		USERID = USERID.toUpperCase();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		try {
			String newCategory = updateCategory.getNewCategory();
			String oldCategory = updateCategory.getOldCategory();

			if (newCategory != null && !newCategory.isEmpty()) {
				if (newCategory.equals(oldCategory)) {
					logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID
							+ " OLD & NEW CATEGORY SAME"
							+ " UpdateCategory-JSONRefresh");
				} else {

					int updateResult = amazeService.updateCategory(USERID,
							updateCategory);
					if (updateResult < 0) {
						long end = System.currentTimeMillis();
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " updateCategory E03 " + (end - start)
								+ " No records updated"
								+ updateCategoryStatistics.totalTimeRequired());
						ifError = true;
						throw new CustomException("E03 " + (end - start));
					}
					if (updateResult > 0) {
						logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " UpdateCategory-Success-" + updateResult
								+ " record(s)");
					}
					if (updateResult == 0) {
						logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " UpdateCategory-JSONRefresh");
					}
				}
			} else {

				logger.info(" CHANNEL " + CHANNEL + " USERID " + USERID + " NEW CATEGORY:"
						+ newCategory + " UpdateCategory-JSONRefresh");
			}

			dataMap = getTransactions(USERID, RRN, null, MOBILE, CHANNEL, null, null);
		}

		catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " updateCategory E01 "
						+ updateCategoryStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID + " updateCategory E00 "
						+ updateCategoryStatistics.totalTimeRequired());
				updateCategory(updateCategory, USERID, RRN, MOBILE, CHANNEL, 1);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
		}

		catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " updateCategory E01 "
								+ updateCategoryStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" CHANNEL " + CHANNEL + " USERID " + USERID
								+ " updateCategory E00 "
								+ updateCategoryStatistics.totalTimeRequired());
						updateCategory(updateCategory, USERID, RRN, MOBILE,
								CHANNEL, 1);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
			}
		}
		if (ifError == false) {
			logger.info(" USERID " + USERID + " updateCategory " + "Success "
					+ updateCategoryStatistics.totalTimeRequired());
		}
		return dataMap;
	}

	@RequestMapping(value = "/test", method = RequestMethod.POST)
	public Map<String, Object> test(@RequestBody UpdateCategory updateCategory,
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {

		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		Map<String, Object> sa = new LinkedHashMap<String, Object>();
		Map<String, Integer> insp = new LinkedHashMap<String, Integer>();
		insp.put("Income amount", 555);
		sa.put("MAR 18", insp);
		USERID = USERID.toUpperCase();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		dataMap.put("Spending Overview", sa);
		Map<String, Object> Result = new LinkedHashMap<>();
		try {
			Result = amazeService.getCachedData(USERID);
			dataMap = getTransactions(USERID, RRN, null, MOBILE, CHANNEL, null, null);
		}

		catch (MongoTimeoutException exception) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", "E01");
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					dataMap.put("STATUS", 500);
					dataMap.put("ERROR_CODE", "E01");
				} else {
					dataMap.put("STATUS", 500);
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
			}
		}
		return (Map<String, Object>) Result.get("data");
	}
	
	
	@RequestMapping(value = "/getIVRMessage", method = RequestMethod.GET)
	public Map<String, Object> getIVRMessage(@RequestParam String USERID,
			@RequestParam Double RRN, @RequestParam String MOBILE,
			@RequestParam String CHANNEL,
			@RequestParam String ACC,
			//@RequestParam String SCENARIONO,
			int...attemptNumber) {
		Boolean ifError = false;
		Statistics transactionStatistics = new Statistics();
		USERID = USERID.toUpperCase();
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		dataMap.put("ACC", ACC);
		//dataMap.put("SCENARIO_NO", SCENARIONO);
		try {
			List<DBObject> ivrmessges = amazeService.getIVRMessage(USERID, MOBILE, ACC);
			if (ivrmessges.size() == 0) {
				logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
						//+ "SCENARIONO "+SCENARIONO
						+ " getIVRMessage E02 "
						+ transactionStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E02");
			}
			dataMap.put("IVRMESSAGES", ivrmessges);
			dataMap.put("STATUS", 200);
			
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
						//+ " SCENARIONO "+SCENARIONO
						+ " getIVRMessage E01 "
						+ transactionStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" USERID " + USERID + " getIVRMessage E00 "
						+ transactionStatistics.totalTimeRequired());
				getIVRMessage(USERID, RRN, MOBILE, CHANNEL, ACC, attemptNumber);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
//			logger.info("customtom ="+S);
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" USERID " + USERID+" MOBILE "+MOBILE+" ACCOUNT "+ACC
							//	+"SCENARIONO "+SCENARIONO
								+ " getIVRMessage E01 "
								+ transactionStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" USERID " + USERID+" MOBILE "+MOBILE+" ACCOUNT "+ACC
							//	+"SCENARIONO "+SCENARIONO
								+ " getIVRMessage E00 "
								+ transactionStatistics.totalTimeRequired());
						getIVRMessage(USERID, RRN, MOBILE, CHANNEL, ACC, attemptNumber);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
			}
		}
//		if (ifError == false){
			if (!ifError){
			logger.info(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
				//	+" SCENARIONO "+SCENARIONO
					+ " getIVRMessage " + "Success "
					+ transactionStatistics.totalTimeRequired());
		}
		return dataMap;
		
	}
	@RequestMapping(value = "/putIVRResponse", method = RequestMethod.POST)
	public Map<String, Object> putIVRResponse(@RequestBody CallCenterResponse callCenterResponse,
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			@RequestParam String ACC,
			@RequestParam String SCENARIONO,
			int... attemptNumber
			) {		
		
		long start = System.currentTimeMillis();
		Statistics putIVRResponseStatistics = new Statistics();
		
		Boolean ifError = false;
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		
		dataMap.put("ACCOUNT_NO", ACC);
		dataMap.put("MOILE", MOBILE);
		
		try {
			int insertResult = amazeService.insertIVRResponse(callCenterResponse);
			if (insertResult < 0) {
				logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
						+" SCENARIONO "+SCENARIONO
						+ " putIVRResponse " + " E03 "
						+ putIVRResponseStatistics.totalTimeRequired());
				ifError = true;
				throw new CustomException("E03");
			}
//			if (insertResult > 0) {
//				logger.info(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
//						+" SCENARIONO "+SCENARIONO
//						+ " getIVRMessage " + "Success "
//						+ putIVRResponseStatistics.totalTimeRequired() + insertResult + " records ");
//				dataMap.put("STATUS", 200);
//			}
//			dataMap.put("STATUS", 200);
			
		} catch (MongoTimeoutException exception) {
			if (attemptNumber.length > 0) {
				logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
						//+" SCENARIONO "+SCENARIONO
						+ " putIVRResponse E01 "
						+ putIVRResponseStatistics.totalTimeRequired());
				ifError = true;
				dataMap.put("STATUS", 500);
				dataMap.put("ERROR_CODE", "E01");
			} else {
				logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
						+" SCENARIONO "+SCENARIONO
						+ " putIVRResponse E00 "
						+ putIVRResponseStatistics.totalTimeRequired());
				putIVRResponse(callCenterResponse, USERID, RRN, MOBILE, CHANNEL, ACC, SCENARIONO, attemptNumber);
			}
		} catch (CustomException S) {
			dataMap.put("STATUS", 500);
			dataMap.put("ERROR_CODE", S.getMessage());
//			logger.info("customtom ="+S);
		} catch (Exception exception) {
			try {
				if (exception.getCause().getCause() instanceof SocketTimeoutException) {
					if (attemptNumber.length > 0) {
						logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
								//+" SCENARIONO "+SCENARIONO
								+ " putIVRResponse E01 "
								+ putIVRResponseStatistics.totalTimeRequired());
						ifError = true;
						dataMap.put("STATUS", 500);
						dataMap.put("ERROR_CODE", "E01");
					} else {
						logger.error(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
								//+" SCENARIONO "+SCENARIONO
								+ " putIVRResponse E00 "
								+ putIVRResponseStatistics.totalTimeRequired());
						putIVRResponse(callCenterResponse, USERID, RRN, MOBILE, CHANNEL, ACC, SCENARIONO,attemptNumber);
					}
				} else {
					dataMap.put("STATUS", 500);
					ifError = true;
				}
			} catch (Exception e) {
				dataMap.put("STATUS", 500);
				ifError = true;
			}
		}
////		if (ifError == false){
			if (!ifError){
			logger.info(" USERID " + USERID +" MOBILE "+MOBILE+" ACCOUNT "+ACC
					+" SCENARIONO "+SCENARIONO
					+ " getIVRMessage " + "Success "
					+ putIVRResponseStatistics.totalTimeRequired());
			dataMap.put("STATUS", 200);
		}
		return dataMap;
		
	}
	
	@RequestMapping(value = "/insertCCAudit", method = RequestMethod.POST)
	public Map<String, Object> insertCCAudit(
			@RequestBody UpdateCategory updateCategory,
			@RequestParam String USERID, @RequestParam Double RRN,
			@RequestParam String MOBILE, @RequestParam String CHANNEL,
			int... attemptNumber) {
		long start = System.currentTimeMillis();
		Statistics updateCategoryStatistics = new Statistics();
		Boolean ifError = false;
		Map<String, Object> dataMap = new LinkedHashMap<String, Object>();
		USERID = USERID.toUpperCase();
		dataMap.put("USERID", USERID);
		dataMap.put("RRN", RRN);
		dataMap.put("CHANNEL", CHANNEL);
		dataMap.put("MOBILE", MOBILE);
		
		int updateResult = amazeService.insertData(USERID,
				updateCategory);
		
		return dataMap;
	}
}
