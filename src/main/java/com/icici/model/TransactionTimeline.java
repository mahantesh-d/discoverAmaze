package com.icici.model;

import java.text.SimpleDateFormat;

import java.util.Date;

import org.springframework.data.mongodb.core.mapping.Field;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.format.annotation.DateTimeFormat.ISO;

public class TransactionTimeline {

	private Date TRAN_DATE_TIME;
	private Integer TRAN_DAY;
	private String TRAN_MON;
	private String TRAN_YEAR;
	@Field("SOURCE_ACCOUNT_NUMBER")
	private String ACCOUNT_NO;
	private String SOURCE_SYSTEM_CODE;
	private String ACCOUNT_TYPE;
	private String MERCHANT;
	private String CATEGORY;
	private String DEBIT_CREDIT;
	private Double AMOUNT;
	private String BUCKET;
	private String INCOME_EXPENSE;
	private String DISPLAY_DATE;
	private Long EVENTID;
//	private String MESSAGE;
	private String LINK_FLAG;
	private String DEL_FLAG;
	private String CAPTION;
	private String REDIR_FLAG;
//	private String RED_PARAM;
	
	@DateTimeFormat(iso = ISO.DATE_TIME)
	private Date DATE_CREATED;
	
	
	
	public Long getEVENTID() {
		return EVENTID;
	}

	public void setEVENTID(Long eVENTID) {
		EVENTID = eVENTID;
	}

	private SimpleDateFormat simpleDateFormat = new SimpleDateFormat(
			"dd MMM yy HH:mm:ss a");

	public Date getTRAN_DATE_TIME() {
		return TRAN_DATE_TIME;
	}

	public String getDISPLAY_DATE() {
		return simpleDateFormat.format(TRAN_DATE_TIME);
	}

	public void setTRAN_DATE_TIME(Date tRAN_DATE_TIME) {
		TRAN_DATE_TIME = tRAN_DATE_TIME;
	}

	public Integer getTRAN_DAY() {
		return TRAN_DAY;
	}

	public void setTRAN_DAY(Integer tRAN_DAY) {
		TRAN_DAY = tRAN_DAY;
	}

	public String getTRAN_MON() {
		return TRAN_MON;
	}

	public void setTRAN_MON(String tRAN_MON) {
		TRAN_MON = tRAN_MON;
	}

	public String getTRAN_YEAR() {
		return TRAN_YEAR;
	}

	public void setTRAN_YEAR(String tRAN_YEAR) {
		TRAN_YEAR = tRAN_YEAR;
	}

	public String getACCOUNT_NO() {
		return ACCOUNT_NO;
	}

	public void setACCOUNT_NO(String aCCOUNT_NO) {
		ACCOUNT_NO = aCCOUNT_NO;
	}

	public String getMERCHANT() {
		return MERCHANT;
	}

	public void setMERCHANT(String mERCHANT) {
		MERCHANT = mERCHANT;
	}

	public String getCATEGORY() {
		return CATEGORY;
	}

	public void setCATEGORY(String cATEGORY) {
		CATEGORY = cATEGORY;
	}

	public String getDEBIT_CREDIT() {
		return DEBIT_CREDIT;
	}

	public void setDEBIT_CREDIT(String dEBIT_CREDIT) {
		DEBIT_CREDIT = dEBIT_CREDIT;
	}

	public Double getAMOUNT() {
		return AMOUNT;
	}

	public void setAMOUNT(Double aMOUNT) {
		AMOUNT = aMOUNT;
	}

	public String getBUCKET() {
		return BUCKET;
	}

	public void setBUCKET(String bUCKET) {
		BUCKET = bUCKET;
	}

	public String getINCOME_EXPENSE() {
		return INCOME_EXPENSE;
	}

	public void setINCOME_EXPENSE(String iNCOME_EXPENSE) {
		INCOME_EXPENSE = iNCOME_EXPENSE;
	}
	
//	public String getMESSAGE() {
//		return MESSAGE ;
//	}
//	
//	public void setMESSAGE(String mESSAGE) {
//		MESSAGE = mESSAGE;
//	}
	
	public String getLINK_FLAG() {
		return LINK_FLAG;
	}
	
	public void setLINK_FLAG(String lINK_FLAG ) {
		LINK_FLAG = lINK_FLAG;
	}
	
	public String getDEL_FLAG() {
		return DEL_FLAG;
	}
	
	public void setDEL_FLAG(String dEL_FLAG) {
		DEL_FLAG = dEL_FLAG;
	}
	
	public String getCAPTION() {
		return CAPTION;
	}
	
	public void setCAPTION(String cAPTION) {
		CAPTION = cAPTION;
	}
	
	public String getREDIR_Flag() {
		return REDIR_FLAG;
	}
	
	public void setREDIR_Flag(String rEDIR_Flag) {
		REDIR_FLAG = rEDIR_Flag;
	}
	
//	public String getRED_PARAM() {
//		return RED_PARAM;
//	}
//	
//	public void setRED_PARAM(String rED_PARAM) {
//		RED_PARAM = rED_PARAM;
//	}
	
	public String getACCOUNT_TYPE() {
		switch (SOURCE_SYSTEM_CODE) {
		case "30":
			ACCOUNT_TYPE = "Saving Account";
			break;
		case "20":
			ACCOUNT_TYPE = "Credit Card";
			break;
		case "90":
			ACCOUNT_TYPE = "Pockets";
			break;
		default:
			ACCOUNT_TYPE = "";
			break;
		}
		return ACCOUNT_TYPE;
	}

	public void setSOURCE_SYSTEM_CODE(String sOURCE_SYSTEM_CODE) {
		SOURCE_SYSTEM_CODE = sOURCE_SYSTEM_CODE;
	}

}
