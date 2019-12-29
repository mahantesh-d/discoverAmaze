package com.icici.model;

import java.text.SimpleDateFormat;
import java.util.Date;
public class UpdateCategory {
	
	
	private String oldCategory;
	private Long eventId;

	private String newCategory;
	private Double tranAmount;
	private String accountNo;
	private Long tranDate;
	private String merchant;
	
	public Long getEventId() {
		return eventId;
	}
	public void setEventId(Long eventId) {
		this.eventId = eventId;
	}
	public String getNewCategory() {
		return newCategory;
	}
	public String getOldCategory() {
		return oldCategory;
	}
	public Double getTranAmount() {
		return tranAmount;
	}
	public String getAccountNo() {
		return accountNo;
	}
	public Long getTranDate() {
		return tranDate;
	}
	public String getMerchant() {
		return merchant;
	}
	public void setNewCategory(String newCategory) {
		this.newCategory = newCategory;
	}
	public void setOldCategory(String oldCategory) {
		this.oldCategory = oldCategory;
	}
	public void setTranAmount(Double tranAmount) {
		this.tranAmount = tranAmount;
	}
	public void setAccountNo(String accountNo) {
		this.accountNo = accountNo;
	}
	public void setTranDate(Long tranDate) {
		this.tranDate = tranDate;
	}
	public void setMerchant(String merchant) {
		this.merchant = merchant;
	}
	
	
}

