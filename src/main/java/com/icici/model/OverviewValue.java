package com.icici.model;

public class OverviewValue {

	private Double amount;
	private Double percentage;

	
	public OverviewValue() {
		super();
	}
	public OverviewValue(Double amount, Double percentage) {
		super();
		this.amount = amount;
		this.percentage = percentage;
	}
	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	public Double getPercentage() {
		return percentage;
	}
	public void setPercentage(Double percentage) {
		this.percentage = percentage;
	}
	
}
