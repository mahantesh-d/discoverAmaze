package com.icici.model;

public class Overview {
	private OverviewValue income;	
	private OverviewValue spends;
	
	
	public Overview() {
	}
	public Overview(OverviewValue income, OverviewValue spends) {
		super();
		this.income = income;
		this.spends = spends;
	}
	public OverviewValue getIncome() {
		return income;
	}
	public void setIncome(OverviewValue income) {
		this.income = income;
	}
	public OverviewValue getSpends() {
		return spends;
	}
	public void setSpends(OverviewValue spends) {
		this.spends = spends;
	}
	
}
