package com.icici.model;

public class SpendingCategoryOverview {

	private Double amount;
	private Double budget;
	private Boolean isOverbudget;
	
	public SpendingCategoryOverview() {
	}
	
	public SpendingCategoryOverview(Double amount, Double budget, Boolean isOverbudget) {
		super();
		this.amount = amount;
		this.budget = budget;
		this.isOverbudget = isOverbudget;
	}
	
	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	public Double getBudget() {
		return budget;
	}
	public void setBudget(Double budget) {
		this.budget = budget;
	}
	public Boolean getIsOverbudget() {
		return isOverbudget;
	}
	public void setIsOverbudget(Boolean isOverbudget) {
		this.isOverbudget = isOverbudget;
	}
	
}
