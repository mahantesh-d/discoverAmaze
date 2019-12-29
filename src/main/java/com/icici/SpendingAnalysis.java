package com.icici;

/**
 * @author ntipl
 *
 */
public class SpendingAnalysis {
	private String category;
	private Double amount;
	private Double budget;
	private Boolean overBudget;

	public Double getAmount() {
		return amount;
	}
	public void setAmount(Double amount) {
		this.amount = amount;
	}
	public Boolean getOverBudget() {
		return overBudget;
	}
	public void setOverBudget(Boolean overBudget) {
		this.overBudget = overBudget;
	}
	public Double getBudget() {
		return budget;
	}
	public void setBudget(Double budget) {
		this.budget = budget;
	}
	public String getCategory() {
		return category;
	}
	public void setCategory(String category) {
		this.category = category;
	}
	
}
