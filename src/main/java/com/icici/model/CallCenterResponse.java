package com.icici.model;

public class CallCenterResponse {
	private int scenario_no;
	private String scenario_name;
	private String mobile;
	private String acc_no;
	private String resp_status;
	public int getScenario_no() {
		return scenario_no;
	}
	public void setScenario_no(int scenario_no) {
		this.scenario_no = scenario_no;
	}
	public String getScenario_name() {
		return scenario_name;
	}
	public void setScenario_name(String scenario_name) {
		this.scenario_name = scenario_name;
	}
	public String getMobile() {
		return mobile;
	}
	public void setMobile(String mobile) {
		this.mobile = mobile;
	}
	public String getAcc_no() {
		return acc_no;
	}
	public void setAcc_no(String acc_no) {
		this.acc_no = acc_no;
	}
	public String getResp_status() {
		return resp_status;
	}
	public void setResp_status(String resp_status) {
		this.resp_status = resp_status;
	}
	

}
