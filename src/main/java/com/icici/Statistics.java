package com.icici;

import java.util.Date;

public class Statistics {

	private Date startedDate;

	public Statistics() {
		startedDate =new Date();
	}
	
	public long totalTimeRequired() {
		return (new Date().getTime())-startedDate.getTime();
	}
}
