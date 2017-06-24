package com.spark.practice.datasets;

import java.io.Serializable;

public class RecordHive implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -6378387381445552418L;

	private int key;
	private String value;

	public int getKey() {
		return key;
	}

	public void setKey(int key) {
		this.key = key;
	}

	public String getValue() {
		return value;
	}

	public void setValue(String value) {
		this.value = value;
	}

}
