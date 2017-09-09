package com.direct.webflow;

/**
 * Результат выполнения потока
 * @author lev
 *
 */
public class Result {

	// код ошибки
	private int err;
	// обрабатываемый лиц.счет
	private String lsk;

	// конструктор
	public Result(int err, String lsk) {
		this.err = err;
		this.lsk = lsk;
	}

	public void setErr(int err) {
		this.err = err;
	}

	public int getErr() {
		return err;
	}

	public String getLsk() {
		return lsk;
	}

	
}
