package com.motobang.task;

/**
 * 
 * Created by junfei.Yang on 2020年3月11日.
 */
public class CfgException extends Exception {

	private static final long serialVersionUID = -661377294271386745L;

	public CfgException() {
        super();
    }

    public CfgException(String message) {
        super(message);
    }

    public CfgException(String message, Throwable cause) {
        super(message, cause);
    }

    public CfgException(Throwable cause) {
        super(cause);
    }
}
