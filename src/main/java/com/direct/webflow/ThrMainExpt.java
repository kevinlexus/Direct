package com.direct.webflow;

//Exception class --пока не используется
public class ThrMainExpt  extends Exception {
	
	  private static final long serialVersionUID = 1L;
	  public ThrMainExpt() { super(); }
	  public ThrMainExpt(String message) { super(message); }
	  public ThrMainExpt(String message, Throwable cause) { super(message, cause); }
	  public ThrMainExpt(Throwable cause) { super(cause); }

}
