package com.direct.srv;

import java.util.concurrent.Future;

import com.direct.excp.WrongTableException;
import com.direct.webflow.Result;

public interface ComprTbl {

	public Future<Result> comprTableByLsk(Class tableClass, String lsk, Integer curPeriod);

}