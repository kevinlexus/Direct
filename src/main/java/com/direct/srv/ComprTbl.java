package com.direct.srv;

import java.util.concurrent.Future;

import com.direct.webflow.Result;

public interface ComprTbl {

	public Future<Result> comprTableByLsk(String table, String lsk, Integer backPeriod, Integer curPeriod, boolean isAllPeriods, boolean isByUsl);

}