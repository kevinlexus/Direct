package com.direct.srv;

import com.direct.excp.WrongTableException;

public interface MntBase {

	public boolean comprAllTables(String firstLsk, boolean isAllPeriods);

}