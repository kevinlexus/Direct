package com.direct.srv.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Service;

import com.dic.bill.dao.KartDAO;
import com.dic.bill.dao.ParamDAO;
import com.dic.bill.model.scott.Anabor;
import com.dic.bill.model.scott.Kart;
import com.dic.bill.model.scott.Param;
import com.direct.excp.WrongTableException;
import com.direct.srv.ComprTbl;
import com.direct.srv.MntBase;
import com.direct.webflow.Result;
import com.google.common.collect.Lists;

import lombok.extern.slf4j.Slf4j;
/**
 * Cервис обслуживания базы данных
 * @author lev
 *
 */
@Slf4j
@Service
public class MntBaseImpl implements MntBase{
	
	@Autowired
	private KartDAO kartDao;
	@Autowired
	private ParamDAO paramDao;
	@Autowired
	private ApplicationContext ctx;
	// параметры
	private Param param;
	// текущий период
	private Integer curPeriod;
	// анализировать все периоды?
	private boolean isAllPeriods;
	
	/**
	 * Сжать таблицу 
	 * @throws Exception 
	 * @param - tableClass - класс таблицы
	 * @param - firstLsk - начать с лицевого
	 * @param - isByUsl - использовать ли поле "usl" для критерия сжатия (не подходит для всех таблиц, например archkart)
	 */
	private void comprTable(String table, String firstLsk, Boolean isByUsl) throws Exception {
		long startTime;
		long endTime;
		long totalTime;
		// Кол-во потоков
		int cntThread = 5;
		int a=0;
		String lastLsk = null;
		log.info("Compress table:{},  threads count:{}", table, cntThread);
		startTime = System.currentTimeMillis();
		// Наибольшее время выполнения и лицевой
		String expnsLsk = null;
		Integer expnsTime = 0;
		
		// Порезать список лс на пачки по N штук		
		List<String> lstLsk = kartDao.getAfterLsk(firstLsk).stream().map(t -> t.getLsk())
				//.filter(t -> t.equals("12010228"))
				.collect(Collectors.toList());
		List<List<String>> batch = Lists.partition(lstLsk, cntThread);
		
		
		a=0;
		for (List<String> t : batch) {
			if (a >= 1000) {
				a = 0;
			}
			if (a == 0) {
				log.info("Started new 1000 Lsk :{}", t.stream().findFirst().orElse(null));
			}
			
			a = a + cntThread;
			long startTime2;
			long endTime2;
			long totalTime2;
			startTime2 = System.currentTimeMillis();

			List<Future<Result>> frl = new ArrayList<Future<Result>>();
			for (String lsk : t) {
				ComprTbl comprTbl = ctx.getBean(ComprTbl.class);
				lastLsk = lsk;
				Future<Result> fut = comprTbl.comprTableByLsk(table, lsk, curPeriod, isAllPeriods, isByUsl);
				frl.add(fut);
			};
			
			// проверить окончание всех потоков
			int flag2 = 0;
			while (flag2 == 0) {
				//log.info("========================================== Waiting for threads");
				flag2 = 1;
				for (Future<Result> fut : frl) {
					if (!fut.isDone()) {
						flag2 = 0;
					} else {
							try {
								if (fut.get().getErr() != 0) {
									throw new Exception("Ошибка в потоке err="+fut.get().getErr());
								}
								//log.info("Done thread Lsk={}, Result.err={}",
								//		fut.get().getLsk(), fut.get().getErr());
							} catch (InterruptedException | ExecutionException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}
				}

				try {
					Thread.sleep(100);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			endTime2 = System.currentTimeMillis();
			totalTime2 = endTime2 - startTime2;
			if (totalTime2/cntThread > expnsTime) {
				expnsTime = (int) (totalTime2/cntThread); 
				log.info("MOST EXPENSIVE BATCH {} msec, last Lsk={}", expnsTime, lastLsk);
			} else {
				log.info("Time/Lsk:{} msec, last Lsk={}", totalTime2/cntThread, lastLsk);
			}

		};

		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		log.info("Overall time for compress:{} sec", totalTime/1000, 2);
		
	}
	
	/**
	 * Сжать все необходимые таблицы
	 * @param firstLsk - начать с лиц.сч.
	 * @return
	 */
	public boolean comprAllTables(String firstLsk, String table, boolean isAllPeriods) {
		log.info("===Version 1.4===");
		this.isAllPeriods = isAllPeriods;
		// Получить параметры
		param = paramDao.findAll().stream().findFirst().orElse(null);
		curPeriod = Integer.valueOf(param.getPeriod());
		try {
			comprTable(table, firstLsk, true);
		} catch (Exception e) {
			// Ошибка при выполнении
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
	
	

}
