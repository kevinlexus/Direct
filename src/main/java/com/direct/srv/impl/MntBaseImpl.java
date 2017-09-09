package com.direct.srv.impl;

import java.util.ArrayList;
import java.util.List;
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
	// Параметры
	private Param param;
	// Текущий период
	private Integer curPeriod;
	
	/**
	 * Сжать таблицу 
	 * @throws Exception 
	 */
	private void comprTable(Class tableClass) throws Exception {
		long startTime;
		long endTime;
		long totalTime;
		startTime = System.currentTimeMillis();

		// Порезать список лс на пачки по N штук
		List<String> lstLsk = kartDao.getAll().stream().map(t -> t.getLsk()).collect(Collectors.toList());
		List<List<String>> batch = Lists.partition(lstLsk, 10);
		
		//batch.stream().forEach(t-> {
		for (List<String> t : batch) {
			List<Future<Result>> frl = new ArrayList<Future<Result>>();
			t.stream().forEach(lsk-> {
				ComprTbl comprTbl = ctx.getBean(ComprTbl.class);
				log.info("Started thread lsk={}", lsk);
				Future<Result> fut = comprTbl.comprTableByLsk(tableClass, lsk, curPeriod);
				frl.add(fut);
			});
			
			// проверить окончание всех потоков
			int flag2 = 0;
			while (flag2 == 0) {
				log.info("========================================== Waiting for threads");
				flag2 = 1;
				for (Future<Result> fut : frl) {
					if (!fut.isDone()) {
						flag2 = 0;
					} else {
							try {
								if (fut.get().getErr() != 0) {
									throw new Exception("Ошибка в потоке err="+fut.get().getErr());
								}
								log.info("Done thread Lsk={}, Result.err={}",
										fut.get().getLsk(), fut.get().getErr());
							} catch (InterruptedException | ExecutionException e) {
								// TODO Auto-generated catch block
								e.printStackTrace();
							}
					}
				}

				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
		};

		endTime = System.currentTimeMillis();
		totalTime = endTime - startTime;
		log.info("table:{}, Time for compress:{}sec", tableClass, totalTime/1000, 2);
		
	}
	
	public boolean comprAllTables() {
		// Получить параметры
		param = paramDao.findAll().stream().findFirst().orElse(null);
		curPeriod = Integer.valueOf(param.getPeriod());
		try {
			comprTable(Anabor.class);
		} catch (Exception e) {
			// Ошибка при выполнении
			e.printStackTrace();
			return false;
		}
		return true;
	}
	
	
	
	

}
