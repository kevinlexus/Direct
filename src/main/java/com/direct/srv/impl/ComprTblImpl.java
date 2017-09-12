package com.direct.srv.impl;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dic.bill.Compress;
import com.dic.bill.dao.AnaborDAO;
import com.dic.bill.model.scott.Anabor;
import com.direct.excp.WrongTableException;
import com.direct.srv.ComprTbl;
import com.direct.webflow.Result;
import com.ric.bill.Utl;

import lombok.extern.slf4j.Slf4j;


/**
 * Сервис сжатия таблиц 
 * @author lev
 *
 */
@Slf4j
@Service
@Scope("prototype")
public class ComprTblImpl implements ComprTbl {

	//EntityManager - EM нужен на каждый DAO или сервис свой!
    @PersistenceContext
    private EntityManager em;
	@Autowired
	private AnaborDAO anaborDao;

	// Все элементы по лиц.счету по всем периодам
	List<Compress> lst;
	// отсортированный список периодов  
	SortedSet<Integer> lstPeriod;
	// период последнего обработанного массива
	private Integer lastUsed;
	// Текущий период
	private Integer curPeriod;
	// isByUsl - использовать ли поле "usl" для критерия сжатия
	boolean isByUsl;
	
	
	/**
     * Сжать таблицу, содержащую mg1, mg2
	 * @param lsk - лиц.счет
	 * @param curPeriod - текущий период
	 * @param isByUsl - использовать ли поле "usl" для критерия сжатия
	 * @throws WrongTableException 
	 */
	@Async //- Async чтобы организовался поток
    @Transactional
	public Future<Result> comprTableByLsk(Class tableClass, String lsk, Integer curPeriod, boolean isByUsl) {
		//log.info("НАЧАЛО сжатия лиц.счет:{}", lsk);
		this.isByUsl = isByUsl;
    	Result res = new Result(0, lsk);
    	this.curPeriod = curPeriod;
    	lst = new ArrayList<Compress>();
    	// Список услуг
    	Set<String> lstUsl = new TreeSet<String>();
    	// Минимальный, максимальный период
    	Integer minPeriod, maxPeriod;
 	   	// Получить все элементы по лиц.счету
    	if (tableClass == Anabor.class) {
        	lst.addAll(anaborDao.getByLsk(lsk));
    	} else {
    		// Ошибка - не тот класс таблицы
    		res.setErr(2);
    		return new AsyncResult<Result>(res);
    	}
    	
    	// Список всех услуг
    	lstUsl.addAll(lst.stream().map(t -> t.getUsl()).distinct().collect(Collectors.toSet()));

    	if (isByUsl) {
    		// Сжимать с использованием кода услуги USL
    		for (String usl :lstUsl) {
    			lastUsed = null;
    	    	// Получить все периоды, фильтр - по услуге
    	    	Set<Integer> lstPeriodPrep = lst.stream().filter(d -> d.getUsl().equals(usl)).map(t -> t.getMg1()).collect(Collectors.toSet());
    	    	// Получить все периоды mg1, уникальные - по услуге
    	    	lstPeriod = new TreeSet<Integer>();
    	    	// Отсортированные
    	    	lstPeriod.addAll(lstPeriodPrep.stream().distinct().collect(Collectors.toSet()));
    	    	minPeriod = lstPeriod.first();
    	    	maxPeriod = lstPeriod.last();
    	    	
    	    	Integer period = minPeriod; 
    	    	while (period <= maxPeriod) {
    	    		comparePeriod(period, usl);
    	    		// Период +1
    	    		period = Integer.valueOf(Utl.addMonth(String.valueOf(period), 1));
    	    	}
    	    	
    	    	// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
    	    	checkLastUsed(maxPeriod, usl);
    		}
			
		} else {
    		// Сжимать без использования кода услуги USL
			lastUsed = null;
	    	// Получить все периоды, отсортированные по mg1
	    	Set<Integer> lstPeriodPrep = lst.stream().map(t -> t.getMg1()).collect(Collectors.toSet());
	    	// Получить все периоды mg1, уникальные
	    	lstPeriod = new TreeSet<Integer>();
	    	lstPeriod.addAll(lstPeriodPrep.stream().distinct().collect(Collectors.toSet()));
	    	minPeriod = lstPeriod.first();
	    	maxPeriod = lstPeriod.last();
	    	
	    	Integer period = minPeriod; 
	    	while (period <= maxPeriod) {
	    		comparePeriod(period, null);
	    		// Период +1
	    		period = Integer.valueOf(Utl.addMonth(String.valueOf(period), 1));
	    	}
	    	
	    	// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
	    	checkLastUsed(maxPeriod, null);
			
		}
    	
		//log.info("ОКОНЧАНИЕ сжатия лиц.счет:{}", lsk);
    	return new AsyncResult<Result>(res);
    }

    /**
     * Проверка, установка в последнем обработанном массиве корректность замыкающего периода mg2
     */
    private void checkLastUsed(Integer period, String usl) {
		lst.stream().filter(t -> usl == null || t.getUsl().equals(usl))
				.filter(t -> t.getMg1().equals(lastUsed))
				.filter(t -> !t.getMg2().equals(period) && isByUsl 
				
				).forEach(d -> {
			// Проставить корректный замыкающий период
			d.setMg2(period);
		});
	}

    /**
     * Сравнить массив
     * @param period - период массива 
     * @param usl - код услуги
     */
	private void comparePeriod(Integer period, String usl) {
    	if (lastUsed == null && lstPeriod.contains(period)) {
    		// последнего массива нет, но период содержится в общем списке 
    		lastUsed = period;
    	} else if (lastUsed != null && !lstPeriod.contains(period)) {
    		// последний массив есть, но проверяемый период не содержится в общем списке (разрыв периода) 
    		// проставить в заключительном периоде последнего массива, период -1
			replacePeriod(lastUsed, Integer.valueOf(Utl.addMonth(String.valueOf(period), -1)), usl);
			lastUsed = null;
    	} else {
    		// сравнить новый массив с последним
    		if (comparePeriod(period, lastUsed, usl)) {
    			// элементы совпали, удалить элементы сравниваемого массива
    			delPeriod(period, usl);
    		} else {
    			// элементы разные, закрыть в последнем период действия
    			replacePeriod(lastUsed, Integer.valueOf(Utl.addMonth(String.valueOf(period), -1)), usl);
    			// пометить период нового массива как замыкающий
    			lastUsed = period;
    		}
    	}
    }

	/**
	 * Удаление элементов массива
	 * @param period - период массива
     * @param usl - код услуги
	 */
    private void delPeriod(Integer period, String usl) {
    	List<Compress> lstDel = lst.stream()
    			.filter(t -> usl == null || t.getUsl().equals(usl))
    			.filter(t -> t.getMg1().equals(period)).collect(Collectors.toList());
    	
		for (Iterator<Compress> iterator = lstDel.iterator(); iterator.hasNext();) {
			em.remove(iterator.next());
		}
		
	}

	/**
     * Проставить в одном массиве период действия, на другой период
     * @param period1 - Период расширяемый
     * @param period2 - Период новый
     * @param usl - код услуги
     */
    private void replacePeriod(Integer period1, Integer period2, String usl) {
    	// Найти массив по period1, и чтобы он уже не был расширен до period2
		lst.stream()
			.filter(t -> usl == null || t.getUsl().equals(usl))
		    .filter(t -> t.getMg1().equals(period1) && !t.getMg2().equals(period2)).forEach(d -> {
			d.setMg2(period2);
		});
	}
    
    /**
     * Сравнить элементы одного массива с другим
     * @param period1 - Период 1
     * @param period1 - Период 2
     * @param usl - код услуги
     * @return 
     * @return 
     */
    private boolean comparePeriod(Integer period1, Integer period2, String usl) {
		List<Compress> filtLst = lst.stream()
				.filter(t -> usl == null || t.getUsl().equals(usl))
				.filter(t -> t.getMg1().equals(period1)).collect(Collectors.toList());
		for (Compress t: filtLst) {
			if (!searchElement(t, period2, usl)) {
				// не найден в точности хотя бы один элемент - выход
				return false;
			}
		}
		// все элементы найдены, - коллекции одинаковы
		return true;
	}

    /**
     * Найти похожий элемент в списке других элементов
     * @param elem - Элемент
     * @param period - Период поиска
     * @param usl - код услуги
     * @return
     */
    private Boolean searchElement(Compress elem, Integer period, String usl) {
		Compress foundElem = lst.stream()
				.filter(t -> usl == null || t.getUsl().equals(usl))
				.filter(t -> t.getMg1().equals(period))
				.filter(t -> t.same(elem)).findFirst().orElse(null);
		if (foundElem == null) {
			return false;
		} else {
			return true;
		}
    }
    
}
