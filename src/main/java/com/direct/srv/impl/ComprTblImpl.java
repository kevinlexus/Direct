package com.direct.srv.impl;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.apache.commons.collections4.CollectionUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.Async;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dic.bill.Compress;
import com.dic.bill.dao.AchargeDAO;
import com.dic.bill.dao.AnaborDAO;
import com.dic.bill.model.scott.Acharge;
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
	@Autowired
	private AchargeDAO achargeDao;

	// Все элементы по лиц.счету по всем периодам
	List<Compress> lst;
	// отсортированный список периодов  
	SortedSet<Integer> lstPeriod;
	// периоды последнего обработанного массива
	private Integer lastUsed;
	// isByUsl - использовать ли поле "usl" для критерия сжатия
	boolean isByUsl;
	// массив диапазонов периодов mg1, mg2
	private Map<Integer, Integer> lstPeriodPrep;
	// текущий л.с.
	String lsk;
	
	/**
     * Сжать таблицу, содержащую mg1, mg2
	 * @param lsk - лиц.счет
	 * @param table - таблица для сжатия
	 * @param curPeriod - текущий период
	 * @param isAllPeriods - использование начального периода: 
	 *    (если false, то месяц -1 от текущего, если true - с первого минимального по услуге)
	 * @param isByUsl - использовать ли поле "usl" для критерия сжатия
	 * @throws WrongTableException 
	 */
	@Async //- Async чтобы организовался поток
    @Transactional
	public Future<Result> comprTableByLsk(String table, String lsk, Integer curPeriod, boolean isAllPeriods, boolean isByUsl) {
		log.trace("Л.с.:{} Начало сжатия!", lsk);
		this.lsk = lsk;
		this.isByUsl = isByUsl;
    	Result res = new Result(0, lsk);
    	// Период -2 от текущего (минус два месяца, так как сжимаем только архивный и сравниваем его с доархивным)
    	Integer backPeriod = Integer.valueOf(Utl.addMonth(String.valueOf(curPeriod), -2));
		log.trace("Л.с.:{} Период -1 от текущего:{}", lsk, backPeriod);
    	lst = new ArrayList<Compress>();
    	// Список ключей
    	Set<Integer> lstKey = new TreeSet<Integer>();
    	// Минимальный, максимальный период
    	Integer minPeriod, maxPeriod;
 	   	// Получить все элементы по лиц.счету
    	if (table.equals("anabor")) {
    		if (isAllPeriods) {
    			// получить все периоды
            	lst.addAll(anaborDao.getByLsk(lsk));
        		log.trace("Л.с.:{} По всем периодам элементы получены!", lsk, backPeriod);
    		} else {
    			// начиная с периода -2
            	lst.addAll(anaborDao.getByLskPeriod(lsk, backPeriod));
        		log.trace("Л.с.:{} По по периоду начиная с -2 элементы получены!", lsk, backPeriod);
    		}
    	} if (table.equals("acharge")) {
        		if (isAllPeriods) {
        			// получить все периоды
                	lst.addAll(achargeDao.getByLsk(lsk));
            		log.trace("Л.с.:{} По всем периодам элементы получены!", lsk, backPeriod);
        		} else {
        			// начиная с периода -2
                	lst.addAll(achargeDao.getByLskPeriod(lsk, backPeriod));
            		log.trace("Л.с.:{} По по периоду начиная с -2 элементы получены!", lsk, backPeriod);
        		}
    	} else {
    		// Ошибка - не тот класс таблицы
    		log.error("Л.с.:{} Ошибка! Не тот класс таблицы:{}", lsk, table);
    		res.setErr(2);
    		return new AsyncResult<Result>(res);
    	}
    	
    	// Список всех услуг
    	lstKey.addAll(lst.stream().map(t -> t.getKey()).distinct().collect(Collectors.toSet()));
		log.trace("Л.с.:{} список найденных ключей:", lsk);
		lstKey.stream().forEach(t-> {
			log.trace("Л.с.:{} ключ:{}", lsk, t);
		});
		
    	if (isByUsl) {
    		// Сжимать с использованием услуги
    		for (Integer key :lstKey) {
    			lastUsed = null;
    	    	// Получить все периоды, фильтр - по услуге
    	    	
    			minPeriod = lst.stream().filter(d -> d.getKey().equals(key)).map(t -> t.getMgFrom()).min(Integer::compareTo).orElse(null);
    			maxPeriod = lst.stream().filter(d -> d.getKey().equals(key) && d.getMgFrom() < curPeriod) // не включая текущий период
    					.map(t -> t.getMgTo()).max(Integer::compareTo).orElse(null);
    			log.trace("Л.с.:{} мин.период:{}, макс.период:{}", lsk, minPeriod, maxPeriod);
    			
    			// Получить все диапазоны периодов mgFrom, mgTo уникальные - по ключу,
    	    	// отсортированные
    			lstPeriodPrep = new HashMap<Integer, Integer>();
    			
    			lst.stream().filter(t -> t.getKey().equals(key) && t.getMgFrom() < curPeriod).forEach(t-> {
    				if (lstPeriodPrep.get(t.getMgFrom()) == null) {
    					lstPeriodPrep.put(t.getMgFrom(), t.getMgTo());
    				}
    			});
    			
    			lstPeriod = new TreeSet<Integer>();
    			lstPeriod.addAll(lstPeriodPrep.keySet().stream().collect(Collectors.toSet()));

    			lstPeriod.stream().forEach(t-> {
    	    		checkPeriod(t, key);
    				
    			});
    			
    			// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
    			replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
    		}
			
		}/* else {
    		// Сжимать без использования кода услуги USL
			lastUsed = null;
	    	// Получить все периоды, отсортированные по mg1
	    	Set<Integer> lstPeriodPrep = lst.stream().map(t -> t.getMg1()).collect(Collectors.toSet());
	    	// Получить все периоды mg1, уникальные
	    	lstPeriod = new TreeSet<Integer>();
	    	lstPeriod.addAll(lstPeriodPrep.stream().distinct().collect(Collectors.toSet()));
	    	minPeriod = lstPeriod.first();
	    	maxPeriod = lstPeriod.last();
	    	if (maxPeriod >= curPeriod) {
	    		// не включая текущий период!!! Строго! Иначе будут некорректно добавляться периоды из программы Delphi!
	    		maxPeriod = Integer.valueOf(Utl.addMonth(String.valueOf(curPeriod), -1));
	    	}
	    	
	    	Integer period = minPeriod; 
	    	while (period <= maxPeriod) {
	    		comparePeriod(period, null);
	    		// Период +1
	    		period = Integer.valueOf(Utl.addMonth(String.valueOf(period), 1));
	    	}
	    	
	    	// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
	    	checkLastUsed(maxPeriod, null);
			
		}*/
    	
		//log.trace("ОКОНЧАНИЕ сжатия лиц.счет:{}", lsk);
    	return new AsyncResult<Result>(res);
    }

    /**
     * Проверить массив
     * @param period - период массива 
     * @param key - код услуги
     */
	private void checkPeriod(Integer period, Integer key) {
		log.trace("Л.с.:{}, key={} проверяемый.период:{}", this.lsk, key, period);
		Integer lastUsedMg2 = lstPeriodPrep.get(lastUsed);
		// Период -1 от проверяемого
		Integer chkPeriod = Integer.valueOf(Utl.addMonth(String.valueOf(period),-1));
				
    	if (lastUsed == null) {
    		// последнего массива нет, сохраняем как новый
    		lastUsed = period;
    		log.trace("Л.с.:{}, key={} последнего периода нет, сохранили:{}", this.lsk, key, period);
    	} else if (lastUsed != null && !chkPeriod.equals(lastUsedMg2)) {
    		// последний массив есть, но проверяемый период имеет дату начала большую чем на 1 месяц относительно последнего массива (GAP)
    		// проставить в заключительном периоде последнего массива замыкающий месяц 
			replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
			lastUsed = period;
    		log.trace("Л.с.:{}, key={} найден GAP:{}", this.lsk, key, period);
    	} else {
    		// сравнить новый массив с последним
    		if (comparePeriod(period, lastUsed, key)) {
    			// элементы совпали, удалить элементы сравниваемого массива
    			delPeriod(period, key);
    			// Расширить заключительный период последнего массива на mg2 сравниваемого массива
    			lstPeriodPrep.put(lastUsed, lstPeriodPrep.get(period));
        		log.trace("Л.с.:{}, key={} элементы совпали:{}", this.lsk, key, period);
    		} else {
    			// элементы разные, закрыть в последнем период действия
    			replacePeriod(lastUsed, lstPeriodPrep.get(lastUsed), key);
    			// пометить период нового массива как замыкающий
    			lastUsed = period;
    			// сохранять не надо mg2, так как уже записано это при инициализации массива
        		log.trace("Л.с.:{}, key={} элементы разные:{}", this.lsk, key, period);
    		}
    	}
    }

	/**
	 * Удаление элементов массива
	 * @param period - период массива
     * @param key - код ключа
	 */
    private void delPeriod(Integer period, Integer key) {
    	List<Compress> lstDel = lst.stream()
    			.filter(t -> key == null || t.getKey().equals(key))
    			.filter(t -> t.getMgFrom().equals(period)).collect(Collectors.toList());
    	
		for (Iterator<Compress> iterator = lstDel.iterator(); iterator.hasNext();) {
			em.remove(iterator.next());
		}
		
	}

	/**
     * Проставить в одном массиве период действия, на другой период
     * @param period1 - Период расширяемый
     * @param period2 - Период новый
     * @param key - код ключа
     */
    private void replacePeriod(Integer period1, Integer period2, Integer key) {
    	// Найти массив по period1, и чтобы он еще не был расширен до period2
		lst.stream()
			.filter(t -> key == null || t.getKey().equals(key))
		    .filter(t -> t.getMgFrom().equals(period1) && !t.getMgTo().equals(period2)).forEach(d -> {
			d.setMgTo(period2);
		});
	}
    
    /**
     * Сравнить элементы одного массива с другим
     * @param period1 - Период 1
     * @param period1 - Период 2
     * @param key - код ключа
     * @return 
     * @return 
     */
    private boolean comparePeriod(Integer period1, Integer period2, Integer key) {
		// Получить коллекции hash и сравнить их между собой
    	List<Integer> filtLst1 = lst.stream()
				.filter(t -> key == null || t.getKey().equals(key))
				.filter(t -> t.getMgFrom().equals(period1)).map(t-> t.getHash()).collect(Collectors.toList());
		List<Integer> filtLst2 = lst.stream()
				.filter(t -> key == null || t.getKey().equals(key))
				.filter(t -> t.getMgFrom().equals(period2)).map(t-> t.getHash()).collect(Collectors.toList());
		if (filtLst1.size() != filtLst2.size()) {
			// не равны по размеру
			return false;
		}
		return CollectionUtils.isEqualCollection(filtLst1, filtLst2);
	}

}
