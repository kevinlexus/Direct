package com.direct.srv.impl;

import java.util.ArrayList;
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
	// периоды последнего обработанного массива
	private Integer lastUsed;
	// isByUsl - использовать ли поле "usl" для критерия сжатия
	boolean isByUsl;
	private Map<Integer, Integer> lstPeriodPrep;
	// текущий л.с.
	String lsk;
	
	/**
     * Сжать таблицу, содержащую mg1, mg2
	 * @param lsk - лиц.счет
	 * @param curPeriod - текущий период
	 * @param isAllPeriods - использование начального периода: 
	 *    (если false, то месяц -1 от текущего, если true - с первого минимального по услуге)
	 * @param isByUsl - использовать ли поле "usl" для критерия сжатия
	 * @throws WrongTableException 
	 */
	@Async //- Async чтобы организовался поток
    @Transactional
	public Future<Result> comprTableByLsk(Class tableClass, String lsk, Integer curPeriod, boolean isAllPeriods, boolean isByUsl) {
		log.trace("Л.с.:{} Начало сжатия!", lsk);
		this.lsk = lsk;
		this.isByUsl = isByUsl;
    	Result res = new Result(0, lsk);
    	// Период -2 от текущего (минус два месяца, так как сжимаем только архивный и сравниваем его с доархивным)
    	Integer backPeriod = Integer.valueOf(Utl.addMonth(String.valueOf(curPeriod), -2));
		log.trace("Л.с.:{} Период -1 от текущего:{}", lsk, backPeriod);
    	lst = new ArrayList<Compress>();
    	// Список услуг
    	Set<String> lstUsl = new TreeSet<String>();
    	// Минимальный, максимальный период
    	Integer minPeriod, maxPeriod;
 	   	// Получить все элементы по лиц.счету
    	if (tableClass == Anabor.class) {
    		if (isAllPeriods) {
    			// получить все периоды
            	lst.addAll(anaborDao.getByLsk(lsk));
        		log.trace("Л.с.:{} По всем периодам элементы получены!", lsk, backPeriod);
    		} else {
    			// начиная с периода -2
            	lst.addAll(anaborDao.getByLskPeriod(lsk, backPeriod));
        		log.trace("Л.с.:{} По по периоду начиная с -2 элементы получены!", lsk, backPeriod);
    		}
    	} else {
    		// Ошибка - не тот класс таблицы
    		log.error("Л.с.:{} Ошибка! Не тот класс таблицы:{}", lsk, tableClass);
    		res.setErr(2);
    		return new AsyncResult<Result>(res);
    	}
    	
    	// Список всех услуг
    	lstUsl.addAll(lst.stream().map(t -> t.getUsl()).distinct().collect(Collectors.toSet()));
		log.trace("Л.с.:{} список найденных услуг:", lsk);
		lstUsl.stream().forEach(t-> {
			log.trace("Л.с.:{} услуга:{}", lsk, t);
		});
		
    	if (isByUsl) {
    		// Сжимать с использованием кода услуги USL
    		for (String usl :lstUsl) {
    			lastUsed = null;
    	    	// Получить все периоды, фильтр - по услуге
    	    	
    			minPeriod = lst.stream().filter(d -> d.getUsl().equals(usl)).map(t -> t.getMg1()).min(Integer::compareTo).orElse(null);
    			maxPeriod = lst.stream().filter(d -> d.getUsl().equals(usl) && d.getMg1() < curPeriod) // не включая текущий период
    					.map(t -> t.getMg2()).max(Integer::compareTo).orElse(null);
    			log.trace("Л.с.:{} мин.период:{}, макс.период:{}", lsk, minPeriod, maxPeriod);
    			
    			// Получить все периоды mg1, уникальные - по услуге,
    	    	// отсортированные
    			lstPeriodPrep = lst.stream().filter(d -> d.getUsl().equals(usl) && d.getMg1() < curPeriod)
    										.collect(Collectors.toMap(t->t.getMg1(), t->t.getMg2()));
    			lstPeriod = new TreeSet<Integer>();
    			lstPeriod.addAll(lstPeriodPrep.keySet().stream().collect(Collectors.toSet()));

    			lstPeriod.stream().forEach(t-> {
    	    		checkPeriod(t, usl);
    				
    			});
    	    	// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
    	    	checkLastUsed(maxPeriod, usl);
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
     * Проверка, установка в последнем обработанном массиве корректность замыкающего периода mg2, если он уже не является сжатым (mg1 == mg2)
     */
    private void checkLastUsed(Integer period, String usl) {
		lst.stream().filter(t -> usl == null || t.getUsl().equals(usl))
				.filter(t -> t.getMg1().equals(lastUsed))
				.filter(t -> t.getMg1().equals(t.getMg2()))
				.filter(t -> !t.getMg2().equals(period) && isByUsl 
				
				).forEach(d -> {
			// Проставить корректный замыкающий период
			d.setMg2(period);
		});
	}

    /**
     * Проверить массив
     * @param period - период массива 
     * @param usl - код услуги
     */
	private void checkPeriod(Integer period, String usl) {
		log.trace("Л.с.:{}, usl={} проверяемый.период:{}", this.lsk, usl, period);
		Integer lastUsedMg2 = lstPeriodPrep.get(lastUsed);
		// Период -1 от проверяемого
		Integer chkPeriod = Integer.valueOf(Utl.addMonth(String.valueOf(period),-1));
				
    	if (lastUsed == null) {
    		// последнего массива нет 
    		lastUsed = period;
    		log.trace("Л.с.:{}, usl={} последнего периода нет, сохранили:{}", this.lsk, usl, period);
    	} else if (lastUsed != null && !chkPeriod.equals(lastUsedMg2)) {
    		// последний массив есть, но проверяемый период имеет дату начала большую чем на 1 месяц чем в последнем массиве (GAP)
    		// проставить в заключительном периоде последнего массива, период -1
			replacePeriod(lastUsed, Integer.valueOf(Utl.addMonth(String.valueOf(period), -1)), usl);
			lastUsed = period;
    		log.trace("Л.с.:{}, usl={} найден GAP:{}", this.lsk, usl, period);
    	} else {
    		// сравнить новый массив с последним
    		if (comparePeriod(period, lastUsed, usl)) {
    			// элементы совпали, удалить элементы сравниваемого массива
    			delPeriod(period, usl);
    			// Расширить заключительный период последнего массива
    			lstPeriodPrep.put(lastUsed, period);
        		log.trace("Л.с.:{}, usl={} элементы совпали:{}", this.lsk, usl, period);
    		} else {
    			// элементы разные, закрыть в последнем период действия
    			replacePeriod(lastUsed, Integer.valueOf(Utl.addMonth(String.valueOf(period), -1)), usl);
    			// пометить период нового массива как замыкающий
    			lastUsed = period;
        		log.trace("Л.с.:{}, usl={} элементы разные:{}", this.lsk, usl, period);
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
