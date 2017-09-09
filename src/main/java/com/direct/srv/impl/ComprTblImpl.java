package com.direct.srv.impl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import javax.persistence.EntityManager;
import javax.persistence.PersistenceContext;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.scheduling.annotation.AsyncResult;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.dic.bill.Compress;
import com.dic.bill.dao.AnaborDAO;
import com.dic.bill.model.scott.Anabor;
import com.direct.excp.WrongTableException;
import com.direct.srv.ComprTbl;
import com.direct.webflow.Result;
import com.direct.webflow.WebCtrl;

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
	
	/**
     * Сжать таблицу, содержащую mg1, mg2
	 * @param lsk - лиц.счет
	 * @param curPeriod - текущий период
	 * @throws WrongTableException 
	 */
    @Transactional
    
	public Future<Result> comprTableByLsk(Class tableClass, String lsk, Integer curPeriod) {
		log.info("Сжатие таблицы, лиц.счет:{}", lsk);
    	Result res = new Result(0, lsk);
		lastUsed = null;
    	this.curPeriod = curPeriod;
    	// Все массивы периодов по лиц.сч.
    	lst = new ArrayList<Compress>();
    	lstPeriod = new TreeSet<Integer>();
 	   	// Получить все элементы по лиц.счету
    	if (tableClass == Anabor.class) {
        	lst.addAll(anaborDao.getByLsk(lsk));
    	} else {
    		// Ошибка - не тот класс таблицы
    		res.setErr(2);
    		return new AsyncResult<Result>(res);
    	}
    	// Получить все периоды mg1, отсортированные и уникальные
    	Set<Integer> lstPeriodPrep = lst.stream().map(t -> t.getMg1()).collect(Collectors.toSet());
    	lstPeriod.addAll(lstPeriodPrep.stream().distinct().collect(Collectors.toSet()));
    	
    	for (Integer period: lstPeriod) {
    		comparePeriod(period);
    	}
    	
    	// Проверить, установить в последнем обработанном массиве корректность замыкающего периода mg2
    	checkLastUsed();
    	
    	return new AsyncResult<Result>(res);
    }

    /**
     * Проверка, установка в последнем обработанном массиве корректность замыкающего периода mg2
     */
    private void checkLastUsed() {
		lst.stream().filter(t -> t.getMg1().equals(lastUsed)).filter(t -> !t.getMg2().equals(curPeriod)).forEach(d -> {
			// Проставить корректный замыкающий период
			d.setMg2(curPeriod);
		});
	}

    /**
     * Сравнить массив
     * @param period - период массива 
     */
	private void comparePeriod(Integer period) {
    	if (lastUsed == null) {
    		// последнего массива нет, использовать новый
    		lastUsed = period;
    	} else {
    		// сравнить новый массив с последним
    		if (comparePeriod(period, lastUsed)) {
    			// элементы идентичны, расширить в последнем период действия
    			expandPeriod(lastUsed, period);
    			// удалить элементы сравниваемого массива
    			delPeriod(period);
    		} else {
    			// элементы разные, пометить период нового массива как замыкающий
    			lastUsed = period;
    		}
    	}
    }

	/**
	 * Удаление элементов массива
	 * @param period - период массива
	 */
    private void delPeriod(Integer period) {
    	List<Compress> lstDel = lst.stream().filter(t -> t.getMg1().equals(period)).collect(Collectors.toList());
    	
		for (Iterator<Compress> iterator = lstDel.iterator(); iterator.hasNext();) {
			em.remove(iterator.next());
		}
		
	}

	/**
     * Расширить в одном массиве период действия, на другой период
     * @param period1 - Период расширяемый
     * @param period2 - Период новый
     */
    private void expandPeriod(Integer period1, Integer period2) {
    	// Найти массив по period1, и чтобы он уже не был расширен до period2
		lst.stream().filter(t -> t.getMg1().equals(period1) && !t.getMg2().equals(period2)).forEach(d -> {
			d.setMg2(period2);
		});
	}
    
    /**
     * Сравнить элементы одного массива с другим
     * @param period1 - Период 1
     * @param period1 - Период 2
     * @return 
     * @return 
     */
    private boolean comparePeriod(Integer period1, Integer period2) {
		List<Compress> filtLst = lst.stream().filter(t -> t.getMg1().equals(period1)).collect(Collectors.toList());
		for (Compress t: filtLst) {
			if (!searchElement(t, period2)) {
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
     * @return
     */
    private Boolean searchElement(Compress elem, Integer period) {
		Compress foundElem = lst.stream().filter(t -> t.getMg1().equals(period)).filter(t -> t.same(elem)).findFirst().orElse(null);
		if (foundElem == null) {
			return false;
		} else {
			return true;
		}
    }
    
}
