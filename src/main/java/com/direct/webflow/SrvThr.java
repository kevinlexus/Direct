package com.direct.webflow;

import java.util.ArrayList;
import java.util.List;

import com.dic.bill.model.scott.SprGenItm;
import com.dic.bill.model.scott.TempObj;
import com.dic.bill.utils.DSess;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class SrvThr {

	private static int errChild; // возникла ли ошибка в порождённых потоках? (0-нет, 1-да)
	private static String errTextChild; // описание ошибки возникшей в порожд.потоках
	// общий список объектов для выполнения потоками
	static List<TempObj> tobj;
	static double progress, initCnt; // прогресс выполнения локальный, начальное кол-во объектов
	static SprGenItm itm; // пункт меню, который формируется
	static DSess ds;// сессия из ThrMain
	// вернуть следующий объект из списка
	static synchronized TempObj getNextObj() {
		TempObj retObj = null;
		for (TempObj t : tobj) {
			retObj = t;
			// удалить объект, как "отработанный"
			tobj.remove(t);
			//обновить общий прогресс выполнения
			WebCtrl.incProgress();
			//прогресс локальный
			if (initCnt != 0){
				progress=1-tobj.size()/initCnt;
			} else {
				progress=1;
			}
			ds.beginTrans();
			itm.setProc(progress);
			ds.commitTrans();

			break;
		}
		return retObj; // вернуть или объект или null
	}

	/**
	 * Класс обслуживающий потоки
	 * @param cnt Кол-во потоков
	 * @param var Вариант формирования
	 */
	public SrvThr(DSess ds, List<TempObj> tobj, SprGenItm itm, int cnt, int var) {
		//добавить необходимое кол-во потоков
		SrvThr.itm=itm;
		SrvThr.tobj=tobj;
		SrvThr.ds=ds;
		setErrChild(0); // сбросить ошибки потоков
		List<ThrGen> trl = new ArrayList<ThrGen>();
		SrvThr.initCnt=tobj.size(); // начальное кол-во объектов
		for (int a=1; a <=cnt; a++) {
			
			ThrGen ttr=new ThrGen("ThrGen-"+a, var);
			
			ttr.start();
			trl.add(ttr);
		}
		
		//ждать, когда все потоки выполнятся
		for (ThrGen t : trl) {
			try {
				t.join();
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		//выход
		log.info("SrvThr - exiting");
	}

	public static int getErrChild() {
		return errChild;
	}

	public static void setErrChild(int errChild) {
		SrvThr.errChild = errChild;
	}

	public static String getErrTextChild() {
		return errTextChild;
	}

	public static void setErrTextChild(String errTextChild) {
		SrvThr.errTextChild = errTextChild;
	}

	
}
