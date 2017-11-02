package com.direct.webflow;

import java.util.Date;
import java.util.List;

import com.dic.bill.dao.impl.SprGenItmDaoImpl;
import com.dic.bill.dao.impl.TempObjDaoImpl;
import com.dic.bill.model.scott.SprGenItm;
import com.dic.bill.model.scott.TempObj;
import com.dic.bill.utils.DSess;
import com.direct.srv.MntBase;
import com.direct.srv.impl.MntBaseImpl;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ThrMain extends Thread {
	SprGenItmDaoImpl sprgDao;
	List<SprGenItm> sprg;
	String name; // наименование потока
	ExecProc ex;
	
	static boolean stopped = false;
	DSess ds;

	// конструктор
	ThrMain() {
		log.info("Creating ThrMain!");
		name="ThrMain";
	}


	// этот поток должен создать объект содержащий дома которые нужно
	// сформировать, потоки ThrGen должны "разбирать" у этого объекта дома для
	// формир.
	public void run() {
		ds = new DSess(true); // одна сессия на весь run()
		ex = new ExecProc(ds); 
		sprgDao = new SprGenItmDaoImpl(ds.sess);
		sprg = sprgDao.findAllChecked();

		// формировать
		gen();

		log.info("Thread ThrMain exiting.");
		ds.closeSess();
		setStopped(true);
	}

	/**
	 * подготовка к закрытию формирования, снятие блокировки итогового
	 * @param menuGenItg
	 */
	private void closeGen(SprGenItm menuGenItg, int stateGen, int err, String ErrText) {
		int ret = ex.runWork(2, 0, 0);// Снять блокировку после итогового формирования (можно снимать, даже если его не было)
		if (ret==-1) {
			log.info("ThrMain.closeGen: Ошибка во время разблокировки итогового формирования!");
		}
		
		//начать транзакцию, если не начата
		if (!ds.isActiveTrans()) {
			ds.beginTrans();
		}
		menuGenItg.setErr(err);
		menuGenItg.setState(ErrText);
		menuGenItg.setDt2(new Date());
		WebCtrl.stateGen=String.valueOf(stateGen);
		WebCtrl.incProgress();

		ds.commitTrans();
		ds.closeSess();
		
	}

	//вынес модуль проверки-1 (smpl_chk)
	//проверка 4
	private boolean check(SprGenItm menuGenItg, int var){
		int ret = ex.runWork(var, 0, 0);
		switch (ret) {
		case -1: {
			//Ошибка во время вызова
			closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
			return true; // выйти при ошибке с ошибкой
		}
		case 1: {
			closeGen(menuGenItg, 2, 2, "Найдены ошибки до итогового формирования");
			return true; // выйти при ошибке с ошибкой
		}
		}
		return false; // выйти без ошибки
	}

	//вынес модуль проверки-2
	private boolean check2(SprGenItm menuGenItg, int var){
		//проверка gen_check-1
		int ret = ex.runWork(var, 0, 0);
		switch (ret) {
		case -1: {
			//Ошибка во время вызова
			closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
			return true; // выйти при ошибке с ошибкой
		}
		case 1: {
			closeGen(menuGenItg, 2, 1, ex.doWorkErrText);
			return true; // выйти при ошибке с ошибкой
		}
		}
		
		return false; // выйти при ошибке без ошибки
	}
	

	//вынес модуль простого формирования
	private boolean smplGen(SprGenItm menuGenItg, SprGenItm itm, int var, Double proc) {
		if (ThrMain.isStopped()) {
			//остановиться, если отмена формирования
			closeGen(menuGenItg, 2, 1, "ThrMain: Формирование остановлено");
			return true; // выйти
		}
		ds.beginTrans();
		itm.setDt1(new Date());
		int ret = ex.runWork(var, 0, 0);
		if (ret==-1) {
			//Ошибка во время вызова
			closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
			return true; // выйти при ошибке с ошибкой
		}
		itm.setProc(proc);
		itm.setDt2(new Date());
		WebCtrl.incProgress();
		ds.commitTrans();
		return false;
	}
	
	
	//*****************ФОРМИРОВАНИЕ
	private void gen() {
		int ret=-1;
		SprGenItm menuGenItg, menuMonthOver, menuCheckBG;
		List<TempObj> tobj=null;
		@SuppressWarnings("unused")
		SrvThr srv;
		menuGenItg = sprgDao.getByCd("GEN_ITG");
		menuMonthOver = sprgDao.getByCd("GEN_MONTH_OVER");
		menuCheckBG = sprgDao.getByCd("GEN_CHECK_BEFORE_GEN");

	
		//**********почистить ошибку последнего формирования, % выполнения
		ds.beginTrans();

		//почистить % выполнения
		for (SprGenItm itm : sprg) {
			itm.setProc((double) 0);
			itm.setState(null);
			itm.setDt1(null);
			itm.setDt2(null);
		}
		menuGenItg.setState(null);
		menuGenItg.setErr(0);
		menuGenItg.setState(null);
		menuGenItg.setDt1(new Date());
		ds.commitTrans();
		//**********
		WebCtrl.incProgress();
		
		//**********установить дату формирования
		ds.beginTrans();
		 if (ex.runWork(16, 0, 0)!=0) {
			closeGen(menuGenItg, 2, 1, "ThrMain: Некорректная дата формирования!");
			return; // выйти при ошибке
		 }
		ds.commitTrans();

		//**********(Установить блокировку для итогового формирования)
		if (menuGenItg.getSel()) {
			// выбрано итоговое формирование
			ds.beginTrans();
			ret = ex.runWork(1, 0, 0);// Установить блокировку
			if (ret == 1) {
				//формирование уже запущено
				closeGen(menuGenItg, 2, 1, "ThrMain: Формирование уже запущено!");
				return; // выйти при ошибке
			} else if (ret==-1) {
				//Ошибка во время вызова
				closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
				return; // выйти при ошибке
			}
			// выполнить проверки
			log.info("ThrMain: Начало формир!");
			menuGenItg.setState("Выполняется");
			menuGenItg.setProc(0.1); //установить 10% выполнения
			ds.commitTrans();
		} 
		
		if (menuGenItg.getSel()) {
			
		}

		// сбросить признак что сформировано итоговое (в конце, если всё успешно - поставить)
		// если не переход месяца!
		if (!menuMonthOver.getSel()) {
			ds.beginTrans();
			ret = ex.runWork(18, 0, 0);
			if (ret==-1) {
				//Ошибка во время вызова
				closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
				return; // выйти при ошибке
			}
			ds.commitTrans();
			//**********
		}

		WebCtrl.stateGen="1"; //Выполняется формирование
		WebCtrl.incProgress();

		//**********Закрыть базу для пользователей
		if (menuGenItg.getSel()) {
			ds.beginTrans();
			ret = ex.runWork(3, 0, 0);
			if (ret==-1) {
				//Ошибка во время вызова
				closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
				return; // выйти при ошибке
			}
			log.info("ThrMain: Установлен признак закрытия базы!");
			ds.commitTrans();
		}
		//**********

		//**********Проверки до формирования
		if (menuCheckBG.getSel()) {
			// если выбраны проверки, а они как правило д.б. выбраны при итоговом

			//проверка p_thread.smpl_chk
			
			//новая проверка, на список домов в разных УК, по которым обнаружены открытые лицевые счета  
			if (check(menuGenItg, 38)) {
				return;
			}

			if (check(menuGenItg, 4)) {
				return;
			}
			ds.beginTrans(); 
			menuCheckBG.setProc(0.1);
			menuCheckBG.setDt1(new Date());
			ds.commitTrans();
			WebCtrl.incProgress();

			if (check(menuGenItg, 5)) {
				return;
			}
			if (check(menuGenItg, 6)) {
				return;
			}
			if (check(menuGenItg, 7)) {
				return;
			}
			ds.beginTrans(); 
			menuCheckBG.setProc(0.5);	
			ds.commitTrans();
			WebCtrl.incProgress();

			//**********Проверки gen.gen_check
			//проверка корректности л.с.
			if (check2(menuGenItg, 12)) { //5
				return;
			}
			ds.beginTrans(); 
			menuCheckBG.setProc(0.7);	
			ds.commitTrans();
			WebCtrl.incProgress();

			//основные проверки
			if (check2(menuGenItg, 8)) { //1
				return;
			}
			//корректность периода в ЛК
			if (check2(menuGenItg, 15)) { //8
				return;
			}
			ds.beginTrans(); 
			menuCheckBG.setProc(1.0);	
			menuCheckBG.setDt2(new Date());
			ds.commitTrans();
			WebCtrl.incProgress();
		}

		if (menuMonthOver.getSel()) {
			// выбран переход месяца
			//основные проверки
			if (check2(menuGenItg, 8)) { //1
				return;
			}
			//выполнено ли итоговое
			if (check2(menuGenItg, 10)) { //3
				return;
			}
			
			//проверка сальдо
			if (check2(menuGenItg, 11)) { //4
				return;
			}

			//получение счетчиков
			if (check2(menuGenItg, 14)) { //7
				return;
			}
			
		}				

		//**********Начать формирование
		for (SprGenItm itm : sprg) {
				log.info("Generating menu item: " + itm.getCd());
				switch (itm.getCd()) {

				case "GEN_ADVANCE": {
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();
					// переформировать авансовые платежи
					ret = ex.runWork(36, 0, 0);
					if (ret==-1) {
						// Ошибка во время вызова
						closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
						return; // выйти при ошибке
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					WebCtrl.incProgress();
					ds.commitTrans();
					break;
				}					

				case "GEN_DIST_VOLS1": {
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();
					//чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
					ret = ex.runWork(17, 0, 0);
					if (ret==-1) {
						//Ошибка во время вызова
						closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
						return; // выйти при ошибке
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					WebCtrl.incProgress();
					ds.commitTrans();
					break;
				}					
				case "GEN_DIST_VOLS2": {
					//распределить где нет ОДПУ
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();
					tobj = new TempObjDaoImpl().findAll(2); //найти вводы
					srv = new SrvThr(ds, tobj, itm, 10, 2); // запустить N-потоков, var=2 -
													// по вводам, ждать выполнения
					if (SrvThr.getErrChild() !=0){
						//произошла ошибка в потоках, записать её
						itm.setState("Ошибка: "+SrvThr.getErrTextChild());
						//выйти из формирования
						closeGen(menuGenItg, 2, 1, "Ошибка в потоке");
						return;
					
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					WebCtrl.incProgress();
					ds.commitTrans();
					break;
				}

				case "GEN_DIST_VOLS3": {
					//распределить где есть ОДПУ
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();
					tobj = new TempObjDaoImpl().findAll(3); //найти вводы
					srv = new SrvThr(ds, tobj, itm, 10, 3); // запустить N-потоков, var=3
													// по вводам, ждать выполнения
					if (SrvThr.getErrChild() !=0){
						//произошла ошибка в потоках, записать её
						itm.setState("Ошибка: "+SrvThr.getErrTextChild());
						//выйти из формирования
						closeGen(menuGenItg, 2, 1, "Ошибка в потоке");
						return;
					
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					WebCtrl.incProgress();
					ds.commitTrans();
					break;
				}
				
				case "GEN_CHRG": {
					// начисление (по домам)
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();

					tobj = new TempObjDaoImpl().findAll(4); //найти дома
					srv = new SrvThr(ds, tobj, itm, 10, 1); // запустить N-потоков, var=1 -
													// по домам, ждать выполнения
					if (SrvThr.getErrChild() !=0){
						//произошла ошибка в потоках, записать её
						itm.setState("Ошибка: "+SrvThr.getErrTextChild());
						//выйти из формирования
						closeGen(menuGenItg, 2, 1, "Ошибка в потоке");
						return;
						
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					WebCtrl.incProgress();
					ds.commitTrans();
					break;
				}
				case "GEN_SAL": {
					//сальдо по лиц счетам
					if (smplGen(menuGenItg, itm, 19, 1.0)) { 
						return; // выйти при ошибке
					}
					WebCtrl.incProgress();
					break;
				}

				case "GEN_FLOW": {
					//движение
					if (smplGen(menuGenItg, itm, 20, 0.5)) {
						return; // выйти при ошибке
					}
					itm.setProc(1.0);
					break;
				}
				
				case "GEN_PENYA": {
					// пеня (по лицевым - домам)
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();
					tobj = new TempObjDaoImpl().findAll(4); //найти дома
					srv = new SrvThr(ds, tobj, itm, 10, 4); // запустить N-потоков, var=4 -
													// пеня по домам, ждать выполнения
					if (SrvThr.getErrChild() !=0){
						//произошла ошибка в потоках, записать её
						itm.setState("Ошибка: "+SrvThr.getErrTextChild());
						//выйти из формирования
						closeGen(menuGenItg, 2, 1, "Ошибка в потоке");
						return;
						
					}
					ds.beginTrans();
					itm.setProc(1.0);
					itm.setDt2(new Date());
					ds.commitTrans();
					WebCtrl.incProgress();
					break;
				}
				
				case "GEN_PENYA_DIST": {
					//распределение пени по исх сальдо
					if (smplGen(menuGenItg, itm, 21, 0.5)) {
						return; // выйти при ошибке
					}
					//проверить распр.пени
					if (check2(menuGenItg, 13)) { //1
						return;
					}
					itm.setProc(1.0);
					break;
				}

				case "GEN_SAL_HOUSES": {
					if (smplGen(menuGenItg, itm, 22, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}

				case "GEN_XITO14": {
					//начисление по услугам
					if (smplGen(menuGenItg, itm, 23, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_F3_1": {
					//оплата по операциям
					if (smplGen(menuGenItg, itm, 24, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_F3_1_2": {
					//оплата по операциям, для оборотной
					if (smplGen(menuGenItg, itm, 25, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				

				case "GEN_F2_4": {
					//По УК-организациям Ф.2.4.
					if (smplGen(menuGenItg, itm, 26, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_F1_1": {
					//По пунктам начисления
					if (smplGen(menuGenItg, itm, 27, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}
				
				case "GEN_ARCH_BILLS": {
					//Архив, счета
					if (smplGen(menuGenItg, itm, 28, 1.0)) {
						return; // выйти при ошибке
					}
					
					//проверить распр.пени, после того как она переписана в архив
					if (check2(menuGenItg, 37)) { //1
						return;
					}
					
					break;
				}				
				
				case "GEN_DEBTS": {
					//Задолжники
					if (smplGen(menuGenItg, itm, 29, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_EXP_LISTS": {
					//Списки
					if (smplGen(menuGenItg, itm, 30, 0.1)) {
						return; // выйти при ошибке
					}
					if (smplGen(menuGenItg, itm, 31, 0.5)) {
						return; // выйти при ошибке
					}
					if (smplGen(menuGenItg, itm, 32, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_STAT": {
					//Статистика
					if (smplGen(menuGenItg, itm, 33, 1.0)) {
						return; // выйти при ошибке
					}
					break;
				}				
				
				case "GEN_MONTH_OVER": {
					//Переход периода
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();

					if (smplGen(menuGenItg, itm, 34, 0.3)) { 
						return; // выйти при ошибке
					}
					//После перехода - начисление
					tobj = new TempObjDaoImpl().findAll(1); //найти дома
					srv = new SrvThr(ds, tobj, itm, 10, 1); // запустить N-потоков, var=1 -
													// по домам, ждать выполнения
					ds.beginTrans();
					if (SrvThr.getErrChild() !=0){
						//произошла ошибка в потоках, записать её
						itm.setState("Ошибка: "+SrvThr.getErrTextChild());
						//выйти из формирования
						closeGen(menuGenItg, 2, 1, "Ошибка в потоке");
						return;
						
					}
					itm.setProc(0.5);
					ds.commitTrans();

					//сальдо по лиц счетам
					if (smplGen(menuGenItg, itm, 19, 0.8)) {
						return; // выйти при ошибке
					}
					//движение
					if (smplGen(menuGenItg, itm, 19, 1.0)) {
						return; // выйти при ошибке
					}
					ds.beginTrans();
					itm.setDt2(new Date());
					ds.commitTrans();

					//**********установить дату формирования, после перехода
					ds.beginTrans();
					 if (ex.runWork(16, 0, 0)!=0) {
						closeGen(menuGenItg, 2, 1, "ThrMain: Некорректная дата формирования!");
						return; // выйти при ошибке
					 }
					ds.commitTrans();

					break;
				}		
				
				case "GEN_COMPRESS_ARCH": {
					ds.beginTrans();
					itm.setDt1(new Date());
					ds.commitTrans();

		            MntBase mntBase = WebApplication.applicationContext.getBean(MntBaseImpl.class);
					if (!mntBase.comprAllTables("00000000", null, "anabor", false)) {
						return; // выйти при ошибке
					}
					
					ds.beginTrans();
					itm.setProc(0.3D);
					ds.commitTrans();
					
					if (!mntBase.comprAllTables("00000000", null, "acharge", false)) {
						return; // выйти при ошибке
					}

					ds.beginTrans();
					itm.setProc(0.5D);
					ds.commitTrans();

					if (!mntBase.comprAllTables("00000000", null, "achargeprep", false)) {
						return; // выйти при ошибке
					}

					ds.beginTrans();
					itm.setProc(1D);
					itm.setDt2(new Date());
					ds.commitTrans();
					
					break;
				}				
			}

		}
		
		ds.beginTrans();
		menuGenItg.setProc(0.2); //установить 90% выполнения
		ds.commitTrans();

		//************ Заключительные проверки, при итоговом формировании
		if (menuCheckBG.getSel()) {
			// если выбраны проверки, а они как правило д.б. выбраны при итоговом
			//окончательная проверка на корректность исх.сальдо
			if (check(menuGenItg, 11)) {
				return;
			}
		
		}

		if (menuGenItg.getSel()) {
			// Завершение итогового
			ds.beginTrans();
			menuGenItg.setProc(1.0); //установить 100% выполнения
			menuGenItg.setDt2(new Date());
			closeGen(menuGenItg, 0, 0, "Итоговое формирование выполнено!");

			//установить признак что сформировано итоговое, если дошли до сюда
			ret = ex.runWork(18, 1, 0);
			if (ret==-1) {
				//Ошибка во время вызова
				closeGen(menuGenItg, 2, 1, "ThrMain: "+ex.doWorkErrText);
				return; // выйти при ошибке
			}
		} else {
			// Завершение частичного формирования
			ds.beginTrans();
			closeGen(menuGenItg, 0, 0, "Частичное формирование выполнено!");
		}
			
	}


	static boolean isStopped() {
		return stopped;
	}


	static void setStopped(boolean stopped) {
		ThrMain.stopped = stopped;
	}
}