package com.direct.webflow;

import java.util.List;
import java.util.Random;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;

import com.dic.bill.dao.AnaborDAO;
import com.dic.bill.dao.impl.PrepErrDaoImpl;
import com.dic.bill.dao.impl.SprGenItmDaoImpl;
import com.dic.bill.model.scott.PrepErr;
import com.dic.bill.model.scott.SprGenItm;
import com.dic.bill.utils.DSess;
import com.direct.srv.ComprTbl;
import com.direct.srv.MntBase;

import lombok.extern.slf4j.Slf4j;

@Slf4j
@EnableAutoConfiguration
@Controller
public class WebCtrl {

	ThrMain T1;
	static String stateGen;//Состояние формирования 0-Выполнено успешно, 1-Формируется, 2-выход, с ошибкой
	static int progressGen=0;//Прогресс формирования от 0 и до кол-во итераций
	
	@Autowired
	private MntBase mntBase;	
	
   static void incProgress(){
	   progressGen++;
   }
   

   // текущий ключ допуска
   private int curKey=0; 
   
   /*
    * Сжать Anabor
    * @param firstLsk - начать с лиц.сч.
    */
	@RequestMapping("/scan")
	@ResponseBody
	public String scan(
			@RequestParam(value = "firstLsk", defaultValue = "00000000", required = false) String firstLsk,
			@RequestParam(value = "oneLsk", required = false) String oneLsk,
			@RequestParam(value = "table", required = false) String table,
			@RequestParam(value = "allPeriods", defaultValue = "0", required = false) Integer allPeriods,
			@RequestParam(value = "key", defaultValue = "0", required = false) String key) {
		log.info("GOT /scan with: firstLsk={}, oneLsk={}, table={}, allPeriods={}, key={}", firstLsk, oneLsk, table, allPeriods, key);
		boolean isAllPeriods = false;
		
		if (curKey==0) {
			// нет ключа
			Random random = new Random();
			int max = 100;
			int min = 1;
			int randomNum = random.nextInt(max) % (max - min + 1) + min;
			curKey=randomNum;
			return "Please enter the key="+curKey;
		} else if (curKey==Integer.valueOf(key)) {
			//ключ совпал
			if (allPeriods == 1) {
				// анализировать все периоды
				isAllPeriods = true;
			}
			try {
				if (mntBase.comprAllTables(firstLsk, oneLsk, table, isAllPeriods)) {
					curKey=0;
					return "OK";
				} else {
					curKey=0;
					return "ERROR";
				}
			} catch (Exception e) {
				curKey=0;
				return "ERROR";
			}
		} else {
			return "Wrong key! Please enter the key="+curKey;
		}
	}
   
   @RequestMapping(value = "/getSprgenitm", method = RequestMethod.GET, produces="application/json")
   @ResponseBody
   public List<SprGenItm> getSprGenItm() {
	   return new SprGenItmDaoImpl().findAll();
    }

   @RequestMapping(value = "/getPrepErr", method = RequestMethod.GET, produces="application/json")
   @ResponseBody
   public List<PrepErr> getPrepErr() {
	   return new PrepErrDaoImpl().findAll();
    }


   /*
    * Вернуть статус текущего формирования
    */
   @RequestMapping(value = "/getStateGen", method = RequestMethod.GET)
   @ResponseBody
   public String getStateGen() {
 	   
	   return stateGen;
    }

   /*
    * Вернуть прогресс текущего формирования, для обновления грида у клиента
    */
   @RequestMapping(value = "/getProgress", method = RequestMethod.GET)
   @ResponseBody
   public int getProgress() {
	   return progressGen;
    }
   
   /*
    * Проверить состояние пунктов меню
    */
   @RequestMapping(value = "/checkItms", method = RequestMethod.POST)
   @ResponseBody
   public String checkItms(@RequestParam(value="id") int id, @RequestParam(value="sel") int sel) {
 	   DSess ds = new DSess(false);
	   ExecProc ex =new ExecProc(ds);
	   ds.beginTrans();
 	   ex.runWork(35, id, sel);
	   ds.commitTrans();
	   log.info("/checkItms?id="+id);
	   
	   return null;
    }   
   
   /*
    * Вернуть ошибку, последнего формирования, если есть
    */
   @RequestMapping(value = "/getErrGen", method = RequestMethod.GET)
   @ResponseBody
   public String getErrGen() {
 	   
	    SprGenItmDaoImpl sprgDao = new SprGenItmDaoImpl();
	    SprGenItm menuGenItg;
		menuGenItg = sprgDao.getByCd("GEN_ITG");
		
	   return String.valueOf(menuGenItg.getErr());
    }
   
   @RequestMapping(value = "/editSprgenitm", method = RequestMethod.POST, produces="application/json", consumes="application/json")
   @ResponseBody
   public String editSprGenItm(@RequestBody List<SprGenItm> iList) { //использовать List объектов, со стороны ExtJs в Модели сделано allowSingle: false

 	   DSess ds = new DSess(false);
	   ds.beginTrans();
	   for (SprGenItm itm : iList) {
		   SprGenItm i= (SprGenItm)ds.sess.load(SprGenItm.class, itm.getId());
		   if (itm.getSel() !=null) { 
			    i.setSel(itm.getSel());
			   };
		   if (itm.getName() !=null) { 
			   i.setName(itm.getName());
		   }
       }
	   
	   ds.commitTrans();
	   log.info("/editSprgenitm");
	   return null;
    }

   
	@RequestMapping("/startGen")
    @ResponseBody
    String startGen() {
		
		if (T1 == null || ThrMain.isStopped()) {
		   // Запустить в потоке, чтобы не тормозило request
   	  	   T1= new ThrMain();
           ThrMain.setStopped(false); // НЕ ПОНЯЛ, зачем здесь устанавливать false, если ЭТО новый объект и в нём stopped=false при инициализации... 
   	  	   T1.start();
           log.info("Started thread!");
    	} else {
           log.info("Already started!");
    	   return "Already started!";
    	}
		return "ok";
    }

	@RequestMapping("/stopGen")
    @ResponseBody
    String stopGen() {
    	if (T1 != null) {
	        ThrMain.setStopped(true);
            log.info("Trying to stop!");

	        return "Ended!";
    	} else {
            log.info("Already ended!");
    		return "Already ended!";
    	}
        
    }

/*	public static void main(String[] args) throws Exception {
	   
        SpringApplication.run(WebCtrl.class, args);
    }
*/
	}
