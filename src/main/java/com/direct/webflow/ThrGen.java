package com.direct.webflow;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.HibernateException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.jdbc.Work;

import com.dic.bill.model.scott.TempObj;
import com.dic.bill.utils.DSess;


//класс формирования объектов (домов например)
public class ThrGen extends Thread {
	boolean stopped = false;
	String name;
    int var;//вариант формирования (1-начисление по дому, 2-...)
	DSess ds;
	ExecProc ex;
	
	/**
	 * Формировать по дому
	 * @param house Дом
	 * 		  var Вариант
	 */
	private void GenObj(final int objId, int var) {
		try {
			switch (var) {
			case 1:{
				//вариант 1 - формирование начисления по дому
				ds.sess.doWork(new Work() {
		      	 public void execute(Connection connection) throws SQLException {
  					    //System.out.println("ThrGen.doWork: call scott.c_charges.gen_charges!"+objId);
		      		    CallableStatement call = connection.prepareCall("{ ? = call scott.c_charges.gen_charges(?, ?, ?, ?, ?, ?) }");
		      		    call.registerOutParameter(1, Types.INTEGER);
		      		    call.setString(2, null);
		      		    call.setString(3, null);
		      		    call.setInt(4, objId); //id дома
		      		    call.setNull(5, Types.NUMERIC);
		      		    call.setInt(6, 1);// is commit?
		      		    call.setInt(7, 1);// sendmsg?
		      		    call.execute();
		      		  }
		      	 });
			break;
			}
			
			case 2:{
				//вариант 2 - распределить ОДН во вводах, где нет ОДПУ
				ds.sess.doWork(new Work() {
		      	 public void execute(Connection connection) throws SQLException {
					    //System.out.println("ThrGen.doWork: call scott.p_vvod.gen_dist_wo_vvod_usl!"+objId);
		      		    CallableStatement call = connection.prepareCall("{ call scott.p_vvod.gen_dist_wo_vvod_usl(?) }");
		      		    call.setInt(1, objId); //id ввода
		      		    call.execute();
		      		  }
		      	 });
			break;
			}
			
			case 3:{
				//вариант 3 - распределить ОДН во вводах, где есть ОДПУ
				ds.sess.doWork(new Work() {
		      	 public void execute(Connection connection) throws SQLException {
					    //System.out.println("ThrGen.doWork: call scott.p_thread.gen_dist_odpu!"+objId);
		      		    CallableStatement call = connection.prepareCall("{ call scott.p_thread.gen_dist_odpu(?) }");
		      		    call.setInt(1, objId); //id ввода
		      		    call.execute();
		      		  }
		      	 });
			break;
			}
			
			case 4:{
				// вариант 4 - начислить пеню по домам
				ds.sess.doWork(new Work() {
		      	 public void execute(Connection connection) throws SQLException {
					    //System.out.println("ThrGen.doWork: call scott.c_cpenya.gen_charge_pay_pen_house!"+objId);
		      		    CallableStatement call = connection.prepareCall("{ call scott.c_cpenya.gen_charge_pay_pen_house(?, ?) }");
		      		    call.setNull(1, Types.DATE);// дата, не заполняем, null
		      		    call.setInt(2, objId); // id дома
		      		    call.execute();
		      		  }
		      	 });
			break;
			}

			default: {
				System.out.println("ThrGen.doWork: не найдено вхождение case!");
			}
			
			
			}
		} catch (GenericJDBCException excp) {
			Throwable cause = excp.getCause();
			SrvThr.setErrChild(1); //признак ошибки в вызывающем классе (synchronize не нужен)
			SrvThr.setErrTextChild(cause.getMessage());
			System.out.println("Error while executing "+name+" thread, objId="+objId);
			System.out.println("ThrGen.doWork: "+cause.getMessage());
		} catch (HibernateException excp) {
			Throwable cause = excp.getCause();
			SrvThr.setErrChild(1); //признак ошибки в вызывающем классе (synchronize не нужен)
			SrvThr.setErrTextChild(cause.getMessage());
			System.out.println("Error while executing "+name+" thread, objId="+objId);
			System.out.println("ThrGen.doWork: "+cause.getMessage());
		} catch (Exception excp) {
			//прочие ошибки
			Throwable cause = excp.getCause();
			SrvThr.setErrChild(1); //признак ошибки в вызывающем классе (synchronize не нужен)
			SrvThr.setErrTextChild(cause.getMessage());
			System.out.println("Error while executing "+name+" thread, objId="+objId);
			System.out.println("ThrGen.doWork: "+cause.getMessage());
		}
	}
		
	
	/**
	 * 	 * Формирует информацию по объекту
	 * @param name Наименование потока
	 * @param var Вариант формирования
	 */
	ThrGen(String name, int var) {
		System.out.println("Creating " + name);
		ds=new DSess(true);
		ex = new ExecProc(ds);
		
		ds.beginTrans();
		// установить текущую дату формирования	
		if (ex.runWork(16, 0, 0)!=0) {
			return; // выйти при ошибке
		 }
		ds.commitTrans();
		
		stopped=false;
		this.name=name;
		this.var=var;
		System.out.println("End of Creating " + name);
	}

	public void run() {
				while ((SrvThr.getErrChild() ==0) && (!stopped) && (!ThrMain.isStopped())) {
					TempObj tobj=SrvThr.getNextObj();
					
					if (tobj == null) {
						//если вернулся пустой объект - начать останавливаться, - обрабатывать нечего
						synchronized (this) {
							stopped=true;
						}
					} else {
						//формирование
						System.out.println(this.name+" working with:"+tobj.getId());
							// var - варианты формирования
							ds.beginTrans();
							GenObj(tobj.getId(), var);
							ds.commitOpenTrans();
					}
				}
		ds.closeSess();
	}

}