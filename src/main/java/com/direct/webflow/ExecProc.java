package com.direct.webflow;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Types;

import org.hibernate.HibernateException;
import org.hibernate.exception.GenericJDBCException;
import org.hibernate.jdbc.Work;

import com.dic.bill.utils.DSess;

import lombok.extern.slf4j.Slf4j;

@Slf4j
public class ExecProc {

	public int doWorkRet; 		  //Результат из doWork
	public String doWorkErrText; //Текста ошибки из doWork
	public SQLException doWorkExcpt; //Exeption из doWork
	
	DSess ds; //объект сессии
	
	//класс, выполняющий вызовы процедур Oracle
	ExecProc(DSess ds) {
		this.ds=ds;
	}
	
	/**
	 * 
	 * @param var  вариант исполнения
	 * 		extPar дополнительный параметр
	 *            
	 * @return
	 */
	public int runWork(int var, int extPar, int extPar2) { //используется массив var[] так как doWork требует final переменную... так обманули
	doWorkRet =-1;
	final int varArr[]={var};
	final int extParArr[]={extPar};
	final int extParArr2[]={extPar2};
	try {	
		switch (varArr[0]) {
		case 1: {
			// Установить статус итогового формирования
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ ? = call scott.utils.set_base_state_gen(?) }");
					call.registerOutParameter(1, Types.INTEGER);
					call.setInt(2, 1);
					call.execute();
					doWorkRet = call.getInt(1);
				}
			});
			break;
		}
		case 2: {
			// Снять статус итогового формирования
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ ? = call scott.utils.set_base_state_gen(?) }");
					call.registerOutParameter(1, Types.INTEGER);
					call.setInt(2, 0);
					call.execute();
					doWorkRet = call.getInt(1);
				}
			});
			break;
		}
		case 3: {
			// Закрыть базу для пользователей
			//сперва проверить - надо ли закрывать
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ ? = call scott.utils.get_int_param(?) }");
					call.registerOutParameter(1, Types.INTEGER);
					call.setString(2, "NEED_CLOSE_BASE");
					call.execute();
					doWorkRet = call.getInt(1); 
				}
			});
				if (doWorkRet==1) {
					//да, надо закрывать
					ds.sess.doWork(new Work() {
						public void execute(Connection connection) throws SQLException {
						//try
						//{
						CallableStatement call = connection
								.prepareCall("{ call scott.admin.set_state_base(?) }");
						call.setInt(1, 1);
						call.execute();
					}
					});
				} else
				{
					//не надо закрывать
					doWorkRet=0;
				}
			break;
		}
		case 4: case 5: case 6: case 7: case 38:{
			//проверка smpl_chck
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ ? = call scott.p_thread.smpl_chk(?) }");
					call.registerOutParameter(1, Types.INTEGER);
					//перекодировать в gen.smpl_chck код выполнения
					switch (varArr[0]) {
					case 4:
						  call.setInt(2, 1);
						  break;
					case 5:
						  call.setInt(2, 2);
						  break;
					case 6:
						  call.setInt(2, 3);
						  break;
					case 7:
						  call.setInt(2, 4);
						  break;
					case 38:
						  call.setInt(2, 5);
						  break;
					}
					  
					call.execute();
					doWorkRet = call.getInt(1); 
				}
			});
			break;
		}
			//основные проверки
		case 8: case 9: case 10: case 11: case 12: case 13: case 14: case 15: case 37:{
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_check(?, ?, ?) }");
					call.registerOutParameter(1, Types.INTEGER);
					call.registerOutParameter(2, Types.VARCHAR);
					
					//перекодировать в gen.gen_check код выполнения
					switch (varArr[0]) {
					case 8:
						  call.setInt(3, 1);
						  break;
					case 9:
						  call.setInt(3, 2);
						  break;
					case 10:
						  call.setInt(3, 3);
						  break;
					case 11:
						  call.setInt(3, 4);
						  break;
					case 12:
						  call.setInt(3, 5);
						  break;
					case 13:
						  call.setInt(3, 6);
						  break;
					case 14:
						  call.setInt(3, 7);
						  break;
					case 15:
						  call.setInt(3, 8);
						  break;
					case 37:
						  call.setInt(3, 9);
						  break;
					}
					
					call.execute();
					doWorkRet = call.getInt(1);
					doWorkErrText = call.getString(2);
				}
			});
			break;
			
		}
		
		case 16: {
			//установить текущую дату, до формирования
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.init.set_date_for_gen() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 17: {
			//чистить инф, там где ВООБЩЕ нет счетчиков (нет записи в c_vvod)
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.p_thread.gen_clear_vol() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 18: {
			//сбросить или установить признак что итоговое сформировано 
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.init.set_state(?) }");
					call.setInt(1, extParArr[0]);
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 19: {
			//формирование сальдо 
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_saldo(?) }");
					call.setString(1, "");
					call.execute();
					
					// удалить корректировки по оплате -  не надо всё это. делается в методе scott.gen.gen_saldo() 
					// ред. 09.10.2017
					/*call = connection
							.prepareCall("{ call scott.c_gen_pay.dist_pay_del_corr() }");
					call.execute();
					
					// добавить корректировки по оплате
					call = connection
							.prepareCall("{ call scott.c_gen_pay.dist_pay_add_corr(?) }");
					call.setInt(1, 1);
					call.execute();*/
					
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 20: {
			//движение
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_cpenya.gen_charge_pay_full() }");
					call.execute();
					doWorkRet = 0;
				}
			});
			break;
		}
		case 21: {
		    //распределение начисленной пени по исх.сальдо
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_cpenya.gen_charge_pay_pen(?, ?) }");
	      		    call.setNull(1, Types.DATE);
	      		    call.setInt(2, 0);
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 22: {
		    //сальдо по домам
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_saldo_houses() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 23: {
		    //начисление по услугам (надо ли оно кому???)
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_xito13() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 24: {
		    //оплата по операциям Ф.3.1.
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_opl_xito5() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 25: {
		    //оплата по операциям Ф.3.1. для оборотки
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_opl_xito5_() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 26: {
		    //По УК-организациям Ф.2.4.
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_opl_xito10() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		case 27: {
		    //По пунктам начисления
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_opl_xito3() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 28: {
		    //Архив, счета
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.prepare_arch(?) }");
					call.setString(1, "");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 29: {
		    //Задолжники
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.gen_debits_lsk_month(?) }");
					call.setString(1, "");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 30: {
		    //Списки - privs
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_exp_list.privs_export() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 31: {
		    //Списки - changes
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_exp_list.changes_export() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 32: {
		    //Списки - charges
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_exp_list.charges_export() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 33: {
		    //Статистика
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen_stat.gen_stat_usl(?) }");
					call.setString(1, "");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 34: {
		    //Переход периода
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.gen.go_next_month_year() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
	
		case 35: {
		    //Вызов из WebCtrl
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.p_thread.check_itms(?, ?) }");
					call.setInt(1, extParArr[0]);
					call.setInt(2, extParArr2[0]);
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}

		case 36: {
		    //Перераспределение авансовых платежей
			ds.sess.doWork(new Work() {
				public void execute(Connection connection) throws SQLException {
					CallableStatement call = connection
							.prepareCall("{ call scott.c_dist_pay.dist_pay_lsk_avnc_force() }");
					call.execute();
					doWorkRet = 0;
				}
			});
	
			break;
		}
		
		default: {
			doWorkErrText="ThrMain.doWork: не найдено вхождение case!";
			log.info("ThrMain.doWork: не найдено вхождение case!");
			doWorkRet=-1;
		}
		}
	
	} catch (GenericJDBCException excp) {
		Throwable cause = excp.getCause();
		doWorkRet=-1;//признак ошибки
		doWorkErrText=cause.getMessage();
		log.info("ThrMain.doWork: "+cause.getMessage());
		log.info("Error while executing doWork with var= "+var);
	} catch (HibernateException excp) {
		Throwable cause = excp.getCause();
		doWorkRet=-1;//признак ошибки
		if (cause!=null){
			doWorkErrText=cause.getMessage();
	        log.info("ThrMain.doWork: "+cause.getMessage());
			log.info("Error while executing doWork with var= "+var);
		} else {
			doWorkErrText=excp.getMessage();
	        log.info("Next ThrMain.doWork: "+excp.getMessage());
			log.info("Next Error while executing doWork with var= "+var);
			
		}
	} catch (Exception excp) {
		//прочие ошибки
		Throwable cause = excp.getCause();
		doWorkRet=-1;//признак ошибки
		doWorkErrText=cause.getMessage();
		log.info("ThrMain.doWork: "+cause.getMessage());
		log.info("Error while executing doWork with var= "+var);
	}
	return doWorkRet;
	
	}

}


