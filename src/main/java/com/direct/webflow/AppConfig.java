package com.direct.webflow;

import org.springframework.beans.BeansException;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.ImportResource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;
import org.springframework.scheduling.annotation.EnableAsync;

@Configuration
@ComponentScan({"com.direct.webflow","com.direct.srv", "com.dic.bill.dao"})
@EnableCaching
@EnableAsync
@EnableJpaRepositories(basePackages="com.dic.bill.dao")
@ImportResource("spring.xml")
public class AppConfig {
	
	static ApplicationContext ctx = null;
	
	public void setApplicationContext(ApplicationContext context) throws BeansException {
		ctx = context;
	}
	
	public static ApplicationContext getContext(){
	      return ctx;
	}

	
}
