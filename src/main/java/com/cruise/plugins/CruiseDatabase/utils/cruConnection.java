package com.cruise.plugins.CruiseDatabase.utils;

import java.sql.SQLException;
import java.util.HashMap;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.utils.Parameters;
import com.corecruise.cruise.services.utils.Services;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class cruConnection {
	public static HashMap<String,HikariDataSource> cruConnections = null;
    public cruConnection() {
    	
    }
    public static boolean createConnectionPool(SessionObject so, Services service) {
    	boolean ret = false;

	    	String poolName = service.ParameterValue("PoolName");
	    	if(null == poolName || poolName.trim().length()<1) {
	    		Clog.Error(so, "service", "80001", "PoolName is required and was not supplied.");
	
	    	}else {
	    		if(null == cruConnections) {
	    			cruConnections = new HashMap<String,HikariDataSource>();
	    		}
	    		poolName = poolName.trim().toUpperCase();
	    		try {
	    			HikariConfig jdbcConfig = new HikariConfig();
	    			jdbcConfig.setPoolName(poolName);
	    			jdbcConfig.setDriverClassName(service.ParameterValue("DriverClassName"));
	    			jdbcConfig.setMaximumPoolSize(service.ParameterInt("maximumPoolSize"));
	    			jdbcConfig.setMinimumIdle(service.ParameterInt("minimumIdle"));
	    			jdbcConfig.setJdbcUrl(service.ParameterValue("jdbcUrl"));
	    			jdbcConfig.setUsername(service.ParameterValue("username"));
	    			jdbcConfig.setPassword(service.ParameterValue("password"));
	    			HikariDataSource ds = new HikariDataSource(jdbcConfig);
	    			if(cruConnections.containsKey(poolName)) {
	    				Clog.Warn(so, "service", "80002", "Duplicat pool "+poolName+". Ignoring.");
	    			}else {
	    				cruConnections.put(poolName, ds);
	    				ret = true;
	    			}
	    		}catch(Exception e) {
	                Clog.Error(so, "service", "80003", "Error creating connection:"+e.getMessage());
	    		}
	    	}

    	return ret;
    }
    public static cruConnectionObject getConnection(SessionObject so, Services service) throws Exception {
    	cruConnectionObject ret = new cruConnectionObject();
    	String poolName = service.getParameter("PoolName").getValue();
    	if(null == poolName || poolName.trim().length()<1) {
    		Clog.Error(so, "service", "80010", "(getConnection) PoolName is required and was not supplied.");
    	}else {
    		try {
				ret.getConnection(so, poolName.trim().toUpperCase(), cruConnections);
			} catch (SQLException e) {
				Clog.Error(so, "service", "80020", "(getConnection) Connection failed."+e.getMessage());
				throw e;
			} catch (Exception e) {
				Clog.Error(so, "service", "80030", "(getConnection) Connection failed.\"+e.getMessage()");
				throw e;
			}
    	}
    	return ret;
    }
}
