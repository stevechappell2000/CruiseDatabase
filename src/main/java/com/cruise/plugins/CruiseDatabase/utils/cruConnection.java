package com.cruise.plugins.CruiseDatabase.utils;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.CruiseDatabase.metadata.DBMetaData;
import com.cruise.plugins.CruiseDatabase.metadata.MDLoader;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;

public class cruConnection {
	private static HashMap<String,HikariDataSource> cruConnections = null;
	private static HashMap<String, DBMetaData> cache = new HashMap<String, DBMetaData>();
	public cruConnection() {

	}
	/**
	 * Removes a connection from the pool
	 * @param so
	 * @param service
	 * @return
	 */
	public static boolean removeConnectionPool(SessionObject so, Services service) {
		boolean ret = false;
		String poolName = service.ParameterValue("poolName");
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80001", "PoolName is required and was not supplied.");

		}else {
			poolName = poolName.trim().toUpperCase();

			if(cruConnections.containsKey(poolName)) {
				cruConnections.remove(poolName);
				ret = true;
			}

		}

		return ret;
	}
	public static boolean isPoolLoaded(SessionObject so, Services service) {
		boolean ret = false;
		String poolName = service.ParameterValue("poolName");
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80001", "PoolName is required and was not supplied.");

		}else {
			poolName = poolName.trim().toUpperCase();

			if(null != cruConnections && cruConnections.containsKey(poolName)) {
				ret = true;
			}

		}

		return ret;
	}
	public static DBMetaData getPoolMetaData(SessionObject so, Services service) {
		DBMetaData ret = null;
		String poolName = service.ParameterValue("poolName");
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80001", "poolName is required and was not supplied.");

		}else {
			if(null == cache) {

			}else {
				poolName = poolName.trim().toUpperCase();

				if(cache.containsKey(poolName)) {
					ret = cache.get(poolName);
				}else {
					Clog.Warn(so, "app", "80002", "No poolname found matching:"+poolName);
				}

			}
		}

		return ret;
	}
	/**
	 * Adds a connection to the pool
	 * @param so
	 * @param service
	 * @return
	 */
	public static boolean createConnectionPool(SessionObject so, Services service) {
		boolean ret = false;

		/*jdbcConfig.setPoolName(poolName);
		jdbcConfig.setMaximumPoolSize(service.ParameterInt("maximumPoolSize"));
		jdbcConfig.setMinimumIdle(service.ParameterInt("minimumIdle"));
		jdbcConfig.setJdbcUrl(service.ParameterValue("jdbcUrl").trim());
		jdbcConfig.setDriverClassName(service.ParameterValue("DriverClassName").trim());
		jdbcConfig.setUsername(service.ParameterValue("username"));
		jdbcConfig.setPassword(service.ParameterValue("password"));*/

		String poolName = service.ParameterValue("poolName");
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80001", "PoolName is required and was not supplied.");

		}else {
			if(null == cruConnections) {
				cruConnections = new HashMap<String,HikariDataSource>();
			}

			Properties p = new Properties();
			for (Entry<String, String> entry : service.getParameters().entrySet()) {
				//Shouldn't hurt anything, but removing service and action just in case.
				if(entry.getKey().equalsIgnoreCase("preprocessservice")||entry.getKey().equalsIgnoreCase("postprocessservice")||entry.getKey().equalsIgnoreCase("processservice")||entry.getKey().equalsIgnoreCase("execute")||entry.getKey().equalsIgnoreCase("service")||entry.getKey().equalsIgnoreCase("ID")||entry.getKey().equalsIgnoreCase("action")||entry.getKey().equalsIgnoreCase("pluginName")) {

				}else {
					p.setProperty(entry.getKey(), entry.getValue());
				}
			}
			poolName = poolName.trim().toUpperCase();
			try {
				if(!cruConnections.containsKey(poolName)) {
					HikariConfig jdbcConfig = new HikariConfig(p);
					HikariDataSource ds = new HikariDataSource(jdbcConfig);
					if(cruConnections.containsKey(poolName)) {
						Clog.Warn(so, "service", "80002", "Duplicate pool "+poolName+". Ignoring.");
					}else {
						try {
							if(loadMetaData(so, service, p, ds)) {
								cruConnections.put(poolName, ds);
								ret = true;
							}
						}catch(Exception e) {
							e.printStackTrace();
							Clog.Error(so, "service", "80004", "Failed to load metadata:"+e.getMessage());
						}

					}
				}
			}catch(Exception e) {
				Clog.Error(so, "service", "80003", "Error creating connection:"+e.getMessage());
			}
		}

		return ret;
	}
	public static cruConnectionObject getNewConnection(SessionObject so, Services service) throws Exception {
		cruConnectionObject ret = new cruConnectionObject();
		String poolName = service.Parameter("poolName");
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80010", "(getConnection) PoolName is required and was not supplied.");
		}else {
			try {
				poolName = poolName.trim().toUpperCase();
				ret.setConn(getCRUConnection(so, poolName));
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
	private static Connection getCRUConnection(SessionObject so, String poolName) throws SQLException, Exception {
		Connection conn = null;
		if(null == poolName || poolName.trim().length()<1) {
			Clog.Error(so, "service", "80010", "(getConnection) PoolName is required and was not supplied.");
		}else {
			poolName = poolName.trim().toUpperCase();
			try {
				if(cruConnections.containsKey(poolName)) {
					HikariDataSource ds = cruConnections.get(poolName);
					try {
						conn =  ds.getConnection();
					} catch (SQLException ex) {
						Clog.Error(so, "ser", "80020", "(getConnection) failed:"+ex.getMessage());
						throw ex;
					} 
				}
			}catch(Exception e) {
				Clog.Error(so, "ser", "80020", "(getConnection) failed:"+e.getMessage());
				throw e;
			}
		}
		return conn;
	}
	private static boolean loadMetaData(SessionObject so, Services service, Properties p, HikariDataSource ds) throws Exception {
		boolean ret = false;
		Connection co = null;
		try {
			co = ds.getConnection();
			String poolName = service.ParameterValue("poolName");
			if(null != poolName) {
				poolName = poolName.trim().toUpperCase();
				DatabaseMetaData md = co.getMetaData();
				DBMetaData dbmd = new DBMetaData(p);
				dbmd.setVendor(md.getDatabaseProductName());
				if(MDLoader.loadMetaData(md,so,service,dbmd)) {
					cache.put(poolName, dbmd);
					ret = true;
				}
			}

		}catch(Exception e) {
			throw e;
		}finally {
			if(null != co) {
				co.close();
			}
		}

		return ret;
	}

}
