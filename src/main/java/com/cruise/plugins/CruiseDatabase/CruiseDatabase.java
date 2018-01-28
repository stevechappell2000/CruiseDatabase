package com.cruise.plugins.CruiseDatabase;

import java.sql.ResultSet;

import com.corecruise.core.CoreCruise;
import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.config.CruisePluginEnvironment;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Services;
import com.corecruise.cruise.services.utils.GenericSessionResp;
import com.cruise.plugins.Action;
import com.cruise.plugins.ActionParameter;
import com.cruise.plugins.PlugInMetaData;
import com.cruise.plugins.CruiseDatabase.metadata.DBMetaData;
import com.cruise.plugins.CruiseDatabase.metadata.TableMetaData;
import com.cruise.plugins.CruiseDatabase.utils.JsonNodeRowMapper;
import com.cruise.plugins.CruiseDatabase.utils.QueryBuilder;
import com.cruise.plugins.CruiseDatabase.utils.cruConnection;
import com.cruise.plugins.CruiseDatabase.utils.cruConnectionObject;
import com.fasterxml.jackson.databind.node.ArrayNode;


/**
 * Hello world!
 *
 */
public class CruiseDatabase implements PluginInterface
{


	PlugInMetaData pmd = null;
	CruisePluginEnvironment config = null;
	String pluginName = "CruiseDatabase";
	
	public CruiseDatabase() {
		if(null == config)
			config = CoreCruise.getCruiseConfig(pluginName);
		
    	pmd = new PlugInMetaData(pluginName,"0.0.1","SJC","Database access plugin");
  	
	    int x = 0;
    	pmd.getActions().add(new Action("PlugInInfo", "get information about the pluging"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));
    	
		++x;
    	pmd.getActions().add(new Action("echo", "Echos the request back as response."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","~UUID","Internal Parameter to track service names. You can override"));

    	++x;
    	pmd.getActions().add(new Action("cDBCreatePool", "Create a new PooledConnection"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","CreatePoolService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_DriverClassName","true","org.mariadb.jdbc.Driver","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_jdbcUrl","true","jdbc:mysql://localhost:3306/cruisecore?useSSL=false","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_username","true","root","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_password","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_maximumPoolSize","false","25","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("config_minimumIdle","false","5","Name of the Connection pool. This name is used in subsequent calls."));
 
		++x;
    	pmd.getActions().add(new Action("cDBGetPoolInfo", "Returns the pool information"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","GetPoolInfoService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));

		++x;
    	pmd.getActions().add(new Action("cDBGetTableInfo", "Returns the Table information.\nSerializes a com.cruise.plugins.CruiseDatabase.metadata.TableMetaData object into the SessionObject ResponseObject (see SessionObject.getRepsone())"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","GetTableInfoService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));

		++x;
    	pmd.getActions().add(new Action("select", "Queries a table and returns a JSON array of records.\nSerializes a com.fasterxml.jackson.databind.node.ArrayNode  object into the SessionObject ResponseObject (see SessionObject.getRepsone())"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","SelectService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("selectList","false","","list of fields"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("includeQuery","false","","When true, the query executed is returned."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("orderBy","false","","Sort order CSV List of column names"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("distinct","false","","adds distinct key word to query"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("groupBy","false","","adds a group by"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("holdResults","false","false","When set to true, the resultset is stored in the SessionObject and results are not sent to the client."));

		/*++x;
    	pmd.getActions().add(new Action("forEach", "Updates a record based on supplied primary key."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","UpdateService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","","Table name or list of tables names that make up the 'From' clause"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("includeQuery","false","","When true, the query executed is returned."));
        */
		
		++x;
    	pmd.getActions().add(new Action("update", "Updates a record based on supplied primary key."));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","UpdateService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","","Table name or list of tables names that make up the 'From' clause"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("includeQuery","false","","When true, the query executed is returned."));
    	
		++x;
    	pmd.getActions().add(new Action("insert", "Inserts Record(s) into the specified table"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","UpdateService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","","Table name or list of tables names that make up the 'From' clause"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("includeQuery","false","","When true, the query executed is returned."));

		++x;
    	pmd.getActions().add(new Action("delete", "Deletes Record(s) from the specified table"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("service","true","UpdateService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(x).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("tableName","true","","Table name or list of tables names that make up the 'From' clause"));
    	pmd.getActions().get(x).getActionParams().add(new ActionParameter("includeQuery","false","","When true, the query executed is returned."));

		/*
			String qOrder = service.Parameter("orderBy");
			String qGroupBy = service.Parameter("groupBy");
			boolean bDistinct = new Boolean(service.Parameter("distinct"));
			String qFields = service.Parameter("columnList");
			String sFields = service.Parameter("selectList");
			String qFrom = service.Parameter("fromList");
			String qWhere = service.Parameter("Where"); 
		 */
    
    }

	public boolean executePlugin(SessionObject so, Services service) {
		boolean ret = false;
		String action = service.Action().trim().toLowerCase();
		GenericSessionResp gro = new GenericSessionResp();
		DBMetaData dbmd = null;
		cruConnectionObject cruConn = null;
		switch (action) {
		case "plugininfo":
			if(null != pmd) {
				so.appendToResponse("PlugInInfo", pmd);
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "100001", "Failed to get any information about the plugin.");
			}
			break;
		case "echo":
			if(null != pmd) {
				so.appendToResponse(so.getApplication());
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "100002", "Failed to get any information about the plugin.");
			}
			break;
		case "cdbcreatepool":
			if(!cruConnection.isPoolLoaded(so, service)) {
				ret = cruConnection.createConnectionPool(so,service);
				gro.addParmeter("PoolCreated", new Boolean(ret).toString());
				so.appendToResponse(service.Service()+"."+service.Action(),gro);
				ret = true;
			}else {
				gro.addParmeter("Pool Already exists", "Not added.");
				so.appendToResponse(service.Service()+":"+service.Action(),gro);
				ret = true;
			}
			break;
		case "cdbgetpoolinfo":
			dbmd = cruConnection.getPoolMetaData(so, service);
			if(null != dbmd) {
				so.appendToResponse(service.Service()+":"+service.Action(), dbmd);
				ret = true;
			}else {
				Clog.Error(so, "GetPoolInfo", "100003", "Failed to get any information about the pool");
			}
			break;
		case "cdbgettableinfo":
			dbmd = cruConnection.getPoolMetaData(so, service);
			if(dbmd.getTables().containsKey(service.Parameter("tableName"))) {
				TableMetaData tmd = dbmd.getTables().get(service.Parameter("tableName"));
				so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("tableName"), tmd);
				ret = true;
			}else {
				Clog.Error(so, "GetTableInfo", "100004", "Failed to get any information about the pool");
			}
			break;
		case "cdbgetconnection":
			
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					so.setRequestState(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"), cruConn);
					ret = true;
				}else {
					Clog.Error(so, "ser", "100005", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100006", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		case "select":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createSelect(so, service, false);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					if(null != query && query.length()>0) {
						so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"), gro);
						ResultSet rs = cruConn.getConn().prepareStatement(query).executeQuery();
						if(null != service.Parameter("holdResults") && service.Parameter("holdResults").equalsIgnoreCase("true")){
						  so.setRequestState(service.Service()+"."+service.Action()+".resultSet", rs);
						  gro.addObjectParmeter("Results", "Stored in SessionObject.RequestState as "+service.Service()+"."+service.Action()+".resultSet");
						}else {
							if(null != rs) {
								ArrayNode resultMap= JsonNodeRowMapper.mapRows(so, rs);
								
								if(null != resultMap) {
									gro.addObjectParmeter("Results", resultMap);
								}else {
									gro.addParmeter("Results", "");
								}
							}else {
								gro.addParmeter("Results", "");
							}
						}
						ret = true;
					}
				}else {
					Clog.Error(so, "ser", "100007", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100008", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		/*case "foreach":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createSelect(so, service, false);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"), gro);
					ResultSet rs = cruConn.getConn().prepareStatement(query).executeQuery();
					
					if(null != rs) {
						ResultSetMetaData rsm = rs.getMetaData();
						int colCount = rsm.getColumnCount();
						int i = 0;
						while(rs.next()) {
							Properties p = new Properties();
							for(i=0;i<colCount;i++) {
								p.setProperty(rsm.getColumnName(i), rs.getString(i));
							}
						}
					}else {
						gro.addParmeter("Results", "");
					}
					ret = true;
				}else {
					Clog.Error(so, "ser", "100.03", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "100.04", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;*/
		case "update":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createUpdate(so, service);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					if(null != query && query.length()>0) {
						so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"),gro);
						Integer updated = cruConn.getConn().prepareStatement(query).executeUpdate();
						gro.addParmeter("UpdateCount", updated.toString());
						ret = true;
					}
				}else {
					Clog.Error(so, "ser", "100009", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "1000010", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		case "insert":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createInsert(so, service);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					if(null != query && query.length()>0) {
						so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"),gro);
						Integer updated = cruConn.getConn().prepareStatement(query).executeUpdate();
						gro.addParmeter("InsertCount", updated.toString());
						ret = true;
					}
				}else {
					Clog.Error(so, "ser", "1000011", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "1000012", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;
		case "delete":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createDelete(so, service);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					if(null != query && query.length()>0) {
						so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"),gro);
						Integer updated = cruConn.getConn().prepareStatement(query).executeUpdate();
						gro.addParmeter("InsertCount", updated.toString());
						ret = true;
					}
				}else {
					Clog.Error(so, "ser", "1000013", "executePlugin Failed to added connection to RequestState");
				}
			} catch (Exception e1) {
				Clog.Error(so, "ser", "1000014", "executePlugin Failed:"+e1.getMessage());
				e1.printStackTrace();
			}finally {
				if(null != cruConn) {
					cruConn.Close();
				}
			}
			break;		
		default:
			Clog.Error(so, "service", "1000015", "Invalid Action supplied:"+action);
		}
		return ret;
	}

	@Override
	public PlugInMetaData getPlugInMetaData() {
		// TODO Auto-generated method stub
		return pmd;
	}

	@Override
	public void setPluginVendor(PlugInMetaData PMD) {
		pmd = PMD;
		
	}

	@Override
	public void byPass(SessionObject sessionObject) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public boolean initPlugin() {
		// TODO Auto-generated method stub
		return false;
	}






}
