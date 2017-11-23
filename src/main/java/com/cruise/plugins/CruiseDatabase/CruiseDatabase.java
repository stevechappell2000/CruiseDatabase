package com.cruise.plugins.CruiseDatabase;

import java.sql.ResultSet;

import com.corecruise.cruise.SessionObject;
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
    public CruiseDatabase() {
    	pmd = new PlugInMetaData("CruiseDatabase","0.0.1","SJC","Database access plugin");
    	
    	pmd.getActions().add(new Action("info", "getPlugin Information"));
    	pmd.getActions().get(0).getActionParams().add(new ActionParameter("service","true","*UUID","Internal Parameter to track service names. You can override"));
    	
    	pmd.getActions().add(new Action("CruiseTest", "Test API Call"));
    	pmd.getActions().get(1).getActionParams().add(new ActionParameter("service","true","*UUID","Internal Parameter to track service names. You can override"));
		
    	pmd.getActions().add(new Action("PlugInInfo", "get information about the pluging"));
    	pmd.getActions().get(2).getActionParams().add(new ActionParameter("service","true","*UUID","Internal Parameter to track service names. You can override"));

    	pmd.getActions().add(new Action("cDBCreatePool", "Create a new PooledConnection"));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("service","true","CreatePoolService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("DriverClassName","true","org.mariadb.jdbc.Driver","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("jdbcUrl","true","jdbc:mysql://localhost:3306/cruisecore?useSSL=false","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("username","true","root","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("password","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("maximumPoolSize","false","25","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(3).getActionParams().add(new ActionParameter("minimumIdle","false","5","Name of the Connection pool. This name is used in subsequent calls."));
 
    	pmd.getActions().add(new Action("cDBGetPoolInfo", "Returns the pool information"));
		pmd.getActions().get(4).getActionParams().add(new ActionParameter("service","true","GetPoolInfoService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(4).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));

    	pmd.getActions().add(new Action("cDBGetTableInfo", "Returns the Table information"));
		pmd.getActions().get(5).getActionParams().add(new ActionParameter("service","true","GetTableInfoService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(5).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(5).getActionParams().add(new ActionParameter("tableName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));

    	pmd.getActions().add(new Action("select", "Queries a table"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("service","true","SelectService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("selectList","false","","list of fields"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("orderBy","false","","Sort order CSV List of column names"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("distinct","false","","adds distinct key word to query"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("groupBy","false","","adds a group by"));
		pmd.getActions().get(6).getActionParams().add(new ActionParameter("tableName","true","unknown","Name of the Connection pool. This name is used in subsequent calls."));

    	pmd.getActions().add(new Action("update", "Queries a table"));
		pmd.getActions().get(7).getActionParams().add(new ActionParameter("service","true","UpdateService","This is a unique name for this call to make selecting and parsing results easier"));
		pmd.getActions().get(7).getActionParams().add(new ActionParameter("poolName","true","MyPool","Name of the Connection pool. This name is used in subsequent calls."));
    	pmd.getActions().get(7).getActionParams().add(new ActionParameter("tableName","false","","Table name or list of tables names that make up the 'From' clause"));

    	pmd.getActions().add(new Action("echo", "Echos the request back as response."));
    	pmd.getActions().get(8).getActionParams().add(new ActionParameter("service","true","*UUID","Internal Parameter to track service names. You can override"));
    	
		//pmd.getActions().get(6).getActionParams().add(new ActionParameter("selectList","true","Duel","list of fields"));

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
		case "cruisetest":
			gro.addParmeter("PluginEnabled", "true");
			so.appendToResponse(service.Service()+"."+service.Action(),gro);
			break;
		case "info":
			if(null != pmd) {
				so.appendToResponse("PlugInInfo", pmd);
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "100.01", "Failed to get any information about the plugin.");
			}
			break;
		case "echo":
			if(null != pmd) {
				so.appendToResponse(so.getApplication());
				ret = true;
			}else {
				Clog.Error(so, "PlugInInfo", "100.01", "Failed to get any information about the plugin.");
			}
			break;
		case "cdbcreatepool":
			if(!cruConnection.isPoolLoaded(so, service)) {
				ret = cruConnection.createConnectionPool(so,service);
				gro.addParmeter("PoolCreated", new Boolean(ret).toString());
				so.appendToResponse(service.Service()+"."+service.Action(),gro);
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
				Clog.Error(so, "GetPoolInfo", "100.01", "Failed to get any information about the pool");
			}
			break;
		case "cdbgettableinfo":
			dbmd = cruConnection.getPoolMetaData(so, service);
			if(dbmd.getTables().containsKey(service.Parameter("tableName"))) {
				TableMetaData tmd = dbmd.getTables().get(service.Parameter("tableName"));
				so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("tableName"), tmd);
				ret = true;
			}else {
				Clog.Error(so, "GetTableInfo", "100.02", "Failed to get any information about the pool");
			}
			break;
		case "cdbgetconnection":
			
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					so.setRequestState(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"), cruConn);
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
			break;
		case "select":
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
						ArrayNode resultMap= JsonNodeRowMapper.mapRows(so, rs);
						if(null != resultMap) {
							gro.addObjectParmeter("Results", resultMap);
						}else {
							gro.addParmeter("Results", "");
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
			break;
		case "update":
			try {
				cruConn = cruConnection.getNewConnection(so, service);
				if(null != cruConn) {
					String query = QueryBuilder.createUpdate(so, service);
					if(null != service.Parameter("includeQuery") && service.Parameter("includeQuery").equalsIgnoreCase("true")) {
						gro.addParmeter("Query", query);
					}
					so.appendToResponse(service.Service()+"."+service.Action()+"."+service.Parameter("poolName"),gro);
					Integer updated = cruConn.getConn().prepareStatement(query).executeUpdate();
					gro.addParmeter("UpdateCount", updated.toString());
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
			break;
		default:
			Clog.Error(so, "service", "100.05", "Invalid Action supplied:"+action);
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






}
