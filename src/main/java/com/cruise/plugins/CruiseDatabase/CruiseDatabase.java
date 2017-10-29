package com.cruise.plugins.CruiseDatabase;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.logging.Clog;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Services;
import com.cruise.plugins.CruiseDatabase.utils.cruConnection;

/**
 * Hello world!
 *
 */
public class CruiseDatabase implements PluginInterface
{
	static String[] actions = new String[]{"cDBAdd","cDBUpdate","cDBDelete","cDBFind","getConnection"};
	static String name = "CruiseDatabase";
	static String version = "0.0.1";
	static String vendor = "SJC";
    public CruiseDatabase() {
    	
    }

	public String[] getSupportedActions() {
		// TODO Auto-generated method stub
		return actions;
	}

	public String getPluginName() {
		// TODO Auto-generated method stub
		return name;
	}

	public String getPluginVersion() {
		// TODO Auto-generated method stub
		return version;
	}

	public boolean executePlugin(SessionObject so, Services service) {
		boolean ret = false;
		String action = service.getAction();
	     switch (action) {
         case "getConnection":
        	 ret = cruConnection.createConnection();
             break;
         case "cDBFind":

             break;
         default:
            Clog.Error(so, "service", "90001", "Invalid Action supplied:"+action);
     }
		return ret;
	}

	public String getPluginVendor() {
		// TODO Auto-generated method stub
		return vendor;
	}




}
