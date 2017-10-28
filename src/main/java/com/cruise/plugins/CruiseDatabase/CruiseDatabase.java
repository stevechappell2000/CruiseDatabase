package com.cruise.plugins.CruiseDatabase;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.services.interfaces.PluginInterface;
import com.corecruise.cruise.services.utils.Services;

/**
 * Hello world!
 *
 */
public class CruiseDatabase implements PluginInterface
{
	static String[] actions = new String[]{"cDBAdd","cDBUpdate","cDBDelete","cDBFind"};
	static String name = "CruiseDatabase";
	static String version = "0.0.1";
	static String vendor = "SJC";
    public CruiseDatabase() {
    	
    }
	@Override
	public String[] getSupportedActions() {
		// TODO Auto-generated method stub
		return actions;
	}
	@Override
	public String getPluginName() {
		// TODO Auto-generated method stub
		return name;
	}
	@Override
	public String getPluginVersion() {
		// TODO Auto-generated method stub
		return version;
	}
	@Override
	public boolean executePlugin(SessionObject so, Services service) {
		// TODO Auto-generated method stub
		return true;
	}
	@Override
	public String getPluginVendor() {
		// TODO Auto-generated method stub
		return vendor;
	}




}
