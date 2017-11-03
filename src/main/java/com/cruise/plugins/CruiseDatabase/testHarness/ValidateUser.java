package com.cruise.plugins.CruiseDatabase.testHarness;

import java.util.HashMap;

import com.corecruise.cruise.SessionObject;
import com.corecruise.cruise.services.interfaces.ValidationInterface;

public class ValidateUser implements ValidationInterface {
    HashMap<String,String> validUsers = new HashMap<String,String>();
    boolean ok = false;

	public boolean isInitialized() {
		// TODO Auto-generated method stub
		return ok;
	}

	/**
	 * Simple validate via a hashmap of usernames and passwords.
	 * @return
	 */

	public boolean initializeValidation() {
		validUsers.put("admin", "admin");
		validUsers.put("manager", "manager");
		validUsers.put("developer", "developer");
		validUsers.put("user", "user");
		
		ok = true;
		return ok;
	}

	public boolean validateRequest(SessionObject obj) {
		boolean ret = false;
		if(null != obj.getApplication().getCredentials()) {
			try {
				if(validUsers.containsKey(obj.getApplication().getCredentials().Parameter("username"))){
				   if(obj.getApplication().getCredentials().Parameter("password").equalsIgnoreCase(validUsers.get(obj.getApplication().getCredentials().Parameter("username")))){
					   ret = true;
				   }
				}
			}catch(Exception e) {
				obj.setError(true);
				obj.getApplication().getCredentials().setError(e.getMessage());
			}
		}
		return ret;
	}

	public boolean validateUserPass(String User, String Pass) {
		// TODO Auto-generated method stub
		return false;
	}

	public boolean validateRequest(SessionObject sobj, Object obj) {
		// TODO Auto-generated method stub
		return false;
	}

}
