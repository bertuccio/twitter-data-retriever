package util;

import java.util.ArrayList;
import java.util.List;
import java.util.MissingResourceException;
import java.util.ResourceBundle;

public class Resource {

	private final static String RESOURCE_KEYWORDS = "keywords";
	private final static String RESOURCE_BLACKLIST = "blacklist";

	private static List<String> getResources(String nameResource) throws MissingResourceException {

		List<String> resources = new ArrayList<String>();
		ResourceBundle bundle = ResourceBundle.getBundle(nameResource);
		for (String s : bundle.getString(nameResource).split("[,  ;]+"))
			resources.add(s);

		if (resources.isEmpty())
			throw new MissingResourceException(nameResource, nameResource, nameResource);

		return resources;
	}

	public static List<String> getKeywords() throws MissingResourceException {
		return getResources(RESOURCE_KEYWORDS);
	}

	public static List<String> getBlackList() throws MissingResourceException {
		return getResources(RESOURCE_BLACKLIST);
	}

}
