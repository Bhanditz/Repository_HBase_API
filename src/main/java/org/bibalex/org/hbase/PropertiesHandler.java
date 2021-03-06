package org.bibalex.org.hbase;

import java.io.InputStream;
import java.util.Properties;
import java.io.IOException;

public class PropertiesHandler {

    static Properties prop = new Properties();

    public static void initializeProperties() throws IOException {
        InputStream input = PropertiesHandler.class.getClassLoader().getResourceAsStream("config.properties");
        prop.load(input);
    }

    public static String getProperty(String key){
        return prop.getProperty(key);
    }

}
