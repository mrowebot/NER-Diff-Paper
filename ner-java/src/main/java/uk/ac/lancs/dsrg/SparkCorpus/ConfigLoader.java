package uk.ac.lancs.dsrg.SparkCorpus;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by danielkershaw on 12/04/15.
 */
public class ConfigLoader {

    private static ConfigLoader conf = null;
    Properties prop = new Properties();
    String propFileName = "config.properties";
    final static Logger logger = Logger.getLogger(ConfigLoader.class);

    public static ConfigLoader getInstance(){
        if(conf == null){
            try {
                conf = new ConfigLoader();
            }
            catch(IOException e){
                logger.error("Sorry, something wrong!", e);
            }
        }
        return conf;
    }

    protected ConfigLoader() throws IOException {
        InputStream inputStream = getClass().getClassLoader().getResourceAsStream(propFileName);
            if (inputStream != null) {
                prop.load(inputStream);
            } else {
                throw new FileNotFoundException("property file '" + propFileName + "' not found in the classpath");
            }

    }

    public String getValue(String key){
        return prop.getProperty(key);
    }
}
