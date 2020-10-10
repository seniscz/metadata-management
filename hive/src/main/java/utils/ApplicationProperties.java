package utils;

import org.apache.commons.configuration.Configuration;
import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.exceptions.HiveHookException;

import java.io.File;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLDecoder;
import java.util.Iterator;

/**
 * @ClassName ApplicationProperties
 * @Description TODO
 * @Author chezhao
 * @Date 2020/9/22 16:39
 * @Version 1.0
 **/
public class ApplicationProperties extends PropertiesConfiguration {
    private static final Logger logger = LoggerFactory.getLogger(ApplicationProperties.class);

    public static final String CONFIGURATION_DIRECTORY_PROPERTY = "metadata-hivehook.conf";
    public static final String APPLICATION_PROPERTIES = "kafka-application.properties";

    private static volatile Configuration instance = null;

    private ApplicationProperties(URL url) throws ConfigurationException {
        super(url);
    }

    public static void forceReload() {
        if (instance != null) {
            synchronized (ApplicationProperties.class) {
                if (instance != null) {
                    instance = null;
                }
            }
        }
    }

    public static Configuration get() throws HiveHookException {
        if (instance == null) {
            synchronized (ApplicationProperties.class) {
                if (instance == null) {
                    instance = get(APPLICATION_PROPERTIES);
                }
            }
        }
        return instance;
    }

    public static Configuration get(String fileName) throws HiveHookException {
        String confLocation = System.getProperty(CONFIGURATION_DIRECTORY_PROPERTY);
        // get from jar path
        if(StringUtils.isBlank(confLocation)) {
            confLocation = getProjectPath();
        }
        try {
            URL url = null;

            if (confLocation == null) {
                logger.info("Looking for {} in classpath", fileName);
                url = ApplicationProperties.class.getClassLoader().getResource(fileName);
                if (url == null) {
                    logger.info("Looking for /{} in classpath", fileName);
                    url = ApplicationProperties.class.getClassLoader().getResource("/" + fileName);
                }
            } else {
                url = new File(confLocation, fileName).toURI().toURL();
            }
            logger.info("Loading {} from {}", fileName, url);
            ApplicationProperties appProperties = new ApplicationProperties(url);
            Configuration configuration = appProperties.interpolatedConfiguration();
            logConfiguration(configuration);
            return configuration;
        } catch (Exception e) {
            throw new HiveHookException("Failed to load application properties", e);
        }
    }

    private static void logConfiguration(Configuration configuration) {
        if (logger.isDebugEnabled()) {
            Iterator<String> keys = configuration.getKeys();
            logger.debug("Configuration loaded:");
            while (keys.hasNext()) {
                String key = keys.next();
                logger.debug("{} = {}", key, configuration.getProperty(key));
            }
        }
    }

    /**
     * Get the project path
     * @return
     * @throws UnsupportedEncodingException
     */
    private static String getProjectPath() {
        String filePath = null;
        try
        {
            URL url = ApplicationProperties.class.getProtectionDomain().getCodeSource().getLocation();
            if(url != null) {
                filePath = URLDecoder.decode(url.getPath(), "utf-8");
                filePath = filePath.replace("\\", "/");
                if (filePath.endsWith(".jar")) {
                    filePath = filePath.substring(0, filePath.lastIndexOf("/") + 1);
                }
                File file = new File(filePath);
                filePath = file.getAbsolutePath();
            }
        } catch (Exception e) {
            //ignore
            filePath = null;
        }
        return filePath;
    }







}
