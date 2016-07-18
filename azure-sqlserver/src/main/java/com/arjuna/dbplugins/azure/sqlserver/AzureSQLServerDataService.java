/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.azure.sqlserver;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import com.arjuna.databroker.data.DataConsumer;
import com.arjuna.databroker.data.DataFlow;
import com.arjuna.databroker.data.DataProvider;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.jee.annotation.DataConsumerInjection;
import com.arjuna.databroker.data.jee.annotation.PostConfig;
import com.arjuna.databroker.data.jee.annotation.PostCreated;
import com.arjuna.databroker.data.jee.annotation.PostRecovery;
import com.arjuna.databroker.data.jee.annotation.PreConfig;
import com.arjuna.databroker.data.jee.annotation.PreDelete;

public class AzureSQLServerDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(AzureSQLServerDataService.class.getName());

    public static final String SERVERNAME_PROPERTYNAME   = "Server Name";
    public static final String DATABASENAME_PROPERTYNAME = "Database Name";
    public static final String USER_PROPERTYNAME         = "User";
    public static final String PASSWORD_PROPERTYNAME     = "Password";

    public AzureSQLServerDataService()
    {
        logger.log(Level.FINE, "AzureSQLServerDataService");
    }

    public AzureSQLServerDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "AzureSQLServerDataService: " + name + ", " + properties);

        _name       = name;
        _properties = properties;
    }

    @Override
    public DataFlow getDataFlow()
    {
        return _dataFlow;
    }

    @Override
    public void setDataFlow(DataFlow dataFlow)
    {
        _dataFlow = dataFlow;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public void setName(String name)
    {
        _name = name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return Collections.unmodifiableMap(_properties);
    }

    @Override
    public void setProperties(Map<String, String> properties)
    {
        _properties = properties;
    }

    @PostConfig
    @PostCreated
    @PostRecovery
    public void setup()
    {
        _serverName   = _properties.get(SERVERNAME_PROPERTYNAME);
        _databaseName = _properties.get(DATABASENAME_PROPERTYNAME);
        _user         = _properties.get(USER_PROPERTYNAME);
        _password     = _properties.get(PASSWORD_PROPERTYNAME);

    }

    @PreConfig
    @PreDelete
    public void teardown()
    {
        _serverName   = null;
        _databaseName = null;
        _user         = null;
        _password     = null;
    }

    public void consumeString(String data)
    {
        logger.log(Level.FINE, "AzureSQLServerDataService.consumeString");

        try
        {
            uploadResource(data.getBytes(), null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure SQL Server API invoke", throwable);
        }
    }

    public void consumeBytes(byte[] data)
    {
        logger.log(Level.FINE, "AzureSQLServerDataService.consumeBytes");

        try
        {
            uploadResource(data, null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure SQL Server API invoke", throwable);
        }
    }

    public void consumeMap(Map map)
    {
        logger.log(Level.FINE, "AzureSQLServerDataService.consumeMap");

        try
        {
            byte[] data                = (byte[]) map.get("data");
            String fileName            = (String) map.get("filename");
            String resourceName        = (String) map.get("resourcename");
            String resourceFormat      = (String) map.get("resourceformat");
            String resourceDescription = (String) map.get("resourcedescription");

            uploadResource(data, fileName, resourceName, resourceFormat, resourceDescription);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure SQL Server API invoke", throwable);
        }
    }

    private void uploadResource(byte[] data, String blobName, String resourceName, String resourceFormat, String resourceDescription)
    {
        logger.log(Level.FINE, "AzureSQLServerDataService.consume");

        try
        {
            String connectionURL = "jdbc:sqlserver://" + _serverName + ":1433;" + "databaseName=" + _databaseName + ";user=" + _user + ";password=" + _password;

            Connection connection = null;
            Statement  statement  = null;

            try
            {
                String sql = generateUpdateSQL(data);
                if (sql != null)
                {
                    Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
                    connection = DriverManager.getConnection(connectionURL);

                    

                    statement = connection.createStatement();

                    boolean result = statement.execute(sql);
                    if (! result)
                        logger.log(Level.WARNING, "Problems during updating");
                }
            }
            catch (Exception exception)
            {
                exception.printStackTrace();
            }
            finally
            {
               if (statement != null)
                   try
                   {
                       statement.close();
                   }
                   catch (Exception exception)
                   {
                       logger.log(Level.WARNING, "Problems closing SQL Server statement", exception);
                   }
               if (connection != null)
                   try
                   {
                       connection.close();
                   }
                   catch (Exception exception)
                   {
                       logger.log(Level.WARNING, "Problems closing SQL Server connection", exception);
                   }
            }
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure SQL Server API invoke", throwable);
        }
    }

    private String generateUpdateSQL(byte[] data)
    {
        return null;
    }

    @Override
    public Collection<Class<?>> getDataProviderDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        return dataConsumerDataClasses;
    }

    @Override
    public <T> DataProvider<T> getDataProvider(Class<T> dataClass)
    {
        return null;
    }

    @Override
    public Collection<Class<?>> getDataConsumerDataClasses()
    {
        Set<Class<?>> dataConsumerDataClasses = new HashSet<Class<?>>();

        dataConsumerDataClasses.add(String.class);
        dataConsumerDataClasses.add(byte[].class);
        dataConsumerDataClasses.add(Map.class);

        return dataConsumerDataClasses;
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> DataConsumer<T> getDataConsumer(Class<T> dataClass)
    {
        if (String.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumerString;
        else if (byte[].class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumerBytes;
        else if (Map.class.isAssignableFrom(dataClass))
            return (DataConsumer<T>) _dataConsumerMap;
        else
            return null;
    }

    private String _serverName;
    private String _databaseName;
    private String _user;
    private String _password;

    private DataFlow             _dataFlow;
    private String               _name;
    private Map<String, String>  _properties;
    @DataConsumerInjection(methodName="consumeString")
    private DataConsumer<String> _dataConsumerString;
    @DataConsumerInjection(methodName="consumeBytes")
    private DataConsumer<byte[]> _dataConsumerBytes;
    @DataConsumerInjection(methodName="consumeMap")
    private DataConsumer<Map>    _dataConsumerMap;
}
