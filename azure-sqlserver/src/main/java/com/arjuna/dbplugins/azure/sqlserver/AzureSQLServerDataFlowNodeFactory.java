/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.azure.sqlserver;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import com.arjuna.databroker.data.DataFlowNode;
import com.arjuna.databroker.data.DataFlowNodeFactory;
import com.arjuna.databroker.data.DataService;
import com.arjuna.databroker.data.InvalidClassException;
import com.arjuna.databroker.data.InvalidMetaPropertyException;
import com.arjuna.databroker.data.InvalidNameException;
import com.arjuna.databroker.data.InvalidPropertyException;
import com.arjuna.databroker.data.MissingMetaPropertyException;
import com.arjuna.databroker.data.MissingPropertyException;

public class AzureSQLServerDataFlowNodeFactory implements DataFlowNodeFactory
{
    public AzureSQLServerDataFlowNodeFactory(String name, Map<String, String> properties)
    {
        _name       = name;
        _properties = properties;
    }

    @Override
    public String getName()
    {
        return _name;
    }

    @Override
    public Map<String, String> getProperties()
    {
        return _properties;
    }

    @Override
    public List<Class<? extends DataFlowNode>> getClasses()
    {
        List<Class<? extends DataFlowNode>> classes = new LinkedList<Class<? extends DataFlowNode>>();

        classes.add(DataService.class);

        return classes;
    }

    @Override
    public <T extends DataFlowNode> List<String> getMetaPropertyNames(Class<T> dataFlowNodeClass)
        throws InvalidClassException
    {
        if (dataFlowNodeClass.equals(DataService.class))
        {
            List<String> metaPropertyNames = new LinkedList<String>();

            return metaPropertyNames;
        }
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    @Override
    public <T extends DataFlowNode> List<String> getPropertyNames(Class<T> dataFlowNodeClass, Map<String, String> metaProperties)
        throws InvalidClassException, InvalidMetaPropertyException, MissingMetaPropertyException
    {
        if (dataFlowNodeClass.equals(DataService.class))
        {
            if (metaProperties.isEmpty())
            {
                List<String> propertyNames = new LinkedList<String>();

                propertyNames.add(AzureSQLServerDataService.SERVERNAME_PROPERTYNAME);
                propertyNames.add(AzureSQLServerDataService.DATABASENAME_PROPERTYNAME);
                propertyNames.add(AzureSQLServerDataService.USER_PROPERTYNAME);
                propertyNames.add(AzureSQLServerDataService.PASSWORD_PROPERTYNAME);

                return propertyNames;
            }
            else
                throw new MissingMetaPropertyException("No metaproperties expected", null);
        }
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends DataFlowNode> T createDataFlowNode(String name, Class<T> dataFlowNodeClass, Map<String, String> metaProperties, Map<String, String> properties)
        throws InvalidClassException, InvalidNameException, InvalidMetaPropertyException, MissingMetaPropertyException, InvalidPropertyException, MissingPropertyException
    {
        if (dataFlowNodeClass.equals(DataService.class))
        {
            if (metaProperties.isEmpty())
                return (T) new AzureSQLServerDataService(name, properties);
            else
                throw new InvalidMetaPropertyException("No metaproperties expected", null, null);
        }
        else
            throw new InvalidClassException("Unsupported class", dataFlowNodeClass.getName());
    }

    private String              _name;
    private Map<String, String> _properties;
}
