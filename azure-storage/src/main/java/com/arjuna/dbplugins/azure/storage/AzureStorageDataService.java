/*
 * Copyright (c) 2015, Arjuna Technologies Limited, Newcastle-upon-Tyne, England. All rights reserved.
 */

package com.arjuna.dbplugins.azure.storage;

import java.net.URI;
import java.util.Calendar;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.EnumSet;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TimeZone;
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
import com.microsoft.azure.storage.CloudStorageAccount;
import com.microsoft.azure.storage.blob.BlobContainerPermissions;
import com.microsoft.azure.storage.blob.BlobContainerPublicAccessType;
import com.microsoft.azure.storage.blob.CloudBlobClient;
import com.microsoft.azure.storage.blob.CloudBlobContainer;
import com.microsoft.azure.storage.blob.CloudBlockBlob;
import com.microsoft.azure.storage.blob.SharedAccessBlobPermissions;
import com.microsoft.azure.storage.blob.SharedAccessBlobPolicy;

public class AzureStorageDataService implements DataService
{
    private static final Logger logger = Logger.getLogger(AzureStorageDataService.class.getName());

    public static final String SERVICEBASEURL_PROPERTYNAME    = "Service Base URL";
    public static final String CONTAINERNAME_PROPERTYNAME     = "Container Name";
    public static final String ACCOUNTNAME_PROPERTYNAME       = "Account Name";
    public static final String ACCOUNTKEY_PROPERTYNAME        = "Account Key";
    public static final String STORAGECONNECTION_PROPERTYNAME = "Storage Connection";
    public static final String CONTAINERSAS_PROPERTYNAME      = "Container SAS";

    public AzureStorageDataService()
    {
        logger.log(Level.FINE, "AzureStorageDataService");
    }

    public AzureStorageDataService(String name, Map<String, String> properties)
    {
        logger.log(Level.FINE, "AzureStorageDataService: " + name + ", " + properties);

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
        _serviceBaseURL    = _properties.get(SERVICEBASEURL_PROPERTYNAME);
        _containerName     = _properties.get(CONTAINERNAME_PROPERTYNAME);
        _storageConnection = _properties.get(STORAGECONNECTION_PROPERTYNAME);
        _accountName       = _properties.get(ACCOUNTNAME_PROPERTYNAME);
        _accountKey        = _properties.get(ACCOUNTKEY_PROPERTYNAME);
        _containerSAS      = _properties.get(CONTAINERSAS_PROPERTYNAME);

        if ((_containerSAS != null) && (! "".equals(_containerSAS.trim())))
        {
            String storageConnection = null;
            if ((_storageConnection != null) && (! "".equals(_storageConnection.trim())))
                storageConnection = _storageConnection;
            else if ((_accountName != null) && (! "".equals(_accountName.trim())) && (_accountKey != null) && (! "".equals(_accountKey.trim())))
                storageConnection = "DefaultEndpointsProtocol=http;AccountName=" + _accountName + ";AccountKey=" + _accountKey;

            if (storageConnection != null)
            {
                try
                {
                    CloudStorageAccount storageAccount = CloudStorageAccount.parse(storageConnection);
                    CloudBlobClient     blobClient     = storageAccount.createCloudBlobClient();
                    CloudBlobContainer  blobContainer  = blobClient.getContainerReference(_containerName);
                    blobContainer.createIfNotExists();

                    SharedAccessBlobPolicy blobPolicy   = new SharedAccessBlobPolicy();
                    GregorianCalendar      calendar = new GregorianCalendar(TimeZone.getTimeZone("UTC"));
                    calendar.setTime(new Date());
                    blobPolicy.setSharedAccessStartTime(calendar.getTime());
                    calendar.add(Calendar.HOUR, 6);
                    blobPolicy.setSharedAccessExpiryTime(calendar.getTime());
                    blobPolicy.setPermissions(EnumSet.of(SharedAccessBlobPermissions.READ, SharedAccessBlobPermissions.WRITE));

                    BlobContainerPermissions containerPermissions = new BlobContainerPermissions();
                    containerPermissions.setPublicAccess(BlobContainerPublicAccessType.OFF);
                    containerPermissions.getSharedAccessPolicies().put("accesspolicy", blobPolicy);
                    blobContainer.uploadPermissions(containerPermissions);

                    _containerSAS = blobContainer.generateSharedAccessSignature(blobPolicy, null);
                }
                catch (Throwable throwable)
                {
                    logger.log(Level.WARNING, "Problems with Azure blob store SAS", throwable);
                }
            }
        }
    }

    @PreConfig
    @PreDelete
    public void teardown()
    {
        _serviceBaseURL    = null;
        _containerName     = null;
        _storageConnection = null;
        _accountName       = null;
        _accountKey        = null;
        _containerSAS      = null;
    }

    public void consumeString(String data)
    {
        logger.log(Level.FINE, "AzureStorageDataService.consumeString");

        try
        {
            uploadResource(data.getBytes(), null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure blob store api invoke", throwable);
        }
    }

    public void consumeBytes(byte[] data)
    {
        logger.log(Level.FINE, "AzureStorageDataService.consumeBytes");

        try
        {
            uploadResource(data, null, null, null, null);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure blob store api invoke", throwable);
        }
    }

    public void consumeMap(Map map)
    {
        logger.log(Level.FINE, "AzureStorageDataService.consumeMap");

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
            logger.log(Level.WARNING, "Problems with Azure blob store api invoke", throwable);
        }
    }

    private void uploadResource(byte[] data, String blobName, String resourceName, String resourceFormat, String resourceDescription)
    {
        logger.log(Level.FINE, "AzureStorageDataService.consume");

        try
        {
            CloudBlobClient blobClient = new CloudBlobClient(new URI(_serviceBaseURL));
            URI             blobURI    = new URI(blobClient.getEndpoint().toString() + "/" + _containerName + "/" + blobName + "?" + _containerSAS);
            CloudBlockBlob  blockBlob  = new CloudBlockBlob(blobURI);

            HashMap<String, String> metadata = new HashMap<String, String>();
            if (resourceName != null)
                metadata.put("resourceName", resourceName);
            if (resourceFormat != null)
                metadata.put("resourceFormat", resourceFormat);
            if (resourceDescription != null)
                metadata.put("resourceDescription", resourceDescription);
            blockBlob.setMetadata(metadata);

            blockBlob.uploadFromByteArray(data, 0, data.length);
        }
        catch (Throwable throwable)
        {
            logger.log(Level.WARNING, "Problems with Azure blob store api invoke", throwable);
        }
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
        if (dataClass == String.class)
            return (DataConsumer<T>) _dataConsumerString;
        else if (dataClass == byte[].class)
            return (DataConsumer<T>) _dataConsumerBytes;
        else if (dataClass == Map.class)
            return (DataConsumer<T>) _dataConsumerMap;
        else
            return null;
    }

    private String _serviceBaseURL;
    private String _containerName;
    private String _storageConnection;
    private String _accountName;
    private String _accountKey;
    private String _containerSAS;

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
