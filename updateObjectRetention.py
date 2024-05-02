
#################################################################################################################
###
### Gets the last modified date from x-amz-meta-last-modified metedata dropped by datasync.  Then gets 
### the Default retention period from the parent bucket.  It then calculates the 
### last modified data + retention period and sets this as the new retention period.  If that date is in the past
### then it does nothing.
###
### rustea@amazon.com
### 28-04-2025
##################################################################################################################

import boto3
import time
import json
import datetime

def lambda_handler(event, context,):
    

    ### Declare all variables below
    ###
    ###
    myBucket = "rr-object-lock-test"
    objectLockDateKey = 'x-amz-object-lock-retain-until-date'
    datasyncLastModifiedKey = 'x-amz-meta-last-modified'
    s3 = boto3.client('s3')
    ### 
    ###
    ### Declare all variables above
    
    
    objectLockInfo = s3.get_object_lock_configuration(Bucket=myBucket)
    print('Bucket objectlock info is ', objectLockInfo['ObjectLockConfiguration'])
    
    getObjectMetaData = s3.list_objects_v2(Bucket=myBucket)

    for object in getObjectMetaData['Contents']:
        objectMeta = s3.head_object(Bucket=myBucket, Key=object['Key'])
        if 'x-amz-meta-last-modified' in objectMeta['ResponseMetadata']['HTTPHeaders']:
            #print(object['Key'])
            dateSyncLastModifyDate = objectMeta['ResponseMetadata']['HTTPHeaders'][datasyncLastModifiedKey]

            dateSyncLastModifyDateSeconds = int(dateSyncLastModifyDate)/1000
            orig = datetime.datetime.fromtimestamp(dateSyncLastModifyDateSeconds)
            new = orig + datetime.timedelta(days=objectLockInfo['ObjectLockConfiguration']['Rule']['DefaultRetention']['Days'])
            newRetentionDate = (new.timestamp())
            current_epoch_timestamp_ms = int(time.time() * 1000)
            
                
            if newRetentionDate*1000 > current_epoch_timestamp_ms:
            
                utc_datetime = datetime.datetime.utcfromtimestamp(newRetentionDate)
                newRetentionDateFormatted = utc_datetime.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        
                retentionKey = {'Mode': 'GOVERNANCE','RetainUntilDate': newRetentionDateFormatted }
                adjustRetention = s3.put_object_retention(Bucket=myBucket, Key=object['Key'], Retention=retentionKey, BypassGovernanceRetention=True)
                print('Object ', object['Key'], ' retention set to ', newRetentionDateFormatted)
            else:
                print('Object ', object['Key'], ' retention date is in the past.  Nothing to do  ')
    
            # Get the x-amz-meta-last-modified metadata and add a human readable tag with month/year just in case we want to run lifecycle policies 
            # to delete data based on this date.
            lastModifiedTag = datetime.datetime.fromtimestamp(dateSyncLastModifyDateSeconds)
            lastModifiedTagYYYYMM = str(lastModifiedTag.year) + '-' + str(lastModifiedTag.month)

            tags = [{'Key': 'LastModified','Value': str(lastModifiedTag)},{'Key': 'lastModifiedYYYYMM','Value': lastModifiedTagYYYYMM}]
            addTags = s3.put_object_tagging(Bucket=myBucket,Key=object['Key'],Tagging={'TagSet': tags})

    print('FIN...............')


