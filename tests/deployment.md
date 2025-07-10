# Data Processing Service Deployment - FAQ

## Frequently Asked Questions

### Security and Access Control

#### Q: How do I configure Cloud Run services to only accept Pub/Sub requests while blocking public internet access?

**Problem**: When deploying a Cloud Run service that needs to receive Pub/Sub push notifications, there's a security challenge:

- **Option 1**: Allow `allUsers` access (IAM policy with `--member="allUsers"`)
  - ✅ Pub/Sub can call the service without authentication issues
  - ❌ **Security Risk**: Anyone on the internet can call your service endpoints
  - ❌ Service is completely exposed to public internet attacks

- **Option 2**: Require authentication (remove `allUsers` access)
  - ✅ Service is secure from unauthorized access
  - ❌ Pub/Sub push requests fail with `403 Forbidden` errors
  - ❌ Need complex authentication setup for Pub/Sub service accounts

- **Option 3**: Use `--ingress=internal` setting
  - ✅ Blocks all external internet access
  - ❌ Also blocks legitimate Pub/Sub push requests
  - ❌ Service becomes completely inaccessible for push subscriptions

**Current Symptoms**:
- With authentication required: Pub/Sub push requests return `403 Forbidden`
- With `allUsers` access: Service is vulnerable to public internet attacks
- With internal ingress: Both public and Pub/Sub requests are blocked

**Architecture Challenge**: 
Cloud Run services need to be accessible to Google Cloud Pub/Sub service (`service-[PROJECT-NUMBER]@gcp-sa-pubsub.iam.gserviceaccount.com`) for push subscriptions, but ideally should not be accessible to arbitrary internet traffic.

**Related Error Messages**:
```
POST 403 https://data-processing-service-xxx.a.run.app/api/v1/events/data-ingestion-completed
```

**Service Configuration Context**:
- Project: competitor-destroyer  
- Region: asia-southeast1
- Service: data-processing-service
- Subscription: data-processing-ingestion-events
- Topic: social-analytics-data-ingestion-completed
- Pub/Sub Service Account: service-366008494339@gcp-sa-pubsub.iam.gserviceaccount.com

**Additional Considerations**:
- Push subscriptions require the service to be reachable from Google's Pub/Sub infrastructure
- Pull subscriptions would require the service to authenticate and poll, changing the architecture
- Application-level validation could verify request sources but requires careful implementation
- Network-level controls (VPC, private services) add complexity to the deployment

---

## ✅ **SOLUTION: Authenticated Push Subscriptions**

**Status**: ✅ **RESOLVED** - Secure configuration that blocks public access while allowing Pub/Sub

### **Root Cause**
The issue was missing the **Service Account Token Creator** role for the Pub/Sub service account, which prevented Pub/Sub from creating JWT tokens for authenticated push requests.

### **Complete Solution Steps**

#### **Step 1: Grant Service Account Token Creator Role**
```bash
# Grant Token Creator role to Pub/Sub service account
gcloud projects add-iam-policy-binding competitor-destroyer \
    --member="serviceAccount:service-366008494339@gcp-sa-pubsub.iam.gserviceaccount.com" \
    --role="roles/iam.serviceAccountTokenCreator"
```

#### **Step 2: Remove Public Access (If Present)**
```bash
# Remove allUsers access to secure the service
gcloud run services remove-iam-policy-binding data-processing-service \
    --region=asia-southeast1 \
    --member="allUsers" \
    --role="roles/run.invoker"
```

#### **Step 3: Grant Service Account Access to Cloud Run**
```bash
# Grant data-processing service account permission to invoke Cloud Run
gcloud run services add-iam-policy-binding data-processing-service \
    --region=asia-southeast1 \
    --member="serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com" \
    --role="roles/run.invoker"
```

#### **Step 4: Create Authenticated Push Subscription**
```bash
# Delete existing subscription if it exists
gcloud pubsub subscriptions delete data-processing-ingestion-events

# Create new subscription with authentication
gcloud pubsub subscriptions create data-processing-ingestion-events \
    --topic=social-analytics-data-ingestion-completed \
    --push-endpoint=https://data-processing-service-ud5pi5bwfq-as.a.run.app/api/v1/events/data-ingestion-completed \
    --push-auth-service-account=data-processing-sa@competitor-destroyer.iam.gserviceaccount.com \
    --push-auth-token-audience=https://data-processing-service-ud5pi5bwfq-as.a.run.app \
    --ack-deadline=600
```

### **Verification Commands**

#### **Check IAM Policy**
```bash
# Verify Cloud Run service permissions
gcloud run services get-iam-policy data-processing-service --region=asia-southeast1

# Should show:
# - serviceAccount:data-processing-sa@competitor-destroyer.iam.gserviceaccount.com
# - serviceAccount:service-366008494339@gcp-sa-pubsub.iam.gserviceaccount.com  
# - user:qbao2805@gmail.com
# role: roles/run.invoker
```

#### **Check Subscription Configuration**
```bash
# Verify subscription is configured with authentication
gcloud pubsub subscriptions describe data-processing-ingestion-events

# Should show pushConfig with authentication settings
```

#### **Test Integration**
```bash
# Publish test message
gcloud pubsub topics publish social-analytics-data-ingestion-completed \
    --message='{"test": "authenticated push"}'

# Check logs (should show successful processing, not 403 errors)
gcloud run services logs read data-processing-service \
    --region=asia-southeast1 --limit=10
```

### **Security Benefits Achieved**

✅ **No Public Access**: Service cannot be called by arbitrary internet users  
✅ **Authenticated Pub/Sub**: Only authenticated Pub/Sub requests are accepted  
✅ **JWT Validation**: Service receives valid JWT tokens from Google  
✅ **Service Account Control**: Specific service accounts control access  
✅ **Audit Trail**: All requests are logged and traceable  

### **Key Documentation Reference**

This solution follows the official Google Cloud documentation:
**"Authentication for push subscriptions"** - Pub/Sub Documentation

**Critical Requirement**: The Pub/Sub service account **must** have the `iam.serviceAccountTokenCreator` role to generate JWT tokens for authenticated push requests.

### **Common Troubleshooting**

- **403 Errors**: Check if Service Account Token Creator role is granted
- **Permission Denied**: Ensure you have `iam.serviceAccounts.actAs` permission on the push auth service account
- **Subscription Creation Fails**: Verify the push auth service account exists and you have access to it
- **Still Getting Public Access**: Verify `allUsers` binding is removed from Cloud Run service

### **Architecture Result**

```
Internet Users ❌ BLOCKED
       ↓
Cloud Run Service (Authenticated Only)
       ↑
Google Pub/Sub ✅ AUTHENTICATED JWT
       ↑
Published Messages → Topic → Push Subscription
```

**Status**: 🎉 **Production Ready** - Secure and functional Pub/Sub integration

---

## ❌ **CRITICAL ISSUE: Missing Pub/Sub Topics - Error Storm**

**Date**: July 10, 2025  
**Status**: ✅ **RESOLVED** - Critical infrastructure issue that caused service failure

### **Problem Description**
The data-processing service was experiencing an **error storm** with continuous 404 errors and 500 Internal Server errors due to missing Pub/Sub topics that the service was trying to publish to.

### **Symptoms Observed**
- **Error Storm**: Hundreds of 404 errors per minute in Cloud Run logs
- **500 Internal Server Errors**: Service returning 500 to all requests
- **Pub/Sub Publishing Failures**: `404 Resource not found (resource=social-analytics-data-processing-failed)`
- **Service Degradation**: High error rates causing service instability
- **Resource Exhaustion**: Continuous retry loops consuming service resources

### **Root Cause**
The data-processing service was configured to publish events to Pub/Sub topics that **did not exist**:
- `social-analytics-data-processing-started` ❌ Missing
- `social-analytics-data-processing-completed` ❌ Missing  
- `social-analytics-data-processing-failed` ❌ Missing
- `social-analytics-media-processing-requested` ❌ Missing

### **Error Messages in Logs**
```
ERROR:events.event_publisher:Error publishing event data-processing-failed: 
404 Resource not found (resource=social-analytics-data-processing-failed).

ERROR:google.cloud.pubsub_v1.publisher._batch.thread:Failed to publish 1 messages.

google.api_core.exceptions.NotFound: 404 Resource not found 
(resource=social-analytics-data-processing-failed).
```

### **Impact**
- ❌ **Service Failure**: Data-processing pipeline completely broken
- ❌ **Error Storm**: Logs flooded with errors, making debugging difficult  
- ❌ **Resource Waste**: Continuous failed retry attempts
- ❌ **Data Loss**: Events could not be processed or published
- ❌ **Monitoring Noise**: False alerts from error volume

### **Solution Steps**

#### **1. Create Missing Pub/Sub Topics**
```bash
# Create all required topics for data-processing service
gcloud pubsub topics create social-analytics-data-processing-started --project=competitor-destroyer
gcloud pubsub topics create social-analytics-data-processing-completed --project=competitor-destroyer  
gcloud pubsub topics create social-analytics-data-processing-failed --project=competitor-destroyer
gcloud pubsub topics create social-analytics-media-processing-requested --project=competitor-destroyer
```

#### **2. Verify Topics Creation**
```bash
# Verify all topics exist
gcloud pubsub topics list | grep social-analytics
# Should show 7 total topics including the 4 new ones
```

#### **3. Monitor Service Recovery**
```bash
# Check that error storm has stopped
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=data-processing-service AND severity=ERROR AND timestamp>=\"$(date -u -v-5M +%Y-%m-%dT%H:%M:%S)Z\"" --limit=5
# Should show no recent errors
```

### **Prevention for Future**

#### **Deployment Checklist**
Before deploying data-processing service, ensure these topics exist:
```bash
# Required topics checklist:
✅ social-analytics-crawl-triggered
✅ social-analytics-crawl-failed  
✅ social-analytics-data-ingestion-completed
✅ social-analytics-data-processing-started      # ← CRITICAL
✅ social-analytics-data-processing-completed    # ← CRITICAL
✅ social-analytics-data-processing-failed       # ← CRITICAL
✅ social-analytics-media-processing-requested   # ← CRITICAL
```

#### **Automated Topic Creation Script**
Create a deployment script that ensures all topics exist:
```bash
#!/bin/bash
# File: scripts/ensure_pubsub_topics.sh

TOPICS=(
    "social-analytics-crawl-triggered"
    "social-analytics-crawl-failed"
    "social-analytics-data-ingestion-completed"
    "social-analytics-data-processing-started"
    "social-analytics-data-processing-completed"  
    "social-analytics-data-processing-failed"
    "social-analytics-media-processing-requested"
)

for topic in "${TOPICS[@]}"; do
    echo "Creating topic: $topic"
    gcloud pubsub topics create "$topic" --project="$GOOGLE_CLOUD_PROJECT" || echo "Topic $topic already exists"
done
```

#### **Service Health Check Enhancement**
Update service health check to verify Pub/Sub connectivity:
```python
# Add to health check endpoint
def check_pubsub_topics():
    required_topics = [
        'social-analytics-data-processing-started',
        'social-analytics-data-processing-completed', 
        'social-analytics-data-processing-failed',
        'social-analytics-media-processing-requested'
    ]
    
    for topic in required_topics:
        try:
            topic_path = publisher.topic_path(project_id, topic)
            publisher.get_topic(request={"topic": topic_path})
        except Exception:
            return False, f"Topic {topic} not found"
    
    return True, "All topics available"
```

### **Verification Commands**
```bash
# Check service is healthy after fix
curl -H "Authorization: Bearer $(gcloud auth print-identity-token)" \
  https://data-processing-service-ud5pi5bwfq-as.a.run.app/health

# Verify no recent errors
gcloud logging read "resource.type=cloud_run_revision AND resource.labels.service_name=data-processing-service AND severity=ERROR" --limit=5

# Test event publishing manually
gcloud pubsub topics publish social-analytics-data-processing-completed --message='{"test": "topic_works"}'
```

### **Lessons Learned**
1. **Infrastructure Dependencies**: Always create Pub/Sub topics before deploying services that use them
2. **Error Handling**: Services should gracefully handle missing topics instead of creating error storms
3. **Deployment Order**: Infrastructure components must be created before application deployment
4. **Health Checks**: Include dependency checks in health endpoints
5. **Monitoring**: Set up alerts for 404 Pub/Sub errors to catch this early

### **Related Documentation**
- [Pub/Sub Topic Management](https://cloud.google.com/pubsub/docs/admin)
- [Cloud Run Service Dependencies](https://cloud.google.com/run/docs/overview)
- [Error Handling Best Practices](https://cloud.google.com/apis/design/errors)

**Status**: ✅ **RESOLVED** - Error storm eliminated, service stable, all topics created