### Google Cloud Initial Setup

1. Create an account with your Google email   
2. Setup your first project   
3. Setup `service account & authentication` as described [here](https://cloud.google.com/docs/authentication/getting-started)   
    * Grant `Viewer` role   
    * Download   
    * Create your google service-account-keys (.json) for auth and save the file in this path   
        `~/.google/credentials/` 
    * Download `SDK` for local setup as described [here](https://cloud.google.com/sdk/docs/quickstart)
    * Set environment variable to point to your downloaded GCP keys:
   ```shell
   export GOOGLE_APPLICATION_CREDENTIALS="<path/to/your/service-account-authkeys>.json"
   
   gcloud auth application-default login
   ```
   
### Setup for Access
 
1. IAM Roles for Service account [documentation](https://cloud.google.com/storage/docs/access-control/iam-roles):
   * Go to the [*IAM*](https://console.cloud.google.com/iam-admin/iam) section of *IAM & Admin* 
   * Click the *Edit principal* icon for your service account.
   * Add these roles in addition to `Viewer`:    
      `Storage Admin`   
      `Storage Object Admin`   
      `BigQuery Admin`   
   
2. Enable these APIs for your project:
   * https://console.cloud.google.com/apis/library/iam.googleapis.com
   * https://console.cloud.google.com/apis/library/iamcredentials.googleapis.com
