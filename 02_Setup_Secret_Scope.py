# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
# DBTITLE 1,Set Connection Variables for Secret Scope Configuration
# =====================================================
# 1️⃣ Define Connection Variables (EDIT THESE)
# =====================================================

# Secret scope name (will be created if not exists)
secret_scope_name = "finguard-scope"

# COMMAND ----------

# DBTITLE 1,Setup Databricks API Context and Authentication Variabl ...
# This code is preparing to call the Databricks REST API from inside a Databricks notebook. Its purpose is to get the current workspace URL and an authentication token, then store them in variables so you can create or configure a secret scope.

# Python cell in the same workspace notebook
ctx = dbutils.notebook.entry_point.getDbutils().notebook().getContext() #discover which Databricks workspace is running this notebook.

api_url  = ctx.apiUrl().getOrElse(None)   #This retrieves the Databricks workspace URL.  # e.g. https://adb-...azuredatabricks.net
api_token = ctx.apiToken().getOrElse(None)  # personal access token for this session.  to retrieve an authentication token associated with the notebook's current context.

print(api_url)
print(api_token)  # handle securely, do not log in real code


import requests
import json

# ----------------------------------------
# Configuration
# ----------------------------------------
DATABRICKS_INSTANCE = api_url  # Replace with your workspace URL
DATABRICKS_TOKEN = api_token  # Replace with your PAT

scope_name = secret_scope_name  # Scope to be created
backend_type = "DATABRICKS"     # Use "AZURE_KEYVAULT" if integrating with Key Vault

# COMMAND ----------

# DBTITLE 1,Create Databricks Secret Scope Using REST API Request
# This code creates a secret scope in Databricks using the REST API. A secret scope is a secure container where you can store secrets such as database passwords, API keys, and access tokens.

# ----------------------------------------
# API Endpoint
# ----------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/scopes/create"

# /api/2.0/secrets/scopes/create → Databricks REST API endpoint for creating a secret scope.

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

# Authorization: Sends your Databricks access token to authenticate the request.
# Content-Type: Tells Databricks that the request body is JSON.


# This defines the name of the secret scope.
payload = {
    "scope": scope_name
}

# For example:
# scope_name = "my-secrets"
# The API receives:
# {
#   "scope": "my-secrets"
# }




# ----------------------------------------
# Send request
# ----------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# This sends an HTTP POST request to Databricks.
#     url → Where the request goes.
#     headers → Authentication and content type.
#     data=json.dumps(payload) → Converts the Python dictionary into JSON.




# ----------------------------------------
# Handle response
# ----------------------------------------
if response.status_code == 200:
    print(f"Secret scope '{scope_name}' created successfully.")
else:
    print("Failed to create secret scope.")
    print("Status Code:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# DBTITLE 1,Configure Kafka Connection Parameters and Credentials
kafka_bootstrap_servers = 'pkc-xrnwx.asia-south2.gcp.confluent.cloud:9092'
kafka_topic = 'credit_card-transaction'
kafka_api_key = 'CIHL6TYYZ2K7A47A'
kafka_api_secret = 'cflt4+7yh9ShdfqbcybRKEml/MOUX9iFAktWbKn8Ju8oxfBXKy2c3AnawngueYZg'

kafka_connection_details = json.dumps({
    "bootstrap_servers": kafka_bootstrap_servers,
    "topic": kafka_topic,
    "api_key": kafka_api_key,
    "api_secret": kafka_api_secret
})

# COMMAND ----------

# DBTITLE 1,Display Current Kafka Connection Configuration
print(kafka_connection_details)

# COMMAND ----------

# DBTITLE 1,Upload Kafka Connection Secret to Databricks Scope via  ...
import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key='kafka_connection_details'
secret_value=kafka_connection_details

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)


# COMMAND ----------

# DBTITLE 1,Verify Retrieval and Parsing of Kafka Connection Secret
# =====================================================
# 5️⃣ Verify Secret Retrieval
# =====================================================

try:
    retrieved_json = dbutils.secrets.get(
        scope=secret_scope_name,
        key='kafka_connection_details'
    )
    
    print("Secret retrieved successfully.")
    
    parsed = json.loads(retrieved_json)
    print("Parsed JSON:")
    print(parsed)
    
except Exception as e:
    print("Secret verification failed:")
    print(str(e))


# COMMAND ----------

import requests
import json

scope = secret_scope_name          # Already existing scope
secret_key = 'gmail_api_key'       # Name of the secret entry
secret_value = 'lyph ulba qfon ftay' # Value to store securely

# -------------------------------------------------
# API Endpoint
# -------------------------------------------------
url = f"{DATABRICKS_INSTANCE}/api/2.0/secrets/put"

headers = {
    "Authorization": f"Bearer {DATABRICKS_TOKEN}",
    "Content-Type": "application/json"
}

payload = {
    "scope": scope,
    "key": secret_key,
    "string_value": secret_value
}

# -------------------------------------------------
# Send Request
# -------------------------------------------------
response = requests.post(url, headers=headers, data=json.dumps(payload))

# -------------------------------------------------
# Output
# -------------------------------------------------
if response.status_code == 200:
    print(f"Secret '{secret_key}' created successfully in scope '{scope}'.")
else:
    print("Failed to create secret.")
    print("Status:", response.status_code)
    print("Response:", response.text)