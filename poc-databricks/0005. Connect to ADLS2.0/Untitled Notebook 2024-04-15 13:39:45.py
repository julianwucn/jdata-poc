# Databricks notebook source
service_credential = dbutils.secrets.get(scope="jdata_secrete_scope",key="jdata-clientsecret")

spark.conf.set("fs.azure.account.auth.type.datalakeliuyu.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.datalakeliuyu.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.datalakeliuyu.dfs.core.windows.net", "fcdd8a20-932b-44b7-9f33-b3cfd93e758a")
spark.conf.set("fs.azure.account.oauth2.client.secret.datalakeliuyu.dfs.core.windows.net", service_credential)
spark.conf.set("fs.azure.account.oauth2.client.endpoint.datalakeliuyu.dfs.core.windows.net", "https://login.microsoftonline.com/8aeb13c9-d3e4-4235-ba0f-22cc0fdbd016/oauth2/token")
