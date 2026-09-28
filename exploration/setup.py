# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %sql
# MAGIC CREATE CATALOG IF NOT EXISTS exmox;
# MAGIC USE CATALOG exmox;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE SCHEMA IF NOT EXISTS exmox.bronze;
# MAGIC CREATE SCHEMA IF NOT EXISTS exmox.silver;
# MAGIC CREATE SCHEMA IF NOT EXISTS exmox.gold;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE VOLUME IF NOT EXISTS exmox.bronze.landing;
# MAGIC CREATE VOLUME IF NOT EXISTS exmox.bronze._checkpoints;
# MAGIC