---
title: "Naming Conventions"
---

# Naming Conventions

This document outlines the naming conventions used for schemas, tables, views, columns, and other objects in the data warehouse.

## Table of Contents
- General Principles
- Table Naming Conventions
  - Bronze Rules
  - Silver Rules
  - Gold Rules
- Column Naming Conventions
  - Surrogate Keys
  - Technical Columns
- Stored Procedure Naming

## General Principles
- **Naming Conventions:** Use `snake_case`, with lowercase letters and underscores (`_`) to separate words.
- **Language:** Use English for all names.
- **Avoid Reserved Words:** Do not use SQL reserved words as object names.

## Table Naming Conventions
### Bronze Rules
- All names must start with the source system name, and table names must match their original names without renaming.
- **Format:** `<sourcesystem>_<entity>`
- **Example:** `crm_customer_info` → Customer information from the CRM system.

### Silver Rules
- Follow the same rules as Bronze.

### Gold Rules
- Use meaningful, business-aligned names for tables, starting with the category prefix.
- **Format:** `<category>_<entity>`
  - `dim_` for dimension tables (e.g., `dim_customers`)
  - `fact_` for fact tables (e.g., `fact_sales`)

## Column Naming Conventions
### Surrogate Keys
- **Format:** `<table_name>_key`
- **Example:** `customer_key` → Surrogate key in the `dim_customers` table.

### Technical Columns
- **Format:** `dwh_<column_name>`
- **Example:** `dwh_load_date` → System-generated column storing the record load date.

## Stored Procedure Naming
- **Format:** `load_<layer>`
- **Example:** `load_bronze` → Procedure for loading data into the Bronze layer.

