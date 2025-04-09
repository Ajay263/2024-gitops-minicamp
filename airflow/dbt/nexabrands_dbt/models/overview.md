{% docs __overview__ %}
# Nexabrands Supply Chain Analytics Pipeline

Welcome to the Nexabrands Supply Chain Analytics documentation! This pipeline is designed to track and analyze delivery performance metrics for Nexabrands across its operational cities in South Africa.

## Project Overview

Nexabrands is a growing FMCG manufacturer headquartered in Midrand, South Africa. This pipeline helps analyze and improve delivery service levels before planned expansion to additional metros/Tier 1 cities.

## Key Metrics Tracked

- **On-Time Delivery (OT)%**: Percentage of orders delivered on schedule
- **In-Full Delivery (IF)%**: Percentage of orders delivered with complete quantities
- **On-Time In-Full (OTIF)%**: Percentage of orders delivered both on time and in full
- **Customer-specific Service Level Targets**: Performance against agreed SLAs

## Data Schema

Our pipeline processes data from the following sources:

![Nexabrands Supply Chain Data Schema](https://nexabrands-prod-target.s3.amazonaws.com/dbt-docs/assets/nexabrands_schema.png)

## Dashboard & Insights

The analytics pipeline powers a comprehensive dashboard that enables:

- Daily tracking of OT, IF, and OTIF metrics
- City-wise delivery performance analysis
- Customer-specific service level monitoring
- Trend analysis for proactive issue identification
- Performance comparison against targets

## Pipeline Structure



## Stakeholder Access

This pipeline serves multiple stakeholders:
- **Supply Chain Team**: Daily operational monitoring
- **Account Management**: Customer-specific performance tracking
- **Executive Management**: Strategic overview and expansion planning

## Expansion Readiness Analysis

The pipeline includes specialized models to evaluate delivery performance scalability and identify potential bottlenecks before expansion to new cities.

## Getting Started

Refer to the model documentation below to understand our transformation logic and query examples.
{% enddocs %}
