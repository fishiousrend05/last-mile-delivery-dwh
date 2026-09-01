# Last-Mile Delivery Data Warehouse

A portfolio Data Warehouse project for analyzing last-mile delivery operations, built with the Kimball dimensional modeling methodology.

The project integrates real e-commerce data from the Olist Brazilian E-Commerce dataset with derived and synthetic operational data, external weather data, and public-holiday reference data to build an analytical warehouse for delivery performance analysis.

**Project focus:** Data Warehouse design, dimensional modeling, ETL/ELT, historical tracking, delivery lifecycle analysis, and BI-ready data modeling.

## 1. Project Overview

Last-mile delivery is the final stage of the logistics process, where an order is delivered from a local distribution point to the customer.

The source Olist dataset provides detailed information about orders, customers, sellers, products, payments, and order status, but it does not contain operational data required for deeper last-mile analysis such as:
* Delivery drivers
* Driver assignments
* Individual delivery attempts
* Failed delivery reasons
* Delivery operational KPIs
* Weather conditions during delivery

This project extends the source dataset with derived and synthetic operational data to model a simplified last-mile delivery environment. 

The final Data Warehouse is designed to support analysis such as:
* Delivery success and failure rates
* Delivery attempt performance
* Driver performance
* SLA compliance
* Order delivery lifecycle
* Impact of weather conditions on delivery
* Operational performance by geographic zone and time

## 2. Objectives

### Technical objectives
* Design a Kimball-style dimensional Data Warehouse
* Build a Star Schema for analytical workloads
* Implement different types of fact tables:
  * Transaction fact
  * Periodic snapshot fact
  * Accumulating snapshot fact
* Implement Slowly Changing Dimension Type 2 (SCD2)
* Implement incremental loading
* Build reusable staging and transformation models with dbt
* Integrate data from multiple sources
* Apply data quality tests and integrity constraints
* Produce BI-ready analytical tables

### Domain objectives
* Model the last-mile delivery lifecycle
* Analyze delivery attempts and failures
* Track driver performance
* Measure SLA performance
* Analyze operational patterns across zones, dates, and weather conditions

## 3. Data Sources

| Source | Type | Purpose |
| :--- | :--- | :--- |
| **Olist Brazilian E-Commerce Dataset** | Real / Public | Core order, customer, seller, product, item, and payment data |
| **Faker-generated operational data** | Synthetic | Drivers, driver assignments, delivery attempts, and failed-delivery information |
| **Open-Meteo Historical Weather API** | External | Historical weather enrichment based on delivery date and location |
| **Brazil Public Holiday seed** | Reference / Static | Public-holiday information used by the date dimension |

### Data authenticity
The project intentionally separates source data from simulated operational data. Real / derived data is used as the foundation of the warehouse, while synthetic data is introduced only where the original Olist dataset does not provide the operational attributes required for last-mile delivery analysis.

Synthetic components include:
* Driver information
* Driver assignment
* Delivery attempts
* Failed delivery reasons

These components are used primarily for testing, modeling, and analytical extension, rather than representing actual Olist operational records.

## 4. Architecture

```text
                         ┌──────────────────────┐
                         │ Olist Dataset        │
                         │ CSV                  │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │ Synthetic Data       │
                         │ Faker / Python       │
                         └──────────┬───────────┘
                                    │
                         ┌──────────▼───────────┐
                         │ External Sources     │
                         │ Open-Meteo / Holiday │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │       RAW            │
                         │  Source-aligned data │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │      STAGING         │
                         │ Cleaning &           │
                         │ standardization      │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │        DWH           │
                         │   Kimball Star       │
                         │       Schema         │
                         └──────────┬───────────┘
                                    │
                                    ▼
                         ┌──────────────────────┐
                         │         BI           │
                         │ Dashboard / Analysis │
                         └──────────────────────┘
```

### Data layers

#### Raw
Contains source-aligned data with minimal transformation.
*Examples:* Olist source tables, Generated driver data, Generated delivery attempts, Weather API responses

#### Staging
Responsible for:
* Data type normalization
* Column renaming
* Cleaning
* Deduplication
* Basic business-rule transformations
* Standardizing source structures

#### DWH
Contains the final dimensional model optimized for analytical queries and BI consumption.

## 5. Dimensional Model

The warehouse follows a Kimball Star Schema with three major fact tables.

### 5.1 Fact Tables

| Fact Table | Type | Grain | Main Purpose |
| :--- | :--- | :--- | :--- |
| `fact_delivery_attempts` | Transaction Fact | One row per delivery attempt | Analyze individual delivery attempts and failures |
| `fact_driver_daily_kpi` | Periodic Snapshot | One row per driver per day | Track daily driver performance |
| `fact_order_lifecycle` | Accumulating Snapshot | One row per order | Track the order through its delivery lifecycle |

**`fact_delivery_attempts`**
The transactional fact table records individual delivery attempts. This is the main incrementally loaded fact table. 
*Example analytical metrics:* Attempt number, Successful / failed attempt, Delivery duration, Failure indicator, Driver, Zone, Weather condition.

**`fact_driver_daily_kpi`**
A periodic snapshot representing driver performance at a daily grain.
*Potential metrics include:* Orders assigned, Orders delivered, Failed deliveries, Delivery success rate, Average delivery time, SLA compliance.

**`fact_order_lifecycle`**
An accumulating snapshot that represents the lifecycle of an order. The record is progressively updated as the order moves through different milestones.
*Example milestones:* `Order Placed` → `Assigned` → `Picked Up` → `In Transit` → `Delivery Attempt` → `Delivered / Failed`
This design makes it possible to analyze durations between lifecycle milestones and evaluate SLA performance.

## 6. Dimension Tables

| Dimension | Type | Purpose |
| :--- | :--- | :--- |
| `dim_driver` | SCD Type 2 | Track historical changes in driver attributes |
| `dim_zone` | SCD Type 1 | Represent delivery zones |
| `dim_date` | Standard Dimension | Date attributes and holiday indicators |
| `dim_weather` | Standard Dimension | Weather conditions used for delivery analysis |
| `order_code` | Degenerate Dimension | Order identifier retained within fact tables |

**`dim_driver`**
Uses SCD Type 2 to preserve historical changes. For example, when a driver's assigned zone or operational status changes, a new dimension version is created instead of overwriting the previous record.
*Typical SCD2 attributes:* `driver_key`, `driver_id`, `zone`, `status`, `effective_date`, `end_date`, `is_current`. This allows historical facts to remain associated with the correct version of the driver.

**`dim_zone`**
Uses SCD Type 1. The project treats zone boundaries and attributes as relatively stable, so historical versions are not maintained.

## 7. Bridge Table

**`bridge_order_failed_reason`**
An order can potentially have multiple failed-delivery reasons across different attempts. This creates a many-to-many relationship between orders and failure reasons. The bridge table is used to resolve this relationship:

```text
fact_order_lifecycle ──► bridge_order_failed_reason ──► failed reason
```

This allows the warehouse to analyze:
* Orders affected by specific failure reasons
* Number of failure reasons per order
* Failure reason distribution
* Repeated failure patterns

## 8. ETL / ELT Strategy

The project uses **Python** for ingestion and data generation and **dbt** for transformation and warehouse modeling.

```text
Python
 ├── Load Olist data
 ├── Generate synthetic operational data
 └── Retrieve weather data
          │
          ▼
        RAW
          │
          ▼
        dbt
          │
          ├── Staging models
          ├── Dimension models
          ├── Fact models
          └── Data quality tests
          │
          ▼
        DWH
```

### Loading strategies
Different tables use different loading strategies depending on their characteristics.

| Model | Strategy | Reason |
| :--- | :--- | :--- |
| `fact_delivery_attempts` | Incremental | Transactional table with growing records |
| `fact_driver_daily_kpi` | Full refresh / periodic loading | Daily snapshot derived from operational data |
| `fact_order_lifecycle` | Incremental / update-based | Existing orders may receive new lifecycle milestones |
| Dimensions | Depends on dimension | SCD or standard dimension behavior |

*Note: The project does not force a single loading strategy across every table. Loading strategy is selected based on the table's grain, update behavior, and analytical purpose.*

## 9. Data Quality

Data quality is handled using dbt tests. Examples include:
* unique, not_null, relationships, accepted values
* Fact-to-dimension integrity
* SCD2 consistency
* Duplicate detection
* Business-rule validation

**Example:**
```text
fact_delivery_attempts
        │
        ├── driver_key ───────► dim_driver
        │
        ├── zone_key ─────────► dim_zone
        │
        ├── date_key ─────────► dim_date
        │
        └── weather_key ──────► dim_weather
```
The goal is to ensure that analytical facts remain consistent with their associated dimensions.

## 10. Key Analytical Use Cases

The warehouse is designed to support several analytical perspectives:

* **Delivery Performance:** Delivery success rate, Failed delivery rate, Average delivery duration, Number of delivery attempts, Repeat delivery attempts
* **Driver Performance:** Daily orders handled, Successful deliveries, Failed deliveries, SLA compliance, Average delivery time
* **Order Lifecycle:** Time from order placement to assignment, Assignment-to-pickup duration, Pickup-to-delivery duration, Total delivery lead time, Late delivery rate
* **Geographic Analysis:** Performance by delivery zone, Failure rate by zone, Driver workload by zone, Order volume by zone
* **Weather Analysis:** Delivery performance under different weather conditions, Failure rate by weather condition, Average delivery duration by weather

## 11. Technology Stack

| Area | Technology |
| :--- | :--- |
| **Programming** | Python |
| **Data Generation** | Faker |
| **API Ingestion** | Python / Requests |
| **Transformation** | dbt |
| **Data Warehouse** | PostgreSQL |
| **Data Modeling** | Kimball / Star Schema |
| **Version Control** | Git / GitHub |
| **BI** | Power BI |
| **Geospatial Indexing** | H3 |

## 12. Project Structure

```text
last-mile-delivery-dwh/
│
├── data/
│   ├── raw/
│   └── generated/
│
├── ingestion/
│   ├── olist/
│   ├── weather/
│   └── synthetic/
│
├── dbt/
│   ├── models/
│   │   ├── staging/
│   │   └── marts/
│   │       ├── dimensions/
│   │       └── facts/
│   │
│   ├── seeds/
│   ├── tests/
│   └── macros/
│
├── docs/
├── notebooks/
├── scripts/
└── README.md
```
*(The exact directory structure may evolve as the project develops.)*

## 13. Scope & Design Decisions

Several decisions were made intentionally to keep the project focused on Data Warehouse engineering rather than over-engineering the solution:

* **SCD2 for drivers, SCD1 for zones:** Driver attributes such as operational status or assigned zone can change over time and may affect historical analysis, making SCD2 valuable. Zone definitions are treated as relatively stable, so SCD1 is sufficient for the current project scope.
* **Static holiday reference:** Brazilian public holidays are maintained as a dbt seed rather than retrieved from an API on every pipeline execution. This keeps static reference data deterministic and reduces unnecessary external dependencies.
* **Synthetic operational data:** The original Olist dataset does not contain driver-level or delivery-attempt-level operational data. Synthetic data is therefore used to extend the dataset and demonstrate operational modeling concepts. These records should not be interpreted as actual Olist delivery records.
* **No prediction / optimization in the core scope:** The primary objective of this project is Data Warehouse engineering and analytical modeling. Prediction, simulation, and optimization are considered potential extensions rather than core requirements.

## 14. Limitations

This project is a portfolio / educational implementation rather than a production logistics platform. Key limitations include:
* Olist does not provide actual last-mile delivery attempt logs.
* Driver and delivery-attempt data are synthetic.
* Some operational timestamps are derived or simulated.
* The warehouse does not represent a real logistics company's internal systems.
* Weather data is an enrichment layer rather than an original operational source.
* Business rules are simplified for analytical modeling.

*These limitations are explicitly documented to distinguish source facts from modeled assumptions and synthetic extensions.*

## 15. Current Status

The project is being developed incrementally, with the following major milestones:

- [ ] Define business scope
- [ ] Design dimensional model
- [ ] Define fact table grains
- [ ] Define dimension strategy
- [ ] Define data sources
- [ ] Define synthetic-data scope
- [ ] Complete ingestion pipeline
- [ ] Complete staging models
- [ ] Complete dimension models
- [ ] Complete fact models
- [ ] Implement SCD2
- [ ] Implement incremental models
- [ ] Add dbt data quality tests
- [ ] Generate dbt documentation
- [ ] Build BI dashboard

## 16. Future Improvements

Potential future extensions include:
* Automated orchestration with Prefect
* More advanced data quality monitoring
* Pipeline observability
* Incremental processing for additional fact tables
* Delivery SLA monitoring dashboard
* Driver performance dashboard
* More detailed geographic analysis
* Delivery risk simulation
* Predictive modeling for SLA risk

*(These extensions are outside the core Data Warehouse scope and will only be added if they provide meaningful engineering or analytical value.)*

## 17. Author
Built as a portfolio project to demonstrate practical knowledge of:
*Data Warehousing · Dimensional Modeling · ETL/ELT · dbt · SQL · Python · Data Engineering · Business Intelligence*
