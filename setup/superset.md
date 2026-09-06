# Apache Superset BI & Dashboarding Guide

Apache Superset serves as the Business Intelligence (BI) layer, providing interactive visual dashboards on top of Trino and Apache Iceberg.

---

## 1. Initializing Superset

After spinning up the infrastructure with `make infra-up`, bootstrap Superset using the automated script:

```bash
make superset-init
```

This automates:
- Creating the default admin user (`admin` / `admin`).
- Running internal database migrations (`db upgrade`).
- Initializing standard roles and permissions (`superset init`).

Access the Superset UI at:
- **URL:** [http://localhost:8088](http://localhost:8088)
- **Username:** `admin`
- **Password:** `admin`

---

## 2. Connecting Superset to Trino

To connect Superset to the Trino distributed query engine:

1. In Superset, go to **Settings** (top right gear icon) -> **Database Connections**.
2. Click the **+ Database** button in the top right corner.
3. Select **Trino** from the database dropdown (provided by `sqlalchemy-trino`).
4. Enter the **SQLAlchemy URI**:
   ```text
   trino://admin@trino:8080/iceberg/stocks_reporting
   ```
5. Click **Test Connection**. You should see a notification confirming the connection was successful.
6. Under the **Advanced** tab -> **Security**, enable:
   - `Allow CREATE TABLE AS with DDL` (optional)
   - `Expose in SQL Lab`
7. Click **Connect** / **Finish**.

---

## 3. Querying Gold Layer Data in SQL Lab

Open **SQL Lab** -> **SQL Editor**, select the Trino database connection and schema `stocks_reporting`:

### Total Market Daily Summary
```sql
SELECT
    report_date,
    total_volume,
    total_value
FROM iceberg.stocks_reporting.daily_market_summary
ORDER BY report_date DESC;
```

### Top 10 Stocks by Trading Volume
```sql
SELECT
    stock_symbol,
    exchange,
    total_volume,
    total_value,
    transaction_count
FROM iceberg.stocks_reporting.daily_stock_summary
ORDER BY total_volume DESC
LIMIT 10;
```

### Buy vs Sell Order Distribution
```sql
SELECT
    order_type,
    order_count,
    total_volume
FROM iceberg.stocks_reporting.daily_order_type_summary;
```

---

## 4. Building the Dashboard

Create datasets from the Gold reporting tables and build visual charts:
- **Total Volume & Value Timeline:** Line chart tracking total transaction value and volume over dates.
- **Top Traded Stocks:** Horizontal bar chart comparing stock symbols.
- **Order Type Share:** Donut/Pie chart showing buy vs. sell proportions.
- **Exchange Activity:** Bar chart visualizing transaction counts across HOSE, HNX, and UPCOM.
- Group these charts into a unified **Stock Market Lakehouse Overview** dashboard.
