from pathlib import Path
import pandas as pd
import duckdb

PROJECT_DIR = Path(__file__).resolve().parent
RAW_DIR = PROJECT_DIR / "data" / "raw"
PROCESSED_DIR = PROJECT_DIR / "data" / "processed"
OUTPUTS_DIR = PROJECT_DIR / "outputs"

PROCESSED_DIR.mkdir(exist_ok=True, parents=True)
OUTPUTS_DIR.mkdir(exist_ok=True, parents=True)


def main():
    csv_path = RAW_DIR / "DataCoSupplyChainDataset.csv"
    print("Reading:", csv_path)

    # Robust CSV reader (handles Kaggle encoding issues)
    encodings = ["utf-8", "cp1252", "latin1"]
    df = None
    for enc in encodings:
        try:
            df = pd.read_csv(
                csv_path,
                low_memory=False,
                encoding=enc,
                encoding_errors="replace"
            )
            print(f"Loaded using encoding: {enc}")
            break
        except UnicodeDecodeError:
            continue

    if df is None:
        raise RuntimeError("Failed to read CSV with tried encodings.")

    print("Raw shape:", df.shape)

    # Normalize column names to BI/SQL friendly format
    df.columns = (
        df.columns.str.strip()
        .str.replace(" ", "_")
        .str.replace("-", "_")
        .str.replace("/", "_")
        .str.replace("(", "")
        .str.replace(")", "")
    )

    # ---- Feature engineering ----
    df["delivery_delay_days"] = df["Days_for_shipping_real"] - df["Days_for_shipment_scheduled"]
    df["late_delivery_flag"] = (df["delivery_delay_days"] > 0).astype(int)

    df["profit_margin"] = df["Benefit_per_order"] / df["Sales"].replace(0, pd.NA)
    df["loss_making_order_flag"] = (df["Benefit_per_order"] < 0).astype(int)

    # ---- Clean dataset for dashboard/portfolio ----
    keep_cols = [
        "Order_Id",
        "order_date_DateOrders",
        "Market",
        "Order_Region",
        "Order_Country",
        "Order_City",
        "Customer_Segment",
        "Category_Name",
        "Product_Name",
        "Sales",
        "Benefit_per_order",
        "profit_margin",
        "Order_Item_Discount",
        "Order_Item_Discount_Rate",
        "Order_Item_Quantity",
        "Shipping_Mode",
        "Delivery_Status",
        "Late_delivery_risk",
        "Days_for_shipping_real",
        "Days_for_shipment_scheduled",
        "delivery_delay_days",
        "late_delivery_flag",
        "loss_making_order_flag",
        "Type",
    ]

    # Keep only those that exist (safety)
    keep_cols = [c for c in keep_cols if c in df.columns]

    df_clean = df[keep_cols].copy()
    print("Clean shape:", df_clean.shape)

    # Save cleaned file (dashboard-ready)
    clean_csv = PROCESSED_DIR / "dataco_clean.csv"
    clean_parquet = PROCESSED_DIR / "dataco_clean.parquet"
    df_clean.to_csv(clean_csv, index=False)
    df_clean.to_parquet(clean_parquet, index=False)

    print("Saved:", clean_csv)
    print("Saved:", clean_parquet)

    # ---- DuckDB KPI tables ----
    con = duckdb.connect(str(OUTPUTS_DIR / "dataco.duckdb"))
    con.execute("CREATE OR REPLACE TABLE dataco AS SELECT * FROM read_parquet(?)", [str(clean_parquet)])

    # 1) KPI Summary
    con.execute("""
        CREATE OR REPLACE TABLE kpi_summary AS
        SELECT
            COUNT(*) AS total_rows,
            COUNT(DISTINCT Order_Id) AS distinct_orders,
            SUM(Sales) AS total_sales,
            SUM(Benefit_per_order) AS total_profit,
            AVG(profit_margin) AS avg_profit_margin,
            AVG(late_delivery_flag) AS late_delivery_rate,
            AVG(delivery_delay_days) AS avg_delivery_delay_days
        FROM dataco
    """)

    # 2) Profit by Category
    con.execute("""
        CREATE OR REPLACE TABLE profit_by_category AS
        SELECT
            Category_Name,
            COUNT(DISTINCT Order_Id) AS orders,
            SUM(Sales) AS total_sales,
            SUM(Benefit_per_order) AS total_profit,
            AVG(profit_margin) AS avg_profit_margin,
            AVG(late_delivery_flag) AS late_delivery_rate
        FROM dataco
        GROUP BY 1
        ORDER BY total_profit DESC
    """)

    # 3) Late Delivery by Market/Region
    con.execute("""
        CREATE OR REPLACE TABLE late_by_region AS
        SELECT
            Market,
            Order_Region,
            COUNT(DISTINCT Order_Id) AS orders,
            AVG(late_delivery_flag) AS late_delivery_rate,
            AVG(delivery_delay_days) AS avg_delay_days
        FROM dataco
        GROUP BY 1,2
        ORDER BY late_delivery_rate DESC
    """)

    # 4) Shipping mode performance
    con.execute("""
        CREATE OR REPLACE TABLE shipping_mode_performance AS
        SELECT
            Shipping_Mode,
            COUNT(DISTINCT Order_Id) AS orders,
            AVG(late_delivery_flag) AS late_delivery_rate,
            AVG(delivery_delay_days) AS avg_delay_days,
            SUM(Sales) AS total_sales,
            SUM(Benefit_per_order) AS total_profit,
            AVG(profit_margin) AS avg_profit_margin
        FROM dataco
        GROUP BY 1
        ORDER BY late_delivery_rate DESC
    """)

    # 5) Loss-making products
    con.execute("""
        CREATE OR REPLACE TABLE loss_making_products AS
        SELECT
            Product_Name,
            Category_Name,
            COUNT(DISTINCT Order_Id) AS orders,
            SUM(Sales) AS total_sales,
            SUM(Benefit_per_order) AS total_profit,
            AVG(profit_margin) AS avg_profit_margin
        FROM dataco
        GROUP BY 1,2
        ORDER BY total_profit ASC
        LIMIT 25
    """)

    # Export tables to CSV for dashboard/README
    tables = [
        "kpi_summary",
        "profit_by_category",
        "late_by_region",
        "shipping_mode_performance",
        "loss_making_products",
    ]

    for t in tables:
        out_path = OUTPUTS_DIR / f"{t}.csv"
        con.execute(f"COPY {t} TO '{out_path}' (HEADER, DELIMITER ',')")
        print("Exported:", out_path)

    con.close()
    print("DONE ✅")


if __name__ == "__main__":
    main()
