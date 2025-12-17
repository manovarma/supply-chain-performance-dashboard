import pandas as pd
import matplotlib.pyplot as plt
from pathlib import Path

BASE = Path("/Users/mano/Projects/dataco_supply_chain")
OUTPUTS = BASE / "outputs"
REPORTS = BASE / "reports"
REPORTS.mkdir(exist_ok=True)

# Load KPI tables
kpi = pd.read_csv(OUTPUTS / "kpi_summary.csv")
profit_cat = pd.read_csv(OUTPUTS / "profit_by_category.csv")
late_region = pd.read_csv(OUTPUTS / "late_by_region.csv")
ship_mode = pd.read_csv(OUTPUTS / "shipping_mode_performance.csv")
loss_prod = pd.read_csv(OUTPUTS / "loss_making_products.csv")

# 1) Top categories by profit
top_cat = profit_cat.head(10).sort_values("total_profit", ascending=True)
plt.figure()
plt.barh(top_cat["Category_Name"], top_cat["total_profit"])
plt.title("Top 10 Categories by Total Profit")
plt.xlabel("Total Profit")
plt.tight_layout()
plt.savefig(REPORTS / "top_categories_profit.png", dpi=200)

# 2) Late delivery rate by region (top 15)
top_late = late_region.head(15).sort_values("late_delivery_rate", ascending=True)
labels = top_late["Market"].astype(str) + " | " + top_late["Order_Region"].astype(str)
plt.figure()
plt.barh(labels, top_late["late_delivery_rate"])
plt.title("Top 15 Market/Regions by Late Delivery Rate")
plt.xlabel("Late Delivery Rate")
plt.tight_layout()
plt.savefig(REPORTS / "late_delivery_by_region.png", dpi=200)

# 3) Shipping mode performance (late rate)
ship_sorted = ship_mode.sort_values("late_delivery_rate", ascending=True)
plt.figure()
plt.barh(ship_sorted["Shipping_Mode"], ship_sorted["late_delivery_rate"])
plt.title("Late Delivery Rate by Shipping Mode")
plt.xlabel("Late Delivery Rate")
plt.tight_layout()
plt.savefig(REPORTS / "late_delivery_by_shipping_mode.png", dpi=200)

# 4) Bottom 15 products by profit (loss makers)
bottom_loss = loss_prod.head(15).sort_values("total_profit", ascending=True)
plt.figure()
plt.barh(bottom_loss["Product_Name"], bottom_loss["total_profit"])
plt.title("Bottom 15 Products by Total Profit")
plt.xlabel("Total Profit")
plt.tight_layout()
plt.savefig(REPORTS / "bottom_products_profit.png", dpi=200)

# Export loss-making table as a pretty CSV for README/dashboard screenshots
loss_prod.to_csv(REPORTS / "loss_making_products_table.csv", index=False)

print("Charts saved to:", REPORTS)
print("Done ✅")
