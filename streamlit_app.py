import streamlit as st
import altair as alt
import pandas as pd
from snowflake.snowpark.context import get_active_session
from datetime import datetime, timedelta

session = get_active_session()
st.set_page_config(layout="wide")

st.title(":material/tire_repair: Tread Intelligence")
st.caption("Intelligent Inventory Forecasting & Procurement for Tireco")

# ─── sidebar filters ───
st.sidebar.header("Global Filters")

regions = session.sql(
    "SELECT DISTINCT REGION FROM TIRECO_DW.SILVER.DIM_LOCATION WHERE IS_CURRENT = TRUE ORDER BY REGION"
).to_pandas()["REGION"].tolist()
sel_regions = st.sidebar.multiselect("Region", regions, default=regions)

urgency_options = ["CRITICAL", "HIGH", "MEDIUM", "LOW"]
sel_urgency = st.sidebar.multiselect("Urgency Level", urgency_options, default=urgency_options)

region_filter = ", ".join([f"'{r}'" for r in sel_regions]) if sel_regions else "''"
urgency_filter = ", ".join([f"'{u}'" for u in sel_urgency]) if sel_urgency else "''"

# ─── tabs ───
tab_po_hist, tab_recs, tab_actions, tab_inv, tab_sales, tab_forecast = st.tabs([
    ":material/history: PO History",
    ":material/recommend: Recommendations",
    ":material/checklist: Actions Log",
    ":material/inventory_2: Inventory",
    ":material/trending_up: Sales",
    ":material/query_stats: Forecast",
])

# ═══════════════════════════════════════════════════════════════
# TAB 1 – Historical Purchase Order Snapshot
# ═══════════════════════════════════════════════════════════════
with tab_po_hist:
    st.subheader("Historical Purchase Order Snapshot")

    @st.cache_data(ttl=600)
    def load_po_history():
        return session.sql("""
            SELECT
                d.DATE_ACTUAL AS ORDER_DATE,
                d.MONTH_NAME,
                d.YEAR_ACTUAL,
                d.QUARTER_ACTUAL,
                p.BRAND_NAME,
                p.CATEGORY_L1,
                p.CATEGORY_L2,
                l.DC_NAME,
                l.CITY,
                l.STATE,
                l.REGION,
                v.VENDOR_NAME,
                f.ORDERED_QTY,
                f.RECEIVED_QTY,
                f.EXTENDED_COST,
                f.LINE_STATUS,
                f.LEAD_TIME_ACTUAL_DAYS
            FROM TIRECO_DW.SILVER.FACT_PURCHASE_ORDERS f
            JOIN TIRECO_DW.SILVER.DIM_DATE d ON f.ORDER_DATE_SK = d.DATE_SK
            JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON f.PRODUCT_SK = p.PRODUCT_SK
            JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON f.LOCATION_SK = l.LOCATION_SK
            JOIN TIRECO_DW.SILVER.DIM_VENDOR v ON f.VENDOR_SK = v.VENDOR_SK
        """).to_pandas()

    po_df = load_po_history()
    po_filtered = po_df[po_df["REGION"].isin(sel_regions)] if sel_regions else po_df

    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Total POs", f"{len(po_filtered):,}")
    c2.metric("Total Spend", f"${po_filtered['EXTENDED_COST'].sum():,.0f}")
    c3.metric("Avg Lead Time", f"{po_filtered['LEAD_TIME_ACTUAL_DAYS'].mean():.0f} days")
    c4.metric("Unique SKUs", f"{po_filtered['BRAND_NAME'].nunique()}")

    col_a, col_b = st.columns(2)
    with col_a:
        st.caption("PO Spend Over Time")
        spend_by_month = (
            po_filtered.groupby(["ORDER_DATE"])
            .agg(TOTAL_SPEND=("EXTENDED_COST", "sum"), TOTAL_QTY=("ORDERED_QTY", "sum"))
            .reset_index()
            .sort_values("ORDER_DATE")
        )
        spend_by_month["ROLLING_AVG"] = spend_by_month["TOTAL_SPEND"].rolling(7, min_periods=1).mean()
        chart = (
            alt.Chart(spend_by_month)
            .mark_area(opacity=0.3, color="#4FC3F7")
            .encode(
                x=alt.X("ORDER_DATE:T", title="Date"),
                y=alt.Y("TOTAL_SPEND:Q", title="Spend ($)"),
            )
        )
        line = (
            alt.Chart(spend_by_month)
            .mark_line(color="#0277BD", strokeWidth=2)
            .encode(x="ORDER_DATE:T", y="ROLLING_AVG:Q")
        )
        st.altair_chart(chart + line, use_container_width=True)

    with col_b:
        st.caption("PO Spend by Region")
        region_spend = po_filtered.groupby("REGION")["EXTENDED_COST"].sum().reset_index()
        st.bar_chart(region_spend, x="REGION", y="EXTENDED_COST", horizontal=True)

    col_c, col_d = st.columns(2)
    with col_c:
        st.caption("PO Volume by Vendor")
        vendor_vol = po_filtered.groupby("VENDOR_NAME")["ORDERED_QTY"].sum().reset_index()
        st.bar_chart(vendor_vol, x="VENDOR_NAME", y="ORDERED_QTY")

    with col_d:
        st.caption("Lead Time Distribution by Region")
        lt_chart = (
            alt.Chart(po_filtered)
            .mark_boxplot()
            .encode(
                x=alt.X("REGION:N", title="Region"),
                y=alt.Y("LEAD_TIME_ACTUAL_DAYS:Q", title="Lead Time (days)"),
                color="REGION:N",
            )
            .properties(height=300)
        )
        st.altair_chart(lt_chart, use_container_width=True)

    with st.expander("PO Detail Table"):
        st.dataframe(
            po_filtered.sort_values("ORDER_DATE", ascending=False).head(500),
            hide_index=True,
            use_container_width=True,
        )

# ═══════════════════════════════════════════════════════════════
# TAB 2 – PO Recommendations
# ═══════════════════════════════════════════════════════════════
with tab_recs:
    st.subheader("Purchase Order Recommendations")

    view_level = st.segmented_control("View", ["Daily", "Weekly"], default="Daily")

    summary = session.sql("""
        SELECT * FROM TIRECO_DW.PROCUREMENT.V_DASHBOARD_SUMMARY
    """).to_pandas()

    if not summary.empty:
        s = summary.iloc[0]
        m1, m2, m3, m4, m5 = st.columns(5)
        m1.metric("Total Recs", f"{int(s.get('TOTAL_RECOMMENDATIONS', 0)):,}")
        m2.metric(":red[Critical]", int(s.get("CRITICAL_COUNT", 0)))
        m3.metric(":orange[High]", int(s.get("HIGH_COUNT", 0)))
        m4.metric("Total Spend", f"${s.get('TOTAL_RECOMMENDED_SPEND', 0):,.0f}")
        m5.metric("DCs Affected", int(s.get("DCS_AFFECTED", 0)))

    @st.cache_data(ttl=300)
    def load_recommendations():
        return session.sql(f"""
            SELECT *
            FROM TIRECO_DW.PROCUREMENT.V_BUYER_DASHBOARD
            WHERE REGION IN ({region_filter})
              AND URGENCY_LEVEL IN ({urgency_filter})
            ORDER BY PRIORITY_SCORE DESC
        """).to_pandas()

    recs_df = load_recommendations()

    if recs_df.empty:
        st.info("No recommendations match the current filters.")
    else:
        if view_level == "Weekly":
            st.caption("Aggregated weekly view")
            recs_df["WEEK"] = pd.to_datetime(recs_df["GENERATED_AT"]).dt.isocalendar().week.astype(int)
            weekly_agg = (
                recs_df.groupby(["WEEK", "REGION", "URGENCY_LEVEL"])
                .agg(
                    TOTAL_QTY=("RECOMMENDED_ORDER_QTY", "sum"),
                    TOTAL_COST=("ESTIMATED_COST", "sum"),
                    REC_COUNT=("RECOMMENDATION_ID", "count"),
                )
                .reset_index()
            )
            st.dataframe(weekly_agg, hide_index=True, use_container_width=True)
        else:
            st.caption("Daily recommendation detail")

        col_r1, col_r2 = st.columns(2)
        with col_r1:
            st.caption("Recommendations by Urgency & Region")
            urg_chart = (
                alt.Chart(recs_df)
                .mark_bar()
                .encode(
                    x=alt.X("REGION:N", title="Region"),
                    y=alt.Y("count():Q", title="Count"),
                    color=alt.Color(
                        "URGENCY_LEVEL:N",
                        scale=alt.Scale(
                            domain=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
                            range=["#D32F2F", "#FF9800", "#FDD835", "#66BB6A"],
                        ),
                    ),
                )
                .properties(height=300)
            )
            st.altair_chart(urg_chart, use_container_width=True)

        with col_r2:
            st.caption("Spend by Brand")
            brand_spend = recs_df.groupby("BRAND_NAME")["ESTIMATED_COST"].sum().reset_index()
            st.bar_chart(brand_spend, x="BRAND_NAME", y="ESTIMATED_COST")

        st.divider()
        st.subheader("Review Recommendations")

        for _, row in recs_df.head(20).iterrows():
            with st.container(border=True):
                hdr1, hdr2, hdr3 = st.columns([3, 2, 1])
                hdr1.markdown(f"**{row['SKU_NAME']}** — {row['DC_NAME']}")
                hdr2.markdown(f":{'red' if row['URGENCY_LEVEL'] == 'CRITICAL' else 'orange' if row['URGENCY_LEVEL'] == 'HIGH' else 'blue'}-badge[{row['URGENCY_LEVEL']}]")
                hdr3.markdown(f"Score: **{row['PRIORITY_SCORE']}**")

                det1, det2, det3, det4 = st.columns(4)
                det1.metric("Order Qty", f"{int(row['RECOMMENDED_ORDER_QTY']):,}")
                det2.metric("Est. Cost", f"${row['ESTIMATED_COST']:,.0f}")
                det3.metric("Current Inv", f"{int(row['CURRENT_INVENTORY_QTY']):,}")
                det4.metric("Days of Supply", f"{row['CURRENT_DAYS_OF_SUPPLY']:.0f}")

                with st.expander("AI Justification & Impact"):
                    st.write(row["JUSTIFICATION_TEXT"])
                    st.caption("Projected Impact")
                    imp1, imp2, imp3 = st.columns(3)
                    new_inv = int(row["CURRENT_INVENTORY_QTY"]) + int(row["RECOMMENDED_ORDER_QTY"])
                    demand_7d = row.get("FORECAST_DEMAND_7D", 0)
                    new_dos = new_inv / max(demand_7d / 7, 0.01)
                    imp1.metric("Inventory After PO", f"{new_inv:,}", delta=f"+{int(row['RECOMMENDED_ORDER_QTY']):,}")
                    imp2.metric("New Days of Supply", f"{min(new_dos, 999):.0f}")
                    imp3.metric("Lead Time", f"{int(row['LEAD_TIME_DAYS'])} + {int(row['PORT_DELAY_BUFFER_DAYS'])} buffer days")

                btn1, btn2, btn3 = st.columns([1, 1, 4])
                if btn1.button(":material/check_circle: Accept", key=f"acc_{row['RECOMMENDATION_ID']}"):
                    session.sql(f"""
                        CALL TIRECO_DW.PROCUREMENT.SP_UPDATE_RECOMMENDATION_STATUS(
                            {row['RECOMMENDATION_ID']}, 'ACCEPTED', 'DASHBOARD_USER', 'Accepted via Tread Intelligence'
                        )
                    """).collect()
                    st.toast(f"Recommendation {row['RECOMMENDATION_ID']} accepted!", icon=":material/check_circle:")
                    load_recommendations.clear()
                    st.rerun()
                if btn2.button(":material/cancel: Reject", key=f"rej_{row['RECOMMENDATION_ID']}"):
                    session.sql(f"""
                        CALL TIRECO_DW.PROCUREMENT.SP_UPDATE_RECOMMENDATION_STATUS(
                            {row['RECOMMENDATION_ID']}, 'REJECTED', 'DASHBOARD_USER', 'Rejected via Tread Intelligence'
                        )
                    """).collect()
                    st.toast(f"Recommendation {row['RECOMMENDATION_ID']} rejected.", icon=":material/cancel:")
                    load_recommendations.clear()
                    st.rerun()

# ═══════════════════════════════════════════════════════════════
# TAB 3 – Recommendation Actions Log
# ═══════════════════════════════════════════════════════════════
with tab_actions:
    st.subheader("Recommendation Actions Log")

    @st.cache_data(ttl=120)
    def load_actions():
        return session.sql(f"""
            SELECT
                RECOMMENDATION_ID, SKU_NAME, DC_NAME, VENDOR_NAME,
                URGENCY_LEVEL, PRIORITY_SCORE,
                RECOMMENDATION_STATUS, REVIEWER_ID, REVIEWED_AT,
                REVIEWER_NOTES, GENERATED_AT,
                RECOMMENDED_ORDER_QTY, ESTIMATED_COST,
                REGION, BRAND_NAME
            FROM TIRECO_DW.PROCUREMENT.V_RECOMMENDATION_ACTIONS
            WHERE RECOMMENDATION_STATUS IN ('ACCEPTED', 'REJECTED')
              AND REGION IN ({region_filter})
            ORDER BY REVIEWED_AT DESC
        """).to_pandas()

    actions_df = load_actions()

    if actions_df.empty:
        st.info("No accepted or rejected recommendations yet. Use the Recommendations tab to review.")
    else:
        ac1, ac2, ac3 = st.columns(3)
        accepted = actions_df[actions_df["RECOMMENDATION_STATUS"] == "ACCEPTED"]
        rejected = actions_df[actions_df["RECOMMENDATION_STATUS"] == "REJECTED"]
        ac1.metric("Accepted", len(accepted))
        ac2.metric("Rejected", len(rejected))
        ac3.metric("Accepted Spend", f"${accepted['ESTIMATED_COST'].sum():,.0f}")

        status_chart = (
            alt.Chart(actions_df)
            .mark_bar()
            .encode(
                x=alt.X("RECOMMENDATION_STATUS:N", title="Status"),
                y=alt.Y("count():Q", title="Count"),
                color=alt.Color(
                    "RECOMMENDATION_STATUS:N",
                    scale=alt.Scale(
                        domain=["ACCEPTED", "REJECTED"],
                        range=["#43A047", "#E53935"],
                    ),
                ),
            )
            .properties(height=250)
        )
        st.altair_chart(status_chart, use_container_width=True)

        st.dataframe(
            actions_df,
            hide_index=True,
            use_container_width=True,
            column_config={
                "ESTIMATED_COST": st.column_config.NumberColumn("Est. Cost", format="$%,.0f"),
                "REVIEWED_AT": st.column_config.DatetimeColumn("Reviewed At", format="YYYY-MM-DD HH:mm"),
            },
        )

# ═══════════════════════════════════════════════════════════════
# TAB 4 – Inventory Health
# ═══════════════════════════════════════════════════════════════
with tab_inv:
    st.subheader("Current Inventory Health")

    @st.cache_data(ttl=300)
    def load_inventory_health():
        return session.sql(f"""
            SELECT *
            FROM TIRECO_DW.GOLD.INVENTORY_HEALTH_CURRENT
            WHERE REGION IN ({region_filter})
        """).to_pandas()

    inv_df = load_inventory_health()

    if not inv_df.empty:
        iv1, iv2, iv3, iv4 = st.columns(4)
        iv1.metric("Total SKU-DCs", f"{len(inv_df):,}")
        iv2.metric("Total On Hand", f"{inv_df['QTY_ON_HAND'].sum():,.0f}")
        iv3.metric("Total Value", f"${inv_df['EXTENDED_COST'].sum():,.0f}")
        overstock = len(inv_df[inv_df["INVENTORY_STATUS"] == "OVERSTOCK"])
        low = len(inv_df[inv_df["INVENTORY_STATUS"].isin(["LOW", "CRITICAL"])])
        iv4.metric("Low / Overstock", f"{low} / {overstock}")

        col_i1, col_i2 = st.columns(2)
        with col_i1:
            st.caption("Inventory Status Distribution")
            status_counts = inv_df["INVENTORY_STATUS"].value_counts().reset_index()
            status_counts.columns = ["STATUS", "COUNT"]
            status_pie = (
                alt.Chart(status_counts)
                .mark_arc(innerRadius=50)
                .encode(
                    theta="COUNT:Q",
                    color=alt.Color(
                        "STATUS:N",
                        scale=alt.Scale(
                            domain=["OVERSTOCK", "HEALTHY", "LOW", "CRITICAL"],
                            range=["#FFA726", "#66BB6A", "#FFEE58", "#EF5350"],
                        ),
                    ),
                    tooltip=["STATUS", "COUNT"],
                )
                .properties(height=300)
            )
            st.altair_chart(status_pie, use_container_width=True)

        with col_i2:
            st.caption("Qty on Hand by Region")
            region_inv = inv_df.groupby("REGION")["QTY_ON_HAND"].sum().reset_index()
            st.bar_chart(region_inv, x="REGION", y="QTY_ON_HAND")

        st.caption("Top 20 Low-Stock Items")
        low_stock = inv_df[inv_df["INVENTORY_STATUS"].isin(["LOW", "CRITICAL"])].sort_values("QTY_AVAILABLE").head(20)
        st.dataframe(
            low_stock[["SKU_NAME", "DC_NAME", "REGION", "QTY_ON_HAND", "QTY_AVAILABLE", "DAYS_OF_SUPPLY", "INVENTORY_STATUS"]],
            hide_index=True,
            use_container_width=True,
        )

        with st.expander("Full Inventory Detail"):
            st.dataframe(inv_df, hide_index=True, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# TAB 5 – Sales Analytics
# ═══════════════════════════════════════════════════════════════
with tab_sales:
    st.subheader("Sales Analytics")

    sales_view = st.segmented_control("Granularity", ["Daily", "Weekly"], default="Daily", key="sales_gran")

    if sales_view == "Daily":
        @st.cache_data(ttl=600)
        def load_daily_sales():
            return session.sql(f"""
                SELECT DATE_ACTUAL, REGION, BRAND_NAME, CATEGORY_L1, DC_NAME,
                       SUM(SALES_QTY) AS SALES_QTY,
                       SUM(SALES_REVENUE) AS SALES_REVENUE,
                       SUM(SALES_ORDERS_COUNT) AS ORDERS,
                       AVG(AVG_GROSS_MARGIN) AS AVG_MARGIN,
                       SUM(STOCKOUT_FLAG) AS STOCKOUTS
                FROM TIRECO_DW.GOLD.AGG_DEMAND_DAILY
                WHERE REGION IN ({region_filter})
                GROUP BY DATE_ACTUAL, REGION, BRAND_NAME, CATEGORY_L1, DC_NAME
                ORDER BY DATE_ACTUAL DESC
            """).to_pandas()

        sales_df = load_daily_sales()
    else:
        @st.cache_data(ttl=600)
        def load_weekly_sales():
            return session.sql(f"""
                SELECT WEEK_START_DATE, REGION, BRAND_NAME, CATEGORY_L1, DC_CODE,
                       SUM(TOTAL_SALES_QTY) AS SALES_QTY,
                       SUM(TOTAL_SALES_REVENUE) AS SALES_REVENUE,
                       SUM(STOCKOUT_DAYS) AS STOCKOUT_DAYS,
                       AVG(AVG_DAILY_SALES) AS AVG_DAILY_SALES
                FROM TIRECO_DW.GOLD.AGG_DEMAND_WEEKLY
                WHERE REGION IN ({region_filter})
                GROUP BY WEEK_START_DATE, REGION, BRAND_NAME, CATEGORY_L1, DC_CODE
                ORDER BY WEEK_START_DATE DESC
            """).to_pandas()

        sales_df = load_weekly_sales()

    if not sales_df.empty:
        s1, s2, s3 = st.columns(3)
        s1.metric("Total Revenue", f"${sales_df['SALES_REVENUE'].sum():,.0f}")
        s2.metric("Total Units Sold", f"{sales_df['SALES_QTY'].sum():,.0f}")
        if "ORDERS" in sales_df.columns:
            s3.metric("Total Orders", f"{sales_df['ORDERS'].sum():,.0f}")
        else:
            s3.metric("Stockout Days", f"{sales_df.get('STOCKOUT_DAYS', pd.Series([0])).sum():,.0f}")

        date_col = "DATE_ACTUAL" if "DATE_ACTUAL" in sales_df.columns else "WEEK_START_DATE"
        rev_trend = sales_df.groupby(date_col)["SALES_REVENUE"].sum().reset_index()

        st.caption("Revenue Trend")
        st.line_chart(rev_trend, x=date_col, y="SALES_REVENUE")

        col_s1, col_s2 = st.columns(2)
        with col_s1:
            st.caption("Revenue by Region")
            rr = sales_df.groupby("REGION")["SALES_REVENUE"].sum().reset_index()
            st.bar_chart(rr, x="REGION", y="SALES_REVENUE")

        with col_s2:
            st.caption("Revenue by Brand")
            rb = sales_df.groupby("BRAND_NAME")["SALES_REVENUE"].sum().reset_index().sort_values("SALES_REVENUE", ascending=False).head(10)
            st.bar_chart(rb, x="BRAND_NAME", y="SALES_REVENUE")

        with st.expander("Sales Detail"):
            st.dataframe(sales_df.head(500), hide_index=True, use_container_width=True)

# ═══════════════════════════════════════════════════════════════
# TAB 6 – Demand Forecast
# ═══════════════════════════════════════════════════════════════
with tab_forecast:
    st.subheader("ML Demand Forecasts")

    @st.cache_data(ttl=600)
    def load_forecasts():
        return session.sql("""
            SELECT
                f.FORECAST_DATE,
                f.PREDICTED_DEMAND_QTY,
                f.PREDICTION_LOWER_95,
                f.PREDICTION_UPPER_95,
                f.MODEL_CONFIDENCE,
                f.ANOMALY_FLAG,
                p.SKU_NAME,
                p.BRAND_NAME,
                l.DC_NAME,
                l.REGION
            FROM TIRECO_DW.ML.FORECAST_DEMAND f
            JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON f.SKU_ID = p.SKU_ID AND p.IS_CURRENT = TRUE
            JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON f.DC_ID = l.LOCATION_ID AND l.IS_CURRENT = TRUE
            ORDER BY f.FORECAST_DATE
        """).to_pandas()

    @st.cache_data(ttl=600)
    def load_historical_demand():
        return session.sql(f"""
            SELECT DATE_ACTUAL, REGION,
                   SUM(SALES_QTY) AS DEMAND_QTY
            FROM TIRECO_DW.GOLD.AGG_DEMAND_DAILY
            WHERE DATE_ACTUAL >= DATEADD(day, -30, CURRENT_DATE())
              AND REGION IN ({region_filter})
            GROUP BY DATE_ACTUAL, REGION
            ORDER BY DATE_ACTUAL
        """).to_pandas()

    @st.cache_data(ttl=600)
    def load_weather_seasonality():
        return session.sql(f"""
            SELECT SEASON,
                   AVG(WEATHER_SEVERITY_INDEX) AS AVG_WEATHER_SEVERITY,
                   AVG(WEATHER_TEMP_HIGH_F) AS AVG_TEMP_HIGH,
                   AVG(WEATHER_PRECIP_IN) AS AVG_PRECIP,
                   AVG(WEATHER_SNOW_IN) AS AVG_SNOW,
                   SUM(SALES_QTY) AS TOTAL_SALES,
                   AVG(SALES_QTY) AS AVG_DAILY_SALES,
                   SUM(STOCKOUT_FLAG) AS STOCKOUT_DAYS,
                   COUNT(DISTINCT DATE_ACTUAL) AS DAYS_COUNT
            FROM TIRECO_DW.GOLD.AGG_DEMAND_DAILY
            WHERE REGION IN ({region_filter})
            GROUP BY SEASON
            ORDER BY TOTAL_SALES DESC
        """).to_pandas()

    @st.cache_data(ttl=600)
    def load_monthly_weather_demand():
        return session.sql(f"""
            SELECT MONTH_ACTUAL,
                   DECODE(MONTH_ACTUAL,1,'Jan',2,'Feb',3,'Mar',4,'Apr',5,'May',6,'Jun',
                          7,'Jul',8,'Aug',9,'Sep',10,'Oct',11,'Nov',12,'Dec') AS MONTH_NAME,
                   AVG(WEATHER_SEVERITY_INDEX) AS AVG_WEATHER_SEVERITY,
                   AVG(WEATHER_TEMP_HIGH_F) AS AVG_TEMP,
                   SUM(SALES_QTY) AS TOTAL_SALES,
                   AVG(SALES_QTY) AS AVG_DAILY_SALES
            FROM TIRECO_DW.GOLD.AGG_DEMAND_DAILY
            WHERE REGION IN ({region_filter})
            GROUP BY MONTH_ACTUAL
            ORDER BY MONTH_ACTUAL
        """).to_pandas()

    fc_df = load_forecasts()
    hist_df = load_historical_demand()
    weather_df = load_weather_seasonality()
    monthly_df = load_monthly_weather_demand()

    if fc_df.empty:
        st.info("No forecast data available.")
    else:
        fc_filtered = fc_df[fc_df["REGION"].isin(sel_regions)] if sel_regions else fc_df

        f1, f2, f3, f4 = st.columns(4)
        f1.metric("Forecast Records", f"{len(fc_filtered):,}")
        f2.metric("Avg Confidence", f"{fc_filtered['MODEL_CONFIDENCE'].mean():.1f}%")
        total_predicted = fc_filtered["PREDICTED_DEMAND_QTY"].sum()
        f3.metric("Total Predicted Demand", f"{total_predicted:,.0f}")
        anomalies = fc_filtered["ANOMALY_FLAG"].sum() if "ANOMALY_FLAG" in fc_filtered.columns else 0
        f4.metric("Anomalies Detected", int(anomalies))

        st.caption("Historical Demand (30d) → Forecast Predictions")
        fc_agg = (
            fc_filtered.groupby("FORECAST_DATE")
            .agg(
                PREDICTED=("PREDICTED_DEMAND_QTY", "sum"),
                LOWER=("PREDICTION_LOWER_95", "sum"),
                UPPER=("PREDICTION_UPPER_95", "sum"),
            )
            .reset_index()
            .rename(columns={"FORECAST_DATE": "DATE"})
        )
        fc_agg["TYPE"] = "Forecast"

        hist_agg = hist_df.groupby("DATE_ACTUAL")["DEMAND_QTY"].sum().reset_index()
        hist_agg = hist_agg.rename(columns={"DATE_ACTUAL": "DATE", "DEMAND_QTY": "PREDICTED"})
        hist_agg["TYPE"] = "Historical"
        hist_agg["LOWER"] = hist_agg["PREDICTED"]
        hist_agg["UPPER"] = hist_agg["PREDICTED"]

        combined = pd.concat([hist_agg, fc_agg], ignore_index=True)

        line_chart = (
            alt.Chart(combined)
            .mark_line(strokeWidth=2)
            .encode(
                x=alt.X("DATE:T", title="Date"),
                y=alt.Y("PREDICTED:Q", title="Demand Qty"),
                color=alt.Color(
                    "TYPE:N",
                    scale=alt.Scale(domain=["Historical", "Forecast"], range=["#1565C0", "#FF6F00"]),
                ),
                strokeDash=alt.condition(
                    alt.datum.TYPE == "Forecast", alt.value([6, 3]), alt.value([0]),
                ),
            )
            .properties(height=350)
        )
        band = (
            alt.Chart(fc_agg)
            .mark_area(opacity=0.15, color="#FF6F00")
            .encode(x="DATE:T", y="LOWER:Q", y2="UPPER:Q")
        )
        st.altair_chart(band + line_chart, use_container_width=True)

        col_f1, col_f2 = st.columns(2)
        with col_f1:
            st.caption("Seasonal Demand & Weather Impact")
            if not weather_df.empty:
                season_chart = (
                    alt.Chart(weather_df)
                    .mark_bar(cornerRadiusTopLeft=4, cornerRadiusTopRight=4)
                    .encode(
                        x=alt.X("SEASON:N", title="Season", sort=["SPRING", "SUMMER", "FALL", "WINTER"]),
                        y=alt.Y("AVG_DAILY_SALES:Q", title="Avg Daily Sales (units)"),
                        color=alt.Color(
                            "SEASON:N",
                            scale=alt.Scale(
                                domain=["SPRING", "SUMMER", "FALL", "WINTER"],
                                range=["#66BB6A", "#FFA726", "#EF6C00", "#42A5F5"],
                            ),
                        ),
                        tooltip=["SEASON", "AVG_DAILY_SALES", "AVG_WEATHER_SEVERITY", "AVG_TEMP_HIGH", "AVG_PRECIP"],
                    )
                    .properties(height=280)
                )
                st.altair_chart(season_chart, use_container_width=True)

                for _, row in weather_df.iterrows():
                    with st.container():
                        st.markdown(
                            f"**{row['SEASON']}**: "
                            f"Avg Temp {row['AVG_TEMP_HIGH']:.0f}°F, "
                            f"Precip {row['AVG_PRECIP']:.2f}in, "
                            f"Snow {row['AVG_SNOW']:.2f}in, "
                            f"Weather Severity {row['AVG_WEATHER_SEVERITY']:.2f}, "
                            f"Stockout Days: {int(row['STOCKOUT_DAYS'])}"
                        )

        with col_f2:
            st.caption("Monthly Demand vs Temperature")
            if not monthly_df.empty:
                base = alt.Chart(monthly_df).encode(
                    x=alt.X("MONTH_NAME:N", title="Month", sort=[
                        "Jan", "Feb", "Mar", "Apr", "May", "Jun",
                        "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
                    ]),
                )
                bars = base.mark_bar(color="#42A5F5", opacity=0.7).encode(
                    y=alt.Y("AVG_DAILY_SALES:Q", title="Avg Daily Sales"),
                    tooltip=["MONTH_NAME", "AVG_DAILY_SALES", "AVG_TEMP"],
                )
                temp_line = base.mark_line(color="#EF5350", strokeWidth=2, point=True).encode(
                    y=alt.Y("AVG_TEMP:Q", title="Avg Temp (°F)", axis=alt.Axis(titleColor="#EF5350")),
                )
                st.altair_chart(
                    alt.layer(bars, temp_line).resolve_scale(y="independent").properties(height=280),
                    use_container_width=True,
                )

        st.divider()
        col_f3, col_f4 = st.columns(2)
        with col_f3:
            st.caption("Forecast Confidence Distribution")
            conf_chart = (
                alt.Chart(fc_filtered)
                .mark_bar(color="#7E57C2")
                .encode(
                    x=alt.X("MODEL_CONFIDENCE:Q", bin=alt.Bin(maxbins=20), title="Confidence Score"),
                    y=alt.Y("count():Q", title="Frequency"),
                )
                .properties(height=250)
            )
            st.altair_chart(conf_chart, use_container_width=True)

        with col_f4:
            st.caption("Predicted Demand by Region")
            region_fc = fc_filtered.groupby("REGION")["PREDICTED_DEMAND_QTY"].sum().reset_index()
            st.bar_chart(region_fc, x="REGION", y="PREDICTED_DEMAND_QTY")

        with st.expander("Forecast Detail"):
            st.dataframe(
                fc_filtered[["FORECAST_DATE", "SKU_NAME", "BRAND_NAME", "DC_NAME", "REGION",
                             "PREDICTED_DEMAND_QTY", "PREDICTION_LOWER_95", "PREDICTION_UPPER_95",
                             "MODEL_CONFIDENCE", "ANOMALY_FLAG"]].head(500),
                hide_index=True,
                use_container_width=True,
                column_config={
                    "MODEL_CONFIDENCE": st.column_config.ProgressColumn("Confidence", min_value=0, max_value=1, format="%.0f%%"),
                },
            )
