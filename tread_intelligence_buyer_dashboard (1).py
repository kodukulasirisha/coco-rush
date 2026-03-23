import streamlit as st
import pandas as pd
from datetime import datetime

from snowflake.snowpark.context import get_active_session
session = get_active_session()

st.set_page_config(page_title="Tread Intelligence", page_icon="🚛", layout="wide")
st.title("🚛 Tread Intelligence")
st.caption("Intelligent Procurement Dashboard for Tireco")

@st.cache_data(ttl=300)
def get_po_recommendations():
    df = session.sql("""
        SELECT 
            r.RECOMMENDATION_ID,
            r.SKU_CODE,
            r.SKU_NAME,
            p.BRAND_NAME,
            r.DC_CODE,
            r.DC_NAME,
            r.VENDOR_NAME,
            r.RECOMMENDED_ORDER_QTY as RECOMMENDED_QTY,
            COALESCE(pr.UNIT_COST, 75.00) as UNIT_COST,
            r.ESTIMATED_COST as ESTIMATED_TOTAL_COST,
            r.CURRENT_INVENTORY_QTY as CURRENT_INVENTORY,
            r.CURRENT_DAYS_OF_SUPPLY as DAYS_OF_SUPPLY,
            r.FORECAST_DEMAND_28D as FORECASTED_DEMAND_28D,
            r.URGENCY_LEVEL,
            r.PRIORITY_SCORE,
            r.JUSTIFICATION_TEXT,
            r.RISK_FACTORS,
            r.RECOMMENDATION_STATUS as STATUS,
            r.GENERATED_AT as RECOMMENDATION_DATE,
            l.REGION,
            l.CLIMATE_ZONE
        FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS r
        LEFT JOIN TIRECO_DW.SILVER.DIM_PRODUCT p ON r.SKU_ID = p.SKU_ID
        LEFT JOIN TIRECO_DW.SILVER.DIM_LOCATION l ON r.DC_ID = l.LOCATION_ID
        LEFT JOIN TIRECO_DW.BRONZE.RAW_PRICE_LIST pr ON r.SKU_ID = pr.SKU_ID
        WHERE r.RECOMMENDATION_STATUS IN ('DRAFT', 'PENDING_REVIEW')
        ORDER BY r.PRIORITY_SCORE DESC
    """).to_pandas()
    return df

def update_recommendation_status(rec_id, new_status, notes=None, modified_qty=None):
    if modified_qty:
        session.sql(f"""
            UPDATE TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
            SET RECOMMENDATION_STATUS = '{new_status}',
                RECOMMENDED_ORDER_QTY = {modified_qty},
                REVIEWER_NOTES = '{notes or ""}',
                REVIEWED_AT = CURRENT_TIMESTAMP()
            WHERE RECOMMENDATION_ID = {rec_id}
        """).collect()
    else:
        session.sql(f"""
            UPDATE TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
            SET RECOMMENDATION_STATUS = '{new_status}',
                REVIEWER_NOTES = '{notes or ""}',
                REVIEWED_AT = CURRENT_TIMESTAMP()
            WHERE RECOMMENDATION_ID = {rec_id}
        """).collect()

if "actions_taken" not in st.session_state:
    st.session_state.actions_taken = {}
if "selected_recs" not in st.session_state:
    st.session_state.selected_recs = set()

df = get_po_recommendations()

col1, col2, col3, col4 = st.columns(4)
total_recs = len(df)
critical = len(df[df["URGENCY_LEVEL"] == "CRITICAL"]) if "URGENCY_LEVEL" in df.columns else 0
high = len(df[df["URGENCY_LEVEL"] == "HIGH"]) if "URGENCY_LEVEL" in df.columns else 0
total_spend = df["ESTIMATED_TOTAL_COST"].sum() if "ESTIMATED_TOTAL_COST" in df.columns and len(df) > 0 else 0

col1.metric("Total Recommendations", total_recs)
col2.metric("🔴 Critical", critical)
col3.metric("🟠 High Priority", high)
col4.metric("Est. Total Spend", f"${total_spend:,.0f}")

st.divider()

col_filter1, col_filter2, col_filter3, col_filter4 = st.columns(4)
with col_filter1:
    urgency_filter = st.multiselect(
        "Urgency Level",
        options=["CRITICAL", "HIGH", "MEDIUM", "LOW"],
        default=["CRITICAL", "HIGH"]
    )
with col_filter2:
    dc_options = df["DC_NAME"].unique().tolist() if "DC_NAME" in df.columns and len(df) > 0 else []
    dc_filter = st.multiselect("Distribution Center", options=dc_options)
with col_filter3:
    brand_options = df["BRAND_NAME"].dropna().unique().tolist() if "BRAND_NAME" in df.columns and len(df) > 0 else []
    brand_filter = st.multiselect("Brand", options=brand_options)
with col_filter4:
    region_options = df["REGION"].dropna().unique().tolist() if "REGION" in df.columns and len(df) > 0 else []
    region_filter = st.multiselect("Region", options=region_options)

filtered_df = df.copy()
if urgency_filter and "URGENCY_LEVEL" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["URGENCY_LEVEL"].isin(urgency_filter)]
if dc_filter and "DC_NAME" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["DC_NAME"].isin(dc_filter)]
if brand_filter and "BRAND_NAME" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["BRAND_NAME"].isin(brand_filter)]
if region_filter and "REGION" in filtered_df.columns:
    filtered_df = filtered_df[filtered_df["REGION"].isin(region_filter)]

if "PRIORITY_SCORE" in filtered_df.columns:
    filtered_df = filtered_df.sort_values("PRIORITY_SCORE", ascending=False)

st.subheader(f"Purchase Order Recommendations ({len(filtered_df)})")

def get_urgency_color(urgency):
    colors = {"CRITICAL": "red", "HIGH": "orange", "MEDIUM": "blue", "LOW": "green"}
    return colors.get(urgency, "gray")

for idx, row in filtered_df.iterrows():
    rec_id = row["RECOMMENDATION_ID"]
    is_actioned = rec_id in st.session_state.actions_taken
    
    with st.container(border=True):
        header_col1, header_col2, header_col3 = st.columns([3, 1, 1])
        
        with header_col1:
            st.markdown(f"**{row.get('SKU_NAME', 'Unknown SKU')}**")
            st.caption(f"{row.get('SKU_CODE', '')} | {row.get('DC_NAME', '')} | {row.get('REGION', '')}")
        
        with header_col2:
            urgency = row.get("URGENCY_LEVEL", "MEDIUM")
            urgency_color = get_urgency_color(urgency)
            st.markdown(f":{urgency_color}[**{urgency}**]")
        
        with header_col3:
            priority = row.get("PRIORITY_SCORE", 0) or 0
            st.metric("Priority", f"{priority:.0f}/100")
        
        detail_col1, detail_col2, detail_col3, detail_col4 = st.columns(4)
        rec_qty = row.get("RECOMMENDED_QTY", 0) or 0
        curr_inv = row.get("CURRENT_INVENTORY", 0) or 0
        dos = row.get("DAYS_OF_SUPPLY", 0) or 0
        est_cost = row.get("ESTIMATED_TOTAL_COST", 0) or 0
        
        detail_col1.metric("Recommended Qty", f"{int(rec_qty):,}")
        detail_col2.metric("Current Inventory", f"{int(curr_inv):,}")
        detail_col3.metric("Days of Supply", f"{dos:.1f}")
        detail_col4.metric("Est. Cost", f"${est_cost:,.0f}")
        
        with st.expander("View AI Justification"):
            justification = row.get("JUSTIFICATION_TEXT", "No justification available")
            st.info(justification if justification else "No justification available")
            
            risk_factors = row.get("RISK_FACTORS")
            if risk_factors:
                st.markdown("**Risk Factors:**")
                st.json(risk_factors)
        
        if is_actioned:
            action = st.session_state.actions_taken[rec_id]
            st.success(f"✅ Action taken: {action}")
        else:
            action_col1, action_col2, action_col3, action_col4 = st.columns(4)
            
            with action_col1:
                if st.button("✓ Approve", key=f"approve_{rec_id}", type="primary"):
                    update_recommendation_status(rec_id, "APPROVED")
                    st.session_state.actions_taken[rec_id] = "APPROVED"
                    st.cache_data.clear()
                    st.rerun()
            
            with action_col2:
                if st.button("✎ Modify", key=f"modify_{rec_id}"):
                    st.session_state[f"show_modify_{rec_id}"] = True
            
            with action_col3:
                if st.button("✗ Reject", key=f"reject_{rec_id}"):
                    update_recommendation_status(rec_id, "REJECTED")
                    st.session_state.actions_taken[rec_id] = "REJECTED"
                    st.cache_data.clear()
                    st.rerun()
            
            with action_col4:
                if st.checkbox("Batch", key=f"batch_{rec_id}"):
                    st.session_state.selected_recs.add(rec_id)
                else:
                    st.session_state.selected_recs.discard(rec_id)
            
            if st.session_state.get(f"show_modify_{rec_id}", False):
                new_qty = st.number_input(
                    "Modified Quantity",
                    min_value=1,
                    value=int(rec_qty),
                    key=f"qty_{rec_id}"
                )
                notes = st.text_input("Notes", key=f"notes_{rec_id}")
                if st.button("Confirm", key=f"confirm_{rec_id}"):
                    update_recommendation_status(rec_id, "MODIFIED", notes, new_qty)
                    st.session_state.actions_taken[rec_id] = f"MODIFIED (Qty: {new_qty})"
                    st.session_state[f"show_modify_{rec_id}"] = False
                    st.cache_data.clear()
                    st.rerun()

with st.sidebar:
    st.header("Batch Actions")
    
    batch_count = len(st.session_state.selected_recs)
    st.metric("Selected for Batch", batch_count)
    
    if batch_count > 0:
        selected_df = filtered_df[filtered_df["RECOMMENDATION_ID"].isin(st.session_state.selected_recs)]
        batch_total = selected_df["ESTIMATED_TOTAL_COST"].sum() if len(selected_df) > 0 else 0
        st.metric("Batch Total", f"${batch_total:,.0f}")
        
        if st.button("✓ Approve All Selected", type="primary"):
            for rec_id in list(st.session_state.selected_recs):
                update_recommendation_status(rec_id, "APPROVED", "Batch approved")
                st.session_state.actions_taken[rec_id] = "APPROVED (Batch)"
            st.session_state.selected_recs = set()
            st.cache_data.clear()
            st.rerun()
        
        if st.button("✗ Reject All Selected"):
            for rec_id in list(st.session_state.selected_recs):
                update_recommendation_status(rec_id, "REJECTED", "Batch rejected")
                st.session_state.actions_taken[rec_id] = "REJECTED (Batch)"
            st.session_state.selected_recs = set()
            st.cache_data.clear()
            st.rerun()
        
        if st.button("Clear Selection"):
            st.session_state.selected_recs = set()
            st.rerun()
    
    st.divider()
    st.header("Quick Stats")
    
    if len(df) > 0 and "URGENCY_LEVEL" in df.columns:
        urgency_summary = df.groupby("URGENCY_LEVEL").size().reset_index(name="count")
        st.bar_chart(urgency_summary, x="URGENCY_LEVEL", y="count")
    
    if len(df) > 0 and "DC_NAME" in df.columns:
        st.caption("Spend by DC")
        dc_summary = df.groupby("DC_NAME")["ESTIMATED_TOTAL_COST"].sum().reset_index()
        dc_summary.columns = ["DC", "Total Cost"]
        st.dataframe(dc_summary, hide_index=True, use_container_width=True)

st.divider()
st.caption(f"Last refreshed: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')} | Tread Intelligence v1.0")
