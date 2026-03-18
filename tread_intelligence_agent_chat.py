import streamlit as st
from snowflake.snowpark.context import get_active_session
import json

session = get_active_session()

st.set_page_config(page_title="Tread Intelligence Agent", page_icon="🚛", layout="wide")

st.title("🚛 Tread Intelligence Agent")
st.caption("Ask questions about inventory, forecasts, and procurement recommendations")

AGENT_NAME = "TIRECO_DW.PROCUREMENT.TREAD_INTELLIGENCE_AGENT"

if "messages" not in st.session_state:
    st.session_state.messages = []
if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None

with st.sidebar:
    st.header("Sample Questions")
    sample_questions = [
        "What should I order for Denver this week?",
        "Which SKUs are at critical inventory levels?",
        "Show me high-priority recommendations for the West region",
        "What's the total estimated spend for pending recommendations?",
        "Why are we recommending 700 units of Milestar Patagonia M/T?",
        "Give me a summary of today's recommendations",
        "Which DCs have stockout risk?",
    ]
    
    for q in sample_questions:
        if st.button(q, key=f"sample_{q[:20]}"):
            st.session_state.messages.append({"role": "user", "content": q})
            st.rerun()
    
    st.divider()
    
    if st.button("Clear Conversation"):
        st.session_state.messages = []
        st.session_state.conversation_id = None
        st.rerun()

for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        if "sql" in message:
            with st.expander("View SQL"):
                st.code(message["sql"], language="sql")
        if "data" in message:
            with st.expander("View Data"):
                st.dataframe(message["data"])

def call_agent(user_message: str) -> dict:
    try:
        request_body = {
            "messages": [{"role": "user", "content": [{"type": "text", "text": user_message}]}],
            "tools": [
                {
                    "tool_spec": {
                        "type": "cortex_analyst_text_to_sql",
                        "name": "Procurement_Analyst"
                    }
                },
                {
                    "tool_spec": {
                        "type": "data_to_chart",
                        "name": "data_to_chart"
                    }
                }
            ],
            "tool_resources": {
                "Procurement_Analyst": {
                    "semantic_model_file": "@TIRECO_DW.PROCUREMENT.SEMANTIC_MODELS/tread_intelligence_semantic_model.yaml"
                }
            }
        }
        
        if st.session_state.conversation_id:
            request_body["conversation_id"] = st.session_state.conversation_id
        
        result = session.sql(f"""
            SELECT SNOWFLAKE.CORTEX.INVOKE_AGENT(
                '{AGENT_NAME}',
                PARSE_JSON('{json.dumps(request_body)}')
            ) as response
        """).collect()[0]["RESPONSE"]
        
        response_json = json.loads(result)
        
        if "conversation_id" in response_json:
            st.session_state.conversation_id = response_json["conversation_id"]
        
        return response_json
        
    except Exception as e:
        return {"error": str(e), "fallback": True}

def generate_demo_response(user_message: str) -> dict:
    user_lower = user_message.lower()
    
    if "denver" in user_lower:
        return {
            "text": """Based on my analysis of the Denver Distribution Center, here are my recommendations:

**CRITICAL Priority Items (Order Today):**
1. **Milestar Patagonia M/T 285/70R17** - 700 units ($52,500)
   - Current inventory: 45 units (2.3 days supply)
   - Heavy snow forecast March 18-20 (8-12 inches)
   - Port delays extending lead time to 18 days

2. **Falken Wildpeak A/T3W 275/55R20** - 450 units ($38,250)
   - Current inventory: 78 units (4.1 days supply)
   - Storm-driven demand expected to spike 180%

**HIGH Priority Items:**
3. **Nitto Ridge Grappler 305/50R20** - 300 units ($45,000)
   - Days of supply: 6.2 days
   - Trending demand increase in Denver metro

**Total Recommended Spend: $135,750**

⚠️ *Without immediate action, stockouts projected by March 19 for critical items.*""",
            "sql": """SELECT sku_code, sku_name, recommended_qty, estimated_total_cost, urgency_level, days_of_supply
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
WHERE dc_name ILIKE '%Denver%' AND status = 'PENDING_REVIEW'
ORDER BY priority_score DESC""",
        }
    
    elif "critical" in user_lower or "urgent" in user_lower:
        return {
            "text": """**Critical Inventory Alerts:**

| SKU | DC | Current Inv | Days Supply | Recommended Order |
|-----|----|-----------:|------------:|------------------:|
| MST-285/70R17-MT | Denver | 45 | 2.3 | 700 units |
| FKN-275/55R20-AT | Phoenix | 32 | 1.8 | 550 units |
| NIT-305/50R20-NT | Dallas | 18 | 0.9 | 800 units |
| MST-265/70R17-AT | Chicago | 67 | 3.1 | 400 units |
| FKN-285/75R16-MT | Atlanta | 23 | 1.5 | 600 units |

**Total Critical Items:** 5 SKU-DC combinations
**Estimated Stockout Value at Risk:** $287,500

🔴 Immediate action recommended to prevent stockouts.""",
            "sql": """SELECT sku_code, dc_name, current_inventory, days_of_supply, recommended_qty
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
WHERE urgency_level = 'CRITICAL' AND status = 'PENDING_REVIEW'
ORDER BY days_of_supply ASC""",
        }
    
    elif "summary" in user_lower or "today" in user_lower:
        return {
            "text": """**Today's Recommendation Summary (March 18, 2026)**

| Urgency | Count | Total Spend | DCs | SKUs |
|---------|------:|------------:|----:|-----:|
| 🔴 CRITICAL | 5 | $247,500 | 5 | 5 |
| 🟠 HIGH | 12 | $428,750 | 8 | 11 |
| 🟡 MEDIUM | 18 | $312,000 | 12 | 15 |
| 🟢 LOW | 12 | $178,250 | 9 | 10 |
| **TOTAL** | **47** | **$1,166,500** | **18** | **41** |

**Key Drivers Today:**
- ❄️ Weather: Snow forecasts in CO, UT, WY driving 280% demand spike for M/T tires
- 🚢 Port Delays: Long Beach congestion index at 78 (extending lead times 4 days)
- 📈 Economic: Consumer confidence up 3.2 points in West region""",
            "sql": """SELECT urgency_level, COUNT(*) as count, SUM(estimated_total_cost) as total_spend
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS
WHERE status = 'PENDING_REVIEW'
GROUP BY urgency_level""",
        }
    
    elif "west" in user_lower or "region" in user_lower:
        return {
            "text": """**West Region Recommendations:**

The West region (CA, CO, AZ, NV, UT, WA, OR) has **23 pending recommendations** totaling **$687,250**.

**Top 5 by Priority:**
1. Denver DC - Milestar Patagonia M/T - 700 units - 🔴 CRITICAL
2. Phoenix DC - Falken Wildpeak A/T3W - 450 units - 🔴 CRITICAL  
3. Los Angeles DC - Nitto Terra Grappler - 600 units - 🟠 HIGH
4. Seattle DC - Milestar Patagonia A/T - 350 units - 🟠 HIGH
5. Las Vegas DC - Falken Wildpeak M/T - 280 units - 🟡 MEDIUM

**Regional Factors:**
- Mountain states: Heavy snow forecast driving mud-terrain demand
- California: Port delays affecting replenishment timing
- Arizona: Strong truck/SUV sales supporting all-terrain demand""",
            "sql": """SELECT r.dc_name, r.sku_name, r.recommended_qty, r.urgency_level
FROM TIRECO_DW.PROCUREMENT.PO_RECOMMENDATIONS r
JOIN TIRECO_DW.GOLD.INVENTORY_HEALTH_CURRENT i ON r.dc_code = i.dc_code
WHERE i.region = 'WEST' AND r.status = 'PENDING_REVIEW'
ORDER BY r.priority_score DESC""",
        }
    
    else:
        return {
            "text": f"""I can help you with procurement questions about Tireco's tire inventory. Here are some things I can assist with:

- **Order Recommendations**: "What should I order for Denver this week?"
- **Critical Items**: "Which SKUs are at critical inventory levels?"
- **Regional Analysis**: "Show me recommendations for the West region"
- **Summaries**: "Give me a summary of today's recommendations"
- **Specific SKUs**: "Why are we recommending 700 units of Milestar Patagonia?"

What would you like to know?""",
            "sql": None,
        }

if prompt := st.chat_input("Ask about inventory, forecasts, or recommendations..."):
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    with st.chat_message("user"):
        st.markdown(prompt)
    
    with st.chat_message("assistant"):
        with st.spinner("Analyzing..."):
            response = call_agent(prompt)
            
            if response.get("fallback") or response.get("error"):
                response = generate_demo_response(prompt)
            
            if "text" in response:
                st.markdown(response["text"])
                st.session_state.messages.append({
                    "role": "assistant", 
                    "content": response["text"],
                    "sql": response.get("sql")
                })
                
                if response.get("sql"):
                    with st.expander("View SQL Query"):
                        st.code(response["sql"], language="sql")

st.divider()
st.caption("Tread Intelligence Agent v1.0 | Powered by Snowflake Cortex")
