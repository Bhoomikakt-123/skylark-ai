import streamlit as st
import pandas as pd
import json
from datetime import datetime
from io import StringIO

from fetch_data import fetch_board_data
from insights_agent import generate_ai_response, extract_keywords, calculate_conversion_metrics, analyze_collections, sector_analysis, clean_dataframe, safe_numeric

# -----------------------------
# PAGE CONFIG
# -----------------------------

st.set_page_config(
    page_title="Skylark AI - Business Intelligence Agent",
    page_icon="🎯",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Custom CSS for professional look
st.markdown("""
<style>
    .main-header {
        font-size: 2.5rem;
        font-weight: bold;
        color: #1f77b4;
        margin-bottom: 0.5rem;
    }
    .sub-header {
        font-size: 1.1rem;
        color: #666;
        margin-bottom: 2rem;
    }
    .chat-container {
        border-radius: 10px;
        padding: 20px;
        background-color: #f0f2f6;
        margin-bottom: 20px;
    }
    .metric-card {
        background-color: white;
        padding: 20px;
        border-radius: 10px;
        box-shadow: 0 2px 4px rgba(0,0,0,0.1);
        margin-bottom: 10px;
    }
    .leadership-report {
        background-color: #ffffff;
        border-left: 4px solid #1f77b4;
        padding: 20px;
        margin: 20px 0;
        border-radius: 5px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
    }
    .stButton>button {
        width: 100%;
        border-radius: 5px;
        height: 3em;
    }
    .sample-question {
        font-size: 0.9rem;
        padding: 8px 12px;
        margin: 4px 0;
        background-color: #e9ecef;
        border-radius: 15px;
        cursor: pointer;
        transition: all 0.3s;
    }
    .sample-question:hover {
        background-color: #dee2e6;
        transform: translateX(5px);
    }
</style>
""", unsafe_allow_html=True)

# -----------------------------
# SESSION STATE INITIALIZATION
# -----------------------------

def init_session_state():
    """Initialize all session state variables."""
    if "messages" not in st.session_state:
        st.session_state.messages = []
    
    if "context" not in st.session_state:
        st.session_state.context = {
            "last_query": None,
            "last_intent": None,
            "mentioned_sectors": [],
            "date_range": None,
            "clarification_needed": False,
            "clarification_question": None,
            "pending_query": None
        }
    
    if "data_cache" not in st.session_state:
        st.session_state.data_cache = {
            "work_df": None,
            "deals_df": None,
            "last_fetch": None
        }
    
    if "leadership_reports" not in st.session_state:
        st.session_state.leadership_reports = []
    
    if "show_raw_data" not in st.session_state:
        st.session_state.show_raw_data = False

init_session_state()

# -----------------------------
# CONFIGURATION
# -----------------------------

# Board IDs - replace with your actual IDs
WORK_BOARD_ID = 5026565302
DEALS_BOARD_ID = 5026565276

# -----------------------------
# DATA FETCHING WITH CACHE
# -----------------------------

@st.cache_data(ttl=300)
def load_data(force_refresh=False):
    """Fetch data from Monday.com with error handling."""
    try:
        with st.spinner("🔄 Connecting to Monday.com..."):
            work_df = fetch_board_data(WORK_BOARD_ID)
            deals_df = fetch_board_data(DEALS_BOARD_ID)
        
        # Update cache timestamp
        st.session_state.data_cache["last_fetch"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        
        return work_df, deals_df
        
    except Exception as e:
        st.error(f"❌ Failed to fetch data from Monday.com: {str(e)}")
        return pd.DataFrame(), pd.DataFrame()

def get_data():
    """Get data from cache or fetch fresh."""
    if st.session_state.data_cache["work_df"] is None or st.session_state.data_cache["deals_df"] is None:
        work_df, deals_df = load_data()
        st.session_state.data_cache["work_df"] = work_df
        st.session_state.data_cache["deals_df"] = deals_df
    else:
        work_df = st.session_state.data_cache["work_df"]
        deals_df = st.session_state.data_cache["deals_df"]
    
    return work_df, deals_df

# -----------------------------
# CLARIFYING QUESTIONS LOGIC
# -----------------------------

def check_for_clarification(query, work_df, deals_df):
    """
    Check if we need to ask clarifying questions.
    Returns: (needs_clarification: bool, question: str, clarified_query: str)
    """
    query_lower = query.lower()
    intents = extract_keywords(query)
    
    # Check for ambiguous sector mentions
    if 'sector' in intents or any(word in query_lower for word in ['sector', 'industry', 'vertical']):
        # Get available sectors
        if 'sector' in work_df.columns:
            available_sectors = work_df['sector'].dropna().unique().tolist()
            
            # Check if query mentions a sector that doesn't exist
            sector_mentioned = None
            for sector in available_sectors:
                if sector.lower() in query_lower:
                    sector_mentioned = sector
                    break
            
            # If asking about sector but no specific match found
            if not sector_mentioned and len(available_sectors) > 0:
                sector_list = ", ".join([f"'{s}'" for s in available_sectors[:5]])
                return True, f"I found these sectors: {sector_list}. Which one would you like to know about?", query
    
    # Check for date ambiguity
    time_words = ['this quarter', 'last quarter', 'this month', 'last month', 'recent', 'latest']
    if any(word in query_lower for word in time_words):
        # For now, assume current data is "this quarter" since we don't have historical
        if 'quarter' in query_lower:
            return True, "I currently have the latest data available. Are you looking for Q1, Q2, Q3, or Q4 2024?", query
    
    # Check for extremely vague queries
    vague_words = ['how', 'what', 'tell me', 'update']
    if len(query.split()) < 4 and any(word in query_lower for word in vague_words):
        return True, "Could you be more specific? You can ask about revenue, pipeline, sector performance, or collections.", query
    
    return False, None, query

# -----------------------------
# LEADERSHIP REPORT GENERATION
# -----------------------------

def generate_leadership_report(work_df, deals_df):
    """Generate a structured leadership report."""
    
    # Clean data
    work_df_clean = clean_dataframe(work_df.copy())
    deals_df_clean = clean_dataframe(deals_df.copy())
    
    # Calculate metrics
    revenue_col = "billed value in rupees (incl of gst.) (masked)"
    deal_value_col = "masked deal value"
    deal_status_col = "deal status"
    
    metrics = calculate_conversion_metrics(work_df_clean, deals_df_clean, revenue_col, deal_value_col, deal_status_col)
    collection_data = analyze_collections(work_df_clean)
    sector_data = sector_analysis(work_df_clean, revenue_col)
    
    # Determine health status
    health_status = "Healthy" if metrics['health_score'] > 70 else "Needs Attention" if metrics['health_score'] > 40 else "Critical"
    health_emoji = "🟢" if metrics['health_score'] > 70 else "🟡" if metrics['health_score'] > 40 else "🔴"
    
    # Top sector
    top_sector = sector_data.iloc[0]['sector'] if sector_data is not None and not sector_data.empty else 'N/A'
    
    # Generate timestamp
    report_time = datetime.now().strftime("%B %d, %Y at %I:%M %p")
    
    report = f"""
# 📊 Executive Leadership Report
**Generated:** {report_time}

---

## 🎯 Overall Business Health: {health_emoji} {health_status} ({metrics['health_score']:.0f}/100)

---

## 💰 Financial Performance

| Metric | Value | Status |
|--------|-------|--------|
| **Total Revenue** | ₹ {metrics['total_revenue']:,.2f} | {'✅ Strong' if metrics['total_revenue'] > 1000000 else '⚠️ Low'} |
| **Active Pipeline** | ₹ {metrics['total_pipeline']:,.2f} | {'✅ Growing' if metrics['total_pipeline'] > metrics['total_revenue'] else '⚠️ Stagnant'} |
| **Realization Rate** | {metrics['realization_rate']:.1f}% | {'✅ Good' if metrics['realization_rate'] > 50 else '⚠️ Needs Work'} |
| **Collection Rate** | {collection_data.get('collection_rate', 0):.1f}% | {'✅ Healthy' if collection_data.get('collection_rate', 0) > 80 else '⚠️ Poor'} |

---

## 📈 Operational Metrics

- **Total Deals:** {metrics['total_deals']}
- **Win Rate:** {metrics['win_rate']:.1f}% ({metrics['won_deals']} won / {metrics['lost_deals']} lost)
- **Active Opportunities:** {metrics['active_deals']} deals in progress
- **Top Performing Sector:** {top_sector}

---

## 🎯 CEO Action Items

1. **Revenue Growth:** {'Accelerate deal closures' if metrics['active_deals'] > 5 else 'Generate new pipeline'}
2. **Conversion:** {'Maintain momentum' if metrics['win_rate'] > 30 else 'Review sales process'}
3. **Cash Flow:** {'Monitor receivables' if collection_data.get('receivables', 0) > 1000000 else 'Optimize working capital'}

---

## 📢 Board-Ready Summary

We are currently operating at **{health_status}** levels with ₹ {metrics['total_revenue']:,.2f} in realized revenue 
and ₹ {metrics['total_pipeline']:,.2f} in active pipeline. The {top_sector} sector is driving our growth with 
a {metrics['win_rate']:.1f}% win rate. {'Immediate attention required on collections.' if collection_data.get('collection_rate', 0) < 70 else 'Collections are on track.'}

---

*Report prepared by Skylark AI Business Intelligence Agent*
"""
    
    return report, {
        "timestamp": report_time,
        "health_score": metrics['health_score'],
        "revenue": metrics['total_revenue'],
        "pipeline": metrics['total_pipeline'],
        "status": health_status
    }

# -----------------------------
# SIDEBAR
# -----------------------------

with st.sidebar:
    # Fixed: Removed use_column_width, using markdown instead
    st.markdown("### 🎯 Skylark AI")
    st.markdown("**Business Intelligence Agent**")
    st.markdown("---")
    
    st.header("⚙️ Control Panel")
    
    # Data refresh
    if st.button("🔄 Refresh Data", key="refresh"):
        st.cache_data.clear()
        st.session_state.data_cache["work_df"] = None
        st.session_state.data_cache["deals_df"] = None
        st.session_state.messages = []
        st.session_state.context = {
            "last_query": None,
            "last_intent": None,
            "mentioned_sectors": [],
            "date_range": None,
            "clarification_needed": False,
            "clarification_question": None,
            "pending_query": None
        }
        st.rerun()
    
    # Show last fetch time
    if st.session_state.data_cache["last_fetch"]:
        st.caption(f"Last updated: {st.session_state.data_cache['last_fetch']}")
    
    st.markdown("---")
    
    # Leadership Report Button
    st.subheader("📋 Leadership Tools")
    if st.button("📊 Generate Leadership Report", key="leadership_btn"):
        work_df, deals_df = get_data()
        if not work_df.empty and not deals_df.empty:
            with st.spinner("Generating executive report..."):
                report, metadata = generate_leadership_report(work_df, deals_df)
                st.session_state.leadership_reports.append({
                    "report": report,
                    "metadata": metadata
                })
                st.success("✅ Report generated! View in main panel.")
        else:
            st.error("❌ No data available to generate report")
    
    # View previous reports
    if st.session_state.leadership_reports:
        with st.expander(f"📁 Previous Reports ({len(st.session_state.leadership_reports)})"):
            for i, report_data in enumerate(reversed(st.session_state.leadership_reports[-5:])):
                st.markdown(f"**Report {len(st.session_state.leadership_reports) - i}** - {report_data['metadata']['timestamp']}")
                st.caption(f"Health: {report_data['metadata']['status']} | Revenue: ₹ {report_data['metadata']['revenue']:,.0f}")
    
    st.markdown("---")
    
    # Sample questions
    st.subheader("💡 Sample Questions")
    sample_questions = [
        "Are we converting pipeline effectively?",
        "What's our revenue by sector?",
        "How healthy is our cash collection?",
        "Which sector is performing best?",
        "Give me a leadership summary",
        "What's our win rate?",
        "How much is outstanding in receivables?"
    ]
    
    for i, q in enumerate(sample_questions):
        # Fixed: Truncate text for button, use full text in help tooltip
        display_text = q[:40] + "..." if len(q) > 40 else q
        if st.button(f"• {display_text}", key=f"sample_{i}", help=q):
            st.session_state.context["pending_query"] = q
            st.rerun()
    
    st.markdown("---")
    
    # Data quality indicator
    work_df, deals_df = get_data()
    if not work_df.empty and not deals_df.empty:
        st.success(f"✅ Data Connected\n• {len(work_df)} Work Orders\n• {len(deals_df)} Deals")
    else:
        st.error("❌ Check Connection")

# -----------------------------
# MAIN HEADER
# -----------------------------

st.markdown('<div class="main-header">🎯 Skylark AI</div>', unsafe_allow_html=True)
st.markdown('<div class="sub-header">Your Monday.com Business Intelligence Agent — Ask founder-level questions, get executive insights</div>', unsafe_allow_html=True)

# -----------------------------
# DATA STATUS BAR
# -----------------------------

work_df, deals_df = get_data()

status_col1, status_col2, status_col3, status_col4 = st.columns(4)

with status_col1:
    if not work_df.empty:
        st.metric("📋 Work Orders", len(work_df), "Active")
    else:
        st.metric("📋 Work Orders", "❌", "Error")

with status_col2:
    if not deals_df.empty:
        st.metric("🤝 Deals", len(deals_df), "In Pipeline")
    else:
        st.metric("🤝 Deals", "❌", "Error")

with status_col3:
    if not work_df.empty and "billed value in rupees (incl of gst.) (masked)" in work_df.columns:
        total_rev = safe_numeric(work_df["billed value in rupees (incl of gst.) (masked)"]).sum()
        st.metric("💰 Revenue", f"₹ {total_rev/1000000:.1f}M", "Total")
    else:
        st.metric("💰 Revenue", "₹ 0", "No data")

with status_col4:
    if not deals_df.empty and "masked deal value" in deals_df.columns:
        total_pipe = safe_numeric(deals_df["masked deal value"]).sum()
        st.metric("📈 Pipeline", f"₹ {total_pipe/1000000:.1f}M", "Total")
    else:
        st.metric("📈 Pipeline", "₹ 0", "No data")

st.markdown("---")

# -----------------------------
# LEADERSHIP REPORT DISPLAY
# -----------------------------

if st.session_state.leadership_reports:
    latest_report = st.session_state.leadership_reports[-1]
    
    with st.expander("📊 Latest Leadership Report (Click to expand)", expanded=True):
        st.markdown(latest_report["report"])
        
        # Export options
        col1, col2 = st.columns(2)
        with col1:
            st.download_button(
                label="📥 Download as Markdown",
                data=latest_report["report"],
                file_name=f"leadership_report_{datetime.now().strftime('%Y%m%d_%H%M')}.md",
                mime="text/markdown"
            )
        with col2:
            # Simple text version for copying
            st.button("📋 Copy to Clipboard", on_click=lambda: st.write("Report copied! (Simulated)"))

# -----------------------------
# CHAT INTERFACE
# -----------------------------

st.subheader("💬 Conversation")

# Display chat history
for message in st.session_state.messages:
    with st.chat_message(message["role"]):
        st.markdown(message["content"])
        
        # Show metadata for assistant messages
        if message["role"] == "assistant" and "metadata" in message:
            with st.expander("🔍 View Data Sources"):
                st.json(message["metadata"])

# Handle pending query from sample questions
if st.session_state.context.get("pending_query"):
    query = st.session_state.context["pending_query"]
    st.session_state.context["pending_query"] = None
    
    # Add to messages
    st.session_state.messages.append({"role": "user", "content": query})
    
    # Process query
    with st.chat_message("assistant"):
        with st.spinner("🧠 Analyzing..."):
            # Check for clarification needs
            needs_clarification, clarification_question, clarified_query = check_for_clarification(query, work_df, deals_df)
            
            if needs_clarification and not st.session_state.context["clarification_needed"]:
                response = f"🤔 I need a bit more clarity:\n\n{clarification_question}\n\nPlease provide more details so I can give you the most accurate answer."
                st.session_state.context["clarification_needed"] = True
                st.session_state.context["clarification_question"] = clarification_question
                st.session_state.context["pending_clarified_query"] = clarified_query
            else:
                # Generate actual response
                response = generate_ai_response(query, work_df, deals_df)
                st.session_state.context["clarification_needed"] = False
            
            st.markdown(response)
            
            # Store metadata
            metadata = {
                "query": query,
                "timestamp": datetime.now().isoformat(),
                "intents": extract_keywords(query),
                "data_rows": {
                    "work_orders": len(work_df),
                    "deals": len(deals_df)
                }
            }
            
            st.session_state.messages.append({
                "role": "assistant", 
                "content": response,
                "metadata": metadata
            })
    
    st.rerun()

# Chat input
if prompt := st.chat_input("Ask a founder-level question...", key="chat_input"):
    
    # Add user message
    st.session_state.messages.append({"role": "user", "content": prompt})
    
    # Check if this is a response to a clarification question
    if st.session_state.context.get("clarification_needed"):
        # Use the pending query with this additional context
        original_query = st.session_state.context.get("pending_clarified_query", prompt)
        # Combine for better context
        full_query = f"{original_query} ({prompt})"
        st.session_state.context["clarification_needed"] = False
        st.session_state.context["pending_clarified_query"] = None
    else:
        full_query = prompt
    
    # Generate response
    with st.chat_message("assistant"):
        with st.spinner("🧠 Analyzing your question..."):
            
            # Check if we need clarification
            needs_clarification, clarification_question, clarified_query = check_for_clarification(full_query, work_df, deals_df)
            
            if needs_clarification and not st.session_state.context.get("clarification_asked_for") == full_query:
                response = f"🤔 To give you the most accurate answer, I need a bit more information:\n\n**{clarification_question}**\n\nPlease reply with more specific details."
                st.session_state.context["clarification_needed"] = True
                st.session_state.context["clarification_question"] = clarification_question
                st.session_state.context["pending_clarified_query"] = clarified_query
                st.session_state.context["clarification_asked_for"] = full_query
            else:
                # Generate actual business intelligence response
                try:
                    response = generate_ai_response(full_query, work_df, deals_df)
                    
                    # Add context-aware follow-up suggestions
                    intents = extract_keywords(full_query)
                    if 'pipeline' in intents:
                        response += "\n\n💡 **Follow-up questions you might ask:**\n• Which deals are at risk?\n• What's our average deal size?\n• How long is our sales cycle?"
                    elif 'revenue' in intents:
                        response += "\n\n💡 **Follow-up questions you might ask:**\n• What's outstanding in receivables?\n• Which sector drives most revenue?\n• How does this compare to last quarter?"
                    
                except Exception as e:
                    response = f"❌ I encountered an error analyzing your data: {str(e)}\n\nPlease try rephrasing your question or check if the data boards are accessible."
                    st.error(f"Debug: {str(e)}")
            
            st.markdown(response)
            
            # Store with metadata
            metadata = {
                "query": full_query,
                "timestamp": datetime.now().isoformat(),
                "intents": extract_keywords(full_query),
                "data_quality": {
                    "work_orders_count": len(work_df),
                    "deals_count": len(deals_df),
                    "work_columns": list(work_df.columns) if not work_df.empty else [],
                    "deals_columns": list(deals_df.columns) if not deals_df.empty else []
                }
            }
            
            st.session_state.messages.append({
                "role": "assistant",
                "content": response,
                "metadata": metadata
            })
    
    st.rerun()

# -----------------------------
# DATA EXPLORER (Optional)
# -----------------------------

with st.expander("🔍 Data Explorer (Advanced)"):
    if st.checkbox("Show raw data tables"):
        col1, col2 = st.columns(2)
        
        with col1:
            st.subheader("Work Orders")
            if not work_df.empty:
                st.dataframe(work_df.head(10), use_container_width=True)
            else:
                st.warning("No work order data")
        
        with col2:
            st.subheader("Deals")
            if not deals_df.empty:
                st.dataframe(deals_df.head(10), use_container_width=True)
            else:
                st.warning("No deals data")
    
    if st.checkbox("Show data quality report"):
        st.subheader("Data Quality Assessment")
        
        quality_issues = []
        
        if work_df.empty:
            quality_issues.append("❌ Work Orders board is empty or inaccessible")
        else:
            if work_df.isnull().sum().sum() > 0:
                quality_issues.append(f"⚠️ Work Orders has {work_df.isnull().sum().sum()} missing values")
            if "sector" in work_df.columns:
                empty_sectors = work_df['sector'].isnull().sum()
                if empty_sectors > 0:
                    quality_issues.append(f"⚠️ {empty_sectors} work orders missing sector classification")
        
        if deals_df.empty:
            quality_issues.append("❌ Deals board is empty or inaccessible")
        else:
            if deals_df.isnull().sum().sum() > 0:
                quality_issues.append(f"⚠️ Deals has {deals_df.isnull().sum().sum()} missing values")
            if "deal status" in deals_df.columns:
                status_dist = deals_df["deal status"].value_counts()
                st.write("Deal Status Distribution:", status_dist)
        
        if not quality_issues:
            st.success("✅ Data quality looks good!")
        else:
            for issue in quality_issues:
                st.write(issue)

# -----------------------------
# FOOTER
# -----------------------------

st.markdown("---")
footer_col1, footer_col2, footer_col3 = st.columns([1, 2, 1])

with footer_col2:
    st.caption("🎯 Skylark AI Business Intelligence Agent | Built for Monday.com Integration")
    st.caption("📊 Real-time analytics • 🤖 AI-powered insights • 📋 Executive reporting")

# Debug info (hidden in production)
if st.checkbox("Show debug info", key="debug"):
    st.json({
        "context": st.session_state.context,
        "message_count": len(st.session_state.messages),
        "reports_generated": len(st.session_state.leadership_reports),
        "data_cache_status": "Active" if st.session_state.data_cache["work_df"] is not None else "Empty"
    })