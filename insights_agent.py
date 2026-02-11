import pandas as pd
import re


def clean_dataframe(df):
    """Ensure column names are clean strings."""
    df.columns = [str(col).strip().lower() for col in df.columns]
    return df


def safe_numeric(series):
    """Convert series to numeric, handling currency strings."""
    # Handle string values with currency symbols and commas
    if series.dtype == object:
        series = series.astype(str).str.replace(r'[₹$,]', '', regex=True).str.replace(r',', '', regex=False)
    return pd.to_numeric(series, errors="coerce").fillna(0)


def extract_keywords(query):
    """Extract intent keywords from query for better matching."""
    query = query.lower()
    keywords = {
        'conversion': ['convert', 'conversion', 'converting', 'effectively', 'efficiency', 'win rate', 'close rate'],
        'revenue': ['revenue', 'billed', 'collected', 'realized', 'income', 'earnings', 'money made'],
        'pipeline': ['pipeline', 'deals', 'opportunities', 'forecast', 'upcoming'],
        'sector': ['sector', 'industry', 'vertical', 'segment', 'domain', 'category'],
        'performance': ['performance', 'overview', 'summary', 'health', 'status', 'how are we doing'],
        'collection': ['collection', 'collect', 'receivable', 'outstanding', 'payment', 'unpaid', 'pending'],
        'trends': ['trend', 'growth', 'decline', 'change', 'compare', 'over time', 'quarter', 'monthly'],
        'leadership': ['leadership', 'executive', 'founder', 'ceo', 'report', 'board', 'investor']
    }
    
    detected = []
    for category, words in keywords.items():
        if any(word in query for word in words):
            detected.append(category)
    return detected


def calculate_conversion_metrics(work_df, deals_df, revenue_col, deal_value_col, deal_status_col):
    """Calculate actual conversion and effectiveness metrics."""
    metrics = {}
    
    # Basic counts
    total_deals = len(deals_df) if not deals_df.empty else 0
    
    # Deal status breakdown
    if deal_status_col in deals_df.columns:
        status_counts = deals_df[deal_status_col].value_counts().to_dict()
        won_deals = status_counts.get('Won', 0) + status_counts.get('won', 0) + status_counts.get('Closed Won', 0)
        lost_deals = status_counts.get('Lost', 0) + status_counts.get('lost', 0) + status_counts.get('Closed Lost', 0)
        active_deals = total_deals - won_deals - lost_deals
    else:
        won_deals = lost_deals = active_deals = 0
        status_counts = {}
    
    # Financial totals - ensure numeric
    total_pipeline = 0
    if deal_value_col in deals_df.columns:
        total_pipeline = safe_numeric(deals_df[deal_value_col]).sum()
    
    total_revenue = 0
    if revenue_col in work_df.columns:
        total_revenue = safe_numeric(work_df[revenue_col]).sum()
    
    # Conversion calculations
    metrics['total_deals'] = int(total_deals)
    metrics['won_deals'] = int(won_deals)
    metrics['lost_deals'] = int(lost_deals)
    metrics['active_deals'] = int(active_deals)
    metrics['win_rate'] = float((won_deals / (won_deals + lost_deals) * 100) if (won_deals + lost_deals) > 0 else 0)
    metrics['conversion_rate'] = float((won_deals / total_deals * 100) if total_deals > 0 else 0)
    metrics['total_pipeline'] = float(total_pipeline)
    metrics['total_revenue'] = float(total_revenue)
    metrics['realization_rate'] = float((total_revenue / total_pipeline * 100) if total_pipeline > 0 else 0)
    metrics['status_counts'] = status_counts
    
    # Pipeline health score (0-100)
    if total_deals > 0:
        health_score = (metrics['win_rate'] * 0.4) + (min(metrics['realization_rate'], 100) * 0.6)
    else:
        health_score = 0
    metrics['health_score'] = float(min(health_score, 100))
    
    return metrics


def analyze_collections(work_df):
    """Analyze collection effectiveness."""
    billed_col = "billed value in rupees (incl of gst.) (masked)"
    collected_col = "collected amount in rupees (incl of gst.) (masked)"
    receivable_col = "amount receivable (masked)"
    
    analysis = {}
    
    if billed_col in work_df.columns:
        total_billed = safe_numeric(work_df[billed_col]).sum()
        analysis['total_billed'] = float(total_billed)
        
        if collected_col in work_df.columns:
            total_collected = safe_numeric(work_df[collected_col]).sum()
            analysis['total_collected'] = float(total_collected)
            analysis['collection_rate'] = float((total_collected / total_billed * 100) if total_billed > 0 else 0)
        else:
            analysis['total_collected'] = 0.0
            analysis['collection_rate'] = 0.0
            
        if receivable_col in work_df.columns:
            analysis['receivables'] = float(safe_numeric(work_df[receivable_col]).sum())
        else:
            analysis['receivables'] = float(analysis['total_billed'] - analysis['total_collected'])
    
    return analysis


def sector_analysis(work_df, revenue_col):
    """Analyze performance by sector."""
    sector_col = "sector"
    
    if sector_col not in work_df.columns or revenue_col not in work_df.columns:
        return None
    
    # Ensure revenue is numeric before grouping
    work_df_copy = work_df.copy()
    work_df_copy[revenue_col] = safe_numeric(work_df_copy[revenue_col])
    
    # Group and aggregate
    sector_data = work_df_copy.groupby(sector_col).agg({
        revenue_col: ['sum', 'count']
    }).reset_index()
    
    # Flatten column names
    sector_data.columns = ['sector', 'revenue', 'deal_count']
    
    # Ensure types are correct
    sector_data['sector'] = sector_data['sector'].astype(str)
    sector_data['revenue'] = pd.to_numeric(sector_data['revenue'], errors='coerce').fillna(0)
    sector_data['deal_count'] = pd.to_numeric(sector_data['deal_count'], errors='coerce').fillna(0).astype(int)
    
    sector_data = sector_data.sort_values('revenue', ascending=False)
    
    return sector_data


def generate_ai_response(query, work_df, deals_df):
    """
    Generate contextual AI response based on query intent.
    Uses keyword extraction and calculated metrics rather than hardcoded strings.
    """
    
    # Clean dataframes
    work_df = clean_dataframe(work_df.copy())
    deals_df = clean_dataframe(deals_df.copy())
    
    # Column mappings (standardized)
    revenue_col = "billed value in rupees (incl of gst.) (masked)"
    deal_value_col = "masked deal value"
    deal_status_col = "deal status"
    
    # Extract intent
    intents = extract_keywords(query)
    
    # Calculate all metrics upfront
    metrics = calculate_conversion_metrics(work_df, deals_df, revenue_col, deal_value_col, deal_status_col)
    collection_data = analyze_collections(work_df)
    sector_data = sector_analysis(work_df, revenue_col)
    
    # PRIORITY 1: Conversion & Effectiveness Questions
    if 'conversion' in intents or any(word in query for word in ['effectively', 'efficiency', 'win rate', 'close rate', 'converting']):
        
        health_status = "🟢 Healthy" if metrics['health_score'] > 70 else "🟡 Needs Attention" if metrics['health_score'] > 40 else "🔴 Critical"
        
        return f"""
🎯 **Pipeline Conversion Effectiveness Analysis**

**Conversion Metrics:**
• Total Deals: **{metrics['total_deals']}**
• Won: **{metrics['won_deals']}** | Lost: **{metrics['lost_deals']}** | Active: **{metrics['active_deals']}**
• **Win Rate: {metrics['win_rate']:.1f}%** (Closed deals only)
• **Overall Conversion: {metrics['conversion_rate']:.1f}%** (All deals)

**Financial Realization:**
• Pipeline Value: ₹ {metrics['total_pipeline']:,.2f}
• Realized Revenue: ₹ {metrics['total_revenue']:,.2f}
• **Realization Rate: {metrics['realization_rate']:.1f}%**
• Pipeline Health Score: **{health_status}** ({metrics['health_score']:.0f}/100)

**Assessment:**
{'✅ Excellent conversion efficiency! Revenue closely matches pipeline.' if metrics['realization_rate'] > 80 else 
 '✅ Good conversion rate. Monitor pipeline quality.' if metrics['realization_rate'] > 50 else 
 '⚠️ Revenue significantly lags pipeline. Focus on deal qualification and closing techniques.'}

**Recommendations:**
1. {'Maintain current sales process' if metrics['win_rate'] > 40 else 'Review sales methodology and training'}
2. {'Accelerate active deal closure' if metrics['active_deals'] > metrics['won_deals'] else 'Generate new pipeline'}
3. {'Improve deal qualification to reduce losses' if metrics['lost_deals'] > metrics['won_deals'] else 'Scale successful approaches'}
"""
    
    # PRIORITY 2: Collection & Cash Flow Questions
    elif 'collection' in intents:
        if not collection_data:
            return "❌ Collection data not available in work orders."
        
        return f"""
💰 **Collection & Receivables Analysis**

**Collection Performance:**
• Total Billed: ₹ {collection_data.get('total_billed', 0):,.2f}
• Total Collected: ₹ {collection_data.get('total_collected', 0):,.2f}
• Outstanding Receivables: ₹ {collection_data.get('receivables', 0):,.2f}
• **Collection Rate: {collection_data.get('collection_rate', 0):.1f}%**

**Cash Flow Health:**
{'✅ Excellent collection rate. Strong cash flow position.' if collection_data.get('collection_rate', 0) > 90 else 
 '⚠️ Moderate collections. Monitor aging receivables.' if collection_data.get('collection_rate', 0) > 70 else 
 '🔴 Poor collection rate. Immediate attention required on receivables.'}

**Action Items:**
1. {'Continue proactive collection practices' if collection_data.get('collection_rate', 0) > 90 else 'Implement stricter payment terms'}
2. {'Review outstanding invoices >30 days' if collection_data.get('receivables', 0) > collection_data.get('total_billed', 0) * 0.2 else 'Maintain current credit policies'}
3. Consider early payment incentives or automated reminders
"""
    
    # PRIORITY 3: Sector Performance Questions
    elif 'sector' in intents:
        if sector_data is None or sector_data.empty:
            return "❌ Sector data not available in work orders."
        
        top_sector = sector_data.iloc[0]
        total_revenue = float(sector_data['revenue'].sum())
        
        # Build sector breakdown safely
        breakdown_lines = []
        for _, row in sector_data.head(5).iterrows():
            sector_name = str(row['sector'])
            revenue_val = float(row['revenue'])
            deal_count_val = int(row['deal_count'])
            percentage = (revenue_val / total_revenue * 100) if total_revenue > 0 else 0
            line = f"  • {sector_name}: ₹ {revenue_val:,.2f} ({percentage:.1f}%) - {deal_count_val} deals"
            breakdown_lines.append(line)
        
        sector_breakdown = "\n".join(breakdown_lines)
        
        top_revenue = float(top_sector['revenue'])
        top_percentage = (top_revenue / total_revenue * 100) if total_revenue > 0 else 0
        
        return f"""
🏆 **Sector Performance Analysis**

**Top Performing Sector: {top_sector['sector']}**
• Revenue: ₹ {top_revenue:,.2f} ({top_percentage:.1f}% of total)
• Deals: {int(top_sector['deal_count'])}

**Sector Breakdown:**
{sector_breakdown}

**Strategic Insights:**
• {'Revenue is well-diversified across sectors' if (top_revenue / total_revenue) < 0.5 else f'Heavy reliance on {top_sector["sector"]} - consider diversification'}
• {'Opportunity to strengthen secondary sectors' if len(sector_data) > 3 else 'Focus on core competency expansion'}

**Recommendations:**
1. Double down on {top_sector['sector']} success factors
2. {'Invest in underperforming sectors' if len(sector_data) > 1 else 'Develop additional sector expertise'}
3. Align marketing spend with high-conversion sectors
"""
    
    # PRIORITY 4: Revenue Specific Questions
    elif 'revenue' in intents:
        collection_rate = collection_data.get('collection_rate', 0) if collection_data else 0
        
        return f"""
💰 **Revenue Analysis**

**Realized Revenue:**
• Total Billed: ₹ {metrics['total_revenue']:,.2f}
• Collected: ₹ {collection_data.get('total_collected', 0):,.2f if collection_data else 0:,.2f}
• Collection Efficiency: {collection_rate:.1f}%

**Revenue vs Pipeline:**
• Pipeline: ₹ {metrics['total_pipeline']:,.2f}
• Conversion Gap: ₹ {metrics['total_pipeline'] - metrics['total_revenue']:,.2f}

**Performance Indicators:**
{'✅ Revenue realization is strong' if metrics['realization_rate'] > 60 else '⚠️ Gap between pipeline and revenue needs attention'}
{'✅ Collections are healthy' if collection_rate > 80 else '⚠️ Improve collection processes'}

**Focus Areas:**
1. Convert remaining pipeline to revenue ({metrics['active_deals']} active deals)
2. {'Accelerate collections' if collection_rate < 90 else 'Maintain collection momentum'}
3. Review pricing in top sectors
"""
    
    # PRIORITY 5: Pipeline Specific Questions (Generic)
    elif 'pipeline' in intents:
        status_lines = []
        for status, count in list(metrics['status_counts'].items())[:5]:
            status_lines.append(f"  • {status}: {count}")
        status_distribution = "\n".join(status_lines) if status_lines else "  • No status data available"
        
        most_common = max(metrics['status_counts'], key=metrics['status_counts'].get) if metrics['status_counts'] else 'N/A'
        
        return f"""
📈 **Pipeline Overview**

**Current Pipeline Status:**
• Total Value: ₹ {metrics['total_pipeline']:,.2f}
• Total Deals: {metrics['total_deals']}
• Active Opportunities: {metrics['active_deals']}
• Won: {metrics['won_deals']} | Lost: {metrics['lost_deals']}

**Deal Status Distribution:**
{status_distribution}

**Pipeline Health:**
• Win Rate: {metrics['win_rate']:.1f}%
• Most Common Status: {most_common}

**Strategic Insight:**
The pipeline of ₹ {metrics['total_pipeline']:,.2f} represents {'strong' if metrics['total_pipeline'] > metrics['total_revenue'] else 'concerning'} future revenue potential. 
{'Focus on closing active deals to realize value.' if metrics['active_deals'] > 0 else 'Generate new opportunities to replenish pipeline.'}
"""
    
    # PRIORITY 6: Trends & Time Analysis
    elif 'trends' in intents:
        return f"""
📊 **Business Trends & Trajectory**

**Current Position:**
• Revenue: ₹ {metrics['total_revenue']:,.2f}
• Pipeline: ₹ {metrics['total_pipeline']:,.2f}
• Ratio (Rev/Pipe): {metrics['realization_rate']:.1f}%

**Trend Indicators:**
• {'Growing: Pipeline exceeds revenue' if metrics['total_pipeline'] > metrics['total_revenue'] else 'Maturing: Revenue catching up to pipeline'}
• {'Healthy win rate' if metrics['win_rate'] > 30 else 'Improve win rate needed'} ({metrics['win_rate']:.1f}%)
• {'Strong collection foundation' if collection_data and collection_data.get('collection_rate', 0) > 80 else 'Collection process needs optimization'}

*Note: Connect time-series data for month-over-month trend analysis*
"""
    
    # PRIORITY 7: Leadership/Executive Summary
    elif 'leadership' in intents or 'performance' in intents:
        health_emoji = "🟢" if metrics['health_score'] > 70 else "🟡" if metrics['health_score'] > 40 else "🔴"
        
        top_sector_name = sector_data.iloc[0]['sector'] if sector_data is not None and not sector_data.empty else 'N/A'
        collection_rate_val = collection_data.get('collection_rate', 0) if collection_data else 0
        receivables_val = collection_data.get('receivables', 0) if collection_data else 0
        
        return f"""
📢 **Executive Leadership Summary**

{health_emoji} **Overall Health Score: {metrics['health_score']:.0f}/100**

**Financial Snapshot:**
• 💰 Revenue Realized: ₹ {metrics['total_revenue']:,.2f}
• 📈 Active Pipeline: ₹ {metrics['total_pipeline']:,.2f}
• 🎯 Conversion Rate: {metrics['conversion_rate']:.1f}%
• 💵 Collection Rate: {collection_rate_val:.1f}%

**Operational Highlights:**
• Top Sector: {top_sector_name}
• Deal Success: {metrics['won_deals']} won / {metrics['lost_deals']} lost
• Outstanding Receivables: ₹ {receivables_val:,.2f}

**CEO Priorities:**
1. **Conversion:** {'Scale success' if metrics['conversion_rate'] > 40 else 'Fix funnel - too many losses'}
2. **Cash Flow:** {'Optimize working capital' if collection_data and receivables_val > metrics['total_revenue'] * 0.3 else 'Maintain strong collections'}
3. **Growth:** {'Diversify sectors' if sector_data is not None and len(sector_data) < 3 else 'Dominate top sectors'}

**Board-Ready Insight:**
We are {'hitting targets with strong unit economics' if metrics['health_score'] > 70 else 'operationally challenged but fixable' if metrics['health_score'] > 40 else 'in critical need of strategy reset'}.
"""
    
    # DEFAULT: Smart Fallback
    else:
        return f"""
📊 **Business Intelligence Summary**

I analyzed your query: *"{query}"*

**Available Metrics:**
• Revenue: ₹ {metrics['total_revenue']:,.2f}
• Pipeline: ₹ {metrics['total_pipeline']:,.2f}
• Win Rate: {metrics['win_rate']:.1f}%
• Active Deals: {metrics['active_deals']}

**Try asking about:**
• "How effective is our pipeline conversion?"
• "Which sector performs best?"
• "What's our collection rate?"
• "Give me a leadership summary"
• "Are we converting pipeline effectively?"

*Detected intents: {', '.join(intents) if intents else 'General inquiry'}*
"""