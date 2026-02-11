"""
Visualization Components
"""
import plotly.graph_objects as go
import plotly.express as px
from plotly.subplots import make_subplots

class Visualizations:
    @staticmethod
    def create_revenue_chart(wo_df):
        """Create revenue breakdown chart"""
        sector_revenue = wo_df.groupby('sector')['contract_value'].sum().reset_index()
        
        fig = px.pie(
            sector_revenue, 
            values='contract_value', 
            names='sector',
            title='Revenue by Sector',
            hole=0.4
        )
        fig.update_traces(textinfo='label+percent', textposition='outside')
        return fig
    
    @staticmethod
    def create_pipeline_funnel(deals_df):
        """Create sales funnel"""
        open_deals = deals_df[deals_df['Deal Status'] == 'Open']
        stage_data = open_deals.groupby('Deal Stage')['Masked Deal value'].sum().reset_index()
        
        # Sort by typical funnel order
        stage_order = [
            'A. Lead Generated',
            'B. Sales Qualified Leads', 
            'C. Demo Done',
            'D. Feasibility',
            'E. Proposal/Commercials Sent',
            'F. Negotiations',
            'G. Project Won',
            'H. Work Order Received'
        ]
        
        stage_data['sort_key'] = stage_data['Deal Stage'].apply(
            lambda x: stage_order.index(x) if x in stage_order else 99
        )
        stage_data = stage_data.sort_values('sort_key')
        
        fig = go.Figure(go.Funnel(
            y=stage_data['Deal Stage'],
            x=stage_data['Masked Deal value'],
            textinfo="value+percent initial"
        ))
        fig.update_layout(title='Sales Pipeline Funnel')
        return fig
    
    @staticmethod
    def create_execution_status_chart(wo_df):
        """Project execution status"""
        status_counts = wo_df['execution_status'].value_counts()
        
        colors = {
            'Completed': '#2ecc71',
            'Ongoing': '#3498db',
            'Not Started': '#95a5a6',
            'Pause / struck': '#e74c3c'
        }
        
        fig = go.Figure(data=[go.Bar(
            x=status_counts.index,
            y=status_counts.values,
            marker_color=[colors.get(s, '#34495e') for s in status_counts.index]
        )])
        fig.update_layout(
            title='Project Execution Status',
            xaxis_title='Status',
            yaxis_title='Count'
        )
        return fig
    
    @staticmethod
    def create_receivables_aging(aging_data):
        """AR aging chart"""
        fig = go.Figure(data=[
            go.Bar(
                x=['Current', '1-30 Days', '31-60 Days', '60+ Days'],
                y=[aging_data['current'], aging_data['30_days'], 
                   aging_data['60_days'], aging_data['90_plus']],
                marker_color=['#2ecc71', '#f1c40f', '#e67e22', '#e74c3c']
            )
        ])
        fig.update_layout(
            title='Receivables Aging',
            xaxis_title='Aging Bucket',
            yaxis_title='Amount (₹)'
        )
        return fig
    
    @staticmethod
    def create_kpi_cards(metrics):
        """Create KPI summary cards (returns HTML)"""
        cards_html = f"""
        <div style="display: flex; justify-content: space-between; gap: 20px; margin-bottom: 30px;">
            <div style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%); 
                        padding: 20px; border-radius: 10px; flex: 1; text-align: center; color: white;">
                <h3 style="margin: 0; font-size: 14px; opacity: 0.9;">Total Pipeline</h3>
                <p style="margin: 10px 0 0 0; font-size: 24px; font-weight: bold;">
                    ₹{metrics.get('total_open_pipeline', 0)/1e6:.1f}M
                </p>
            </div>
            <div style="background: linear-gradient(135deg, #f093fb 0%, #f5576c 100%); 
                        padding: 20px; border-radius: 10px; flex: 1; text-align: center; color: white;">
                <h3 style="margin: 0; font-size: 14px; opacity: 0.9;">Revenue Billed</h3>
                <p style="margin: 10px 0 0 0; font-size: 24px; font-weight: bold;">
                    ₹{metrics.get('total_billed', 0)/1e6:.1f}M
                </p>
            </div>
            <div style="background: linear-gradient(135deg, #4facfe 0%, #00f2fe 100%); 
                        padding: 20px; border-radius: 10px; flex: 1; text-align: center; color: white;">
                <h3 style="margin: 0; font-size: 14px; opacity: 0.9;">Outstanding AR</h3>
                <p style="margin: 10px 0 0 0; font-size: 24px; font-weight: bold;">
                    ₹{metrics.get('total_receivable', 0)/1e6:.1f}M
                </p>
            </div>
        </div>
        """
        return cards_html