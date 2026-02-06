import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from core.database import Database


class Visualizer:
    """数据可视化工具类"""

    def __init__(self, db: Database = None):
        """
        初始化可视化工具

        Args:
            db: 数据库实例
        """
        self.db = db or Database()

    def create_trend_chart(self, user_id: int = None, platform_id: int = None,
                          platform_ids: List[int] = None,
                          user_identity: str = None,
                          days: int = 30) -> go.Figure:
        """
        创建粉丝量趋势图

        Args:
            user_id: 用户ID（可选）
            platform_id: 平台ID（可选）
            platform_ids: 平台ID列表（可选）
            user_identity: 用户标识（可选）
            days: 显示天数

        Returns:
            Plotly图表对象
        """
        start_time = datetime.now() - timedelta(days=days)
        records = self.db.get_follower_records(
            user_id=user_id,
            platform_id=platform_id,
            platform_ids=platform_ids,
            user_identity=user_identity,
            start_time=start_time,
            limit=10000
        )

        if not records:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])

        platforms = self.db.get_all_platforms()
        platform_map = {p['id']: p['name'] for p in platforms}
        df['platform_name'] = df['platform_id'].map(platform_map)

        users = self.db.get_all_users()
        user_map = {u['id']: u for u in users}
        
        if 'user_identity' not in df.columns:
            df['user_identity'] = df['user_id'].map(
                lambda x: user_map.get(x, {}).get('user_identity', '0')
            )

        if user_identity:
            df_filtered = df[df['user_identity'] == user_identity]
            fig = go.Figure()
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
            
            for idx, platform_id in enumerate(df_filtered['platform_id'].unique()):
                platform_data = df_filtered[df_filtered['platform_id'] == platform_id].sort_values('record_time')
                if len(platform_data) > 0:
                    platform_name = platform_map.get(platform_id, f'平台{platform_id}')
                    fig.add_trace(go.Scatter(
                        x=platform_data['record_time'],
                        y=platform_data['follower_count'],
                        mode='lines+markers',
                        name=platform_name,
                        line=dict(color=colors[idx % len(colors)], width=2),
                        marker=dict(size=6)
                    ))
        elif user_id:
            df_grouped = df.sort_values('record_time')
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_grouped['record_time'],
                y=df_grouped['follower_count'],
                mode='lines+markers',
                name='粉丝数量',
                line=dict(color='#1f77b4', width=2),
                marker=dict(size=8)
            ))
        else:
            if platform_ids and len(platform_ids) > 1:
                fig = go.Figure()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
                
                for idx, platform_id in enumerate(platform_ids):
                    platform_data = df[df['platform_id'] == platform_id].sort_values('record_time')
                    if len(platform_data) > 0:
                        platform_name = platform_map.get(platform_id, f'平台{platform_id}')
                        fig.add_trace(go.Scatter(
                            x=platform_data['record_time'],
                            y=platform_data['follower_count'],
                            mode='lines+markers',
                            name=platform_name,
                            line=dict(color=colors[idx % len(colors)], width=2),
                            marker=dict(size=6)
                        ))
            else:
                df_grouped = df.groupby(['user_identity', 'record_time'])['follower_count'].first().reset_index()
                fig = go.Figure()

                for user_identity in df_grouped['user_identity'].unique():
                    user_data = df_grouped[df_grouped['user_identity'] == user_identity].sort_values('record_time')
                    display_name = f'用户 {user_identity}' if user_identity != '0' else '未分组用户'
                    fig.add_trace(go.Scatter(
                        x=user_data['record_time'],
                        y=user_data['follower_count'],
                        mode='lines+markers',
                        name=display_name,
                        line=dict(width=2),
                        marker=dict(size=6)
                    ))

        fig.update_layout(
            title='粉丝量趋势图',
            xaxis_title='时间',
            yaxis_title='粉丝数量',
            hovermode='x unified',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=80),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        fig.update_xaxes(
            tickformat="%Y-%m-%d %H:%M",
            tickangle=-45
        )

        return fig

    def create_comparison_chart(self, platform_id: int = None, user_identity: str = None, days: int = 30) -> go.Figure:
        """
        创建平台对比图

        Args:
            platform_id: 平台ID（可选）
            user_identity: 用户标识（可选）
            days: 显示天数

        Returns:
            Plotly图表对象
        """
        start_time = datetime.now() - timedelta(days=days)
        records = self.db.get_follower_records(
            platform_id=platform_id,
            user_identity=user_identity,
            start_time=start_time
        )

        if not records:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])

        users = self.db.get_all_users()
        user_map = {u['id']: u for u in users}
        
        if 'user_identity' not in df.columns:
            df['user_identity'] = df['user_id'].map(
                lambda x: user_map.get(x, {}).get('user_identity', '0')
            )

        latest_records = df.loc[df.groupby('user_identity')['record_time'].idxmax()]

        fig = go.Figure(data=[
            go.Bar(
                x=latest_records['user_identity'].astype(str),
                y=latest_records['follower_count'],
                marker=dict(
                    color=latest_records['follower_count'],
                    colorscale='Viridis',
                    showscale=True,
                    colorbar=dict(title="粉丝数")
                ),
                text=latest_records['follower_count'],
                textposition='outside'
            )
        ])

        fig.update_layout(
            title='用户粉丝量对比',
            xaxis_title='用户标识',
            yaxis_title='粉丝数量',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=80),
            showlegend=False
        )

        fig.update_xaxes(tickangle=-45)

        return fig

    def create_distribution_chart(self, platform_id: int = None, user_identity: str = None) -> go.Figure:
        """
        创建粉丝量分布图

        Args:
            platform_id: 平台ID（可选）
            user_identity: 用户标识（可选）

        Returns:
            Plotly图表对象
        """
        records = self.db.get_follower_records(platform_id=platform_id, user_identity=user_identity, limit=1000)

        if not records:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        df = pd.DataFrame(records)

        bins = [0, 1000, 10000, 50000, 100000, 500000, 1000000, float('inf')]
        labels = ['0-1K', '1K-10K', '10K-50K', '50K-100K', '100K-500K', '500K-1M', '1M+']
        df['range'] = pd.cut(df['follower_count'], bins=bins, labels=labels)

        distribution = df['range'].value_counts().sort_index()

        fig = go.Figure(data=[
            go.Pie(
                labels=distribution.index,
                values=distribution.values,
                hole=0.4,
                marker=dict(colors=px.colors.qualitative.Set3)
            )
        ])

        fig.update_layout(
            title='粉丝量分布',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=20),
            showlegend=True,
            legend=dict(
                orientation="v",
                yanchor="middle",
                y=0.5,
                xanchor="left",
                x=1.05
            )
        )

        return fig

    def create_growth_rate_chart(self, user_id: int = None, platform_id: int = None,
                                platform_ids: List[int] = None,
                                user_identity: str = None,
                                days: int = 30) -> go.Figure:
        """
        创建增长率图表

        Args:
            user_id: 用户ID（可选）
            platform_id: 平台ID（可选）
            platform_ids: 平台ID列表（可选）
            user_identity: 用户标识（可选）
            days: 显示天数

        Returns:
            Plotly图表对象
        """
        start_time = datetime.now() - timedelta(days=days)
        records = self.db.get_follower_records(
            user_id=user_id,
            platform_id=platform_id,
            platform_ids=platform_ids,
            user_identity=user_identity,
            start_time=start_time
        )

        if len(records) < 2:
            fig = go.Figure()
            fig.add_annotation(
                text="数据不足，无法计算增长率",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=16)
            )
            return fig

        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])

        platforms = self.db.get_all_platforms()
        platform_map = {p['id']: p['name'] for p in platforms}

        users = self.db.get_all_users()
        user_map = {u['id']: u for u in users}
        
        if 'user_identity' not in df.columns:
            df['user_identity'] = df['user_id'].map(
                lambda x: user_map.get(x, {}).get('user_identity', '0')
            )

        if user_identity:
            df_filtered = df[df['user_identity'] == user_identity]
            fig = go.Figure()
            colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
            
            for idx, platform_id in enumerate(df_filtered['platform_id'].unique()):
                platform_data = df_filtered[df_filtered['platform_id'] == platform_id].sort_values('record_time')
                if len(platform_data) >= 2:
                    platform_data = platform_data.copy()
                    platform_data['growth_rate'] = platform_data['follower_count'].pct_change() * 100
                    platform_name = platform_map.get(platform_id, f'平台{platform_id}')
                    fig.add_trace(go.Scatter(
                        x=platform_data['record_time'],
                        y=platform_data['growth_rate'],
                        mode='lines+markers',
                        name=platform_name,
                        line=dict(color=colors[idx % len(colors)], width=2),
                        marker=dict(size=6)
                    ))
        elif user_id:
            df_grouped = df.sort_values('record_time')
            df_grouped = df_grouped.copy()
            df_grouped['growth_rate'] = df_grouped['follower_count'].pct_change() * 100
            fig = go.Figure()
            fig.add_trace(go.Scatter(
                x=df_grouped['record_time'],
                y=df_grouped['growth_rate'],
                mode='lines+markers',
                name='增长率',
                line=dict(color='#2ecc71', width=2),
                marker=dict(size=8)
            ))
        else:
            if platform_ids and len(platform_ids) > 1:
                fig = go.Figure()
                colors = ['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728', '#9467bd', '#8c564b', '#e377c2', '#7f7f7f', '#bcbd22', '#17becf']
                
                for idx, platform_id in enumerate(platform_ids):
                    platform_data = df[df['platform_id'] == platform_id].sort_values('record_time')
                    if len(platform_data) >= 2:
                        platform_data = platform_data.copy()
                        platform_data['growth_rate'] = platform_data['follower_count'].pct_change() * 100
                        platform_name = platform_map.get(platform_id, f'平台{platform_id}')
                        fig.add_trace(go.Scatter(
                            x=platform_data['record_time'],
                            y=platform_data['growth_rate'],
                            mode='lines+markers',
                            name=platform_name,
                            line=dict(color=colors[idx % len(colors)], width=2),
                            marker=dict(size=6)
                        ))
            else:
                df_grouped = df.groupby(['user_identity', 'record_time'])['follower_count'].first().reset_index()
                df_grouped = df_grouped.sort_values('user_identity')
                fig = go.Figure()

                for user_identity in df_grouped['user_identity'].unique():
                    user_data = df_grouped[df_grouped['user_identity'] == user_identity].sort_values('record_time')
                    if len(user_data) >= 2:
                        user_data = user_data.copy()
                        user_data['growth_rate'] = user_data['follower_count'].pct_change() * 100
                        display_name = f'用户 {user_identity}' if user_identity != '0' else '未分组用户'
                        fig.add_trace(go.Scatter(
                            x=user_data['record_time'],
                            y=user_data['growth_rate'],
                            mode='lines+markers',
                            name=display_name,
                            line=dict(width=2),
                            marker=dict(size=6)
                        ))

        fig.update_layout(
            title='粉丝量增长率',
            xaxis_title='时间',
            yaxis_title='增长率 (%)',
            hovermode='x unified',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=80),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        fig.update_xaxes(
            tickformat="%Y-%m-%d %H:%M",
            tickangle=-45
        )

        return fig

    def create_platform_comparison_chart(self, days: int = 30) -> go.Figure:
        """
        创建多平台对比图

        Args:
            days: 显示天数

        Returns:
            Plotly图表对象
        """
        start_time = datetime.now() - timedelta(days=days)
        platforms = self.db.get_all_platforms()

        if not platforms:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无平台数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        fig = go.Figure()

        for platform in platforms:
            records = self.db.get_follower_records(
                platform_id=platform['id'],
                start_time=start_time
            )

            if records:
                df = pd.DataFrame(records)
                df['record_time'] = pd.to_datetime(df['record_time'])

                df_grouped = df.groupby('record_time')['follower_count'].mean().reset_index()
                df_grouped = df_grouped.sort_values('record_time')

                fig.add_trace(go.Scatter(
                    x=df_grouped['record_time'],
                    y=df_grouped['follower_count'],
                    mode='lines+markers',
                    name=platform['name'],
                    line=dict(width=2),
                    marker=dict(size=6)
                ))

        fig.update_layout(
            title='多平台粉丝量对比',
            xaxis_title='时间',
            yaxis_title='平均粉丝数量',
            hovermode='x unified',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=80),
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        fig.update_xaxes(
            tickformat="%Y-%m-%d %H:%M",
            tickangle=-45
        )

        return fig

    def create_task_status_chart(self) -> go.Figure:
        """
        创建任务状态图表

        Returns:
            Plotly图表对象
        """
        task_logs = self.db.get_task_logs(limit=100)

        if not task_logs:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无任务日志",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        df = pd.DataFrame(task_logs)

        status_counts = df['status'].value_counts()

        colors = {
            'success': '#2ecc71',
            'partial_success': '#f39c12',
            'failed': '#e74c3c',
            'running': '#3498db'
        }

        fig = go.Figure(data=[
            go.Bar(
                x=status_counts.index,
                y=status_counts.values,
                marker=dict(
                    color=[colors.get(status, '#95a5a6') for status in status_counts.index]
                ),
                text=status_counts.values,
                textposition='outside'
            )
        ])

        fig.update_layout(
            title='任务执行状态统计',
            xaxis_title='状态',
            yaxis_title='任务数量',
            template='plotly_white',
            height=400,
            margin=dict(l=20, r=20, t=60, b=80),
            showlegend=False
        )

        return fig

    def create_daily_summary_chart(self, days: int = 7, user_identity: str = None) -> go.Figure:
        """
        创建每日数据汇总图表

        Args:
            days: 显示天数
            user_identity: 用户标识（可选）

        Returns:
            Plotly图表对象
        """
        start_time = datetime.now() - timedelta(days=days)
        records = self.db.get_follower_records(start_time=start_time, user_identity=user_identity, limit=10000)

        if not records:
            fig = go.Figure()
            fig.add_annotation(
                text="暂无数据",
                xref="paper", yref="paper",
                x=0.5, y=0.5,
                showarrow=False,
                font=dict(size=20)
            )
            return fig

        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])
        df['date'] = df['record_time'].dt.date

        daily_summary = df.groupby(['date', 'platform_id']).agg({
            'follower_count': 'mean',
            'id': 'count'
        }).reset_index()
        daily_summary.columns = ['date', 'platform_id', 'avg_followers', 'record_count']

        platforms = self.db.get_all_platforms()
        platform_map = {p['id']: p['name'] for p in platforms}
        daily_summary['platform_name'] = daily_summary['platform_id'].map(platform_map)

        fig = go.Figure()

        for platform_name in daily_summary['platform_name'].unique():
            platform_data = daily_summary[daily_summary['platform_name'] == platform_name]
            fig.add_trace(go.Bar(
                x=platform_data['date'].astype(str),
                y=platform_data['record_count'],
                name=platform_name,
                text=platform_data['record_count'],
                textposition='outside'
            ))

        fig.update_layout(
            title='每日数据采集量统计',
            xaxis_title='日期',
            yaxis_title='记录数量',
            template='plotly_white',
            height=500,
            margin=dict(l=20, r=20, t=60, b=80),
            barmode='group',
            legend=dict(
                orientation="h",
                yanchor="bottom",
                y=1.02,
                xanchor="right",
                x=1
            )
        )

        fig.update_xaxes(tickangle=-45)

        return fig

    def export_chart(self, fig: go.Figure, filename: str, format: str = 'png'):
        """
        导出图表

        Args:
            fig: Plotly图表对象
            filename: 文件名
            format: 导出格式 (png, jpg, svg, pdf)
        """
        import os
        from config import DataConfig

        os.makedirs(DataConfig.PROCESSED_DATA_DIR, exist_ok=True)
        filepath = os.path.join(DataConfig.PROCESSED_DATA_DIR, f"{filename}.{format}")

        if format == 'html':
            fig.write_html(filepath)
        else:
            fig.write_image(filepath)

        return filepath
