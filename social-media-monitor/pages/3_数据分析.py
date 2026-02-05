import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from core.database import Database
from core.visualizer import Visualizer
from config import AppConfig
from utils.logger import Logger


st.set_page_config(
    page_title="数据分析 - 社交媒体监控",
    page_icon="📈",
    layout="wide",
    initial_sidebar_state="expanded"
)


def init_page():
    """初始化页面"""
    AppConfig.init()
    db = Database()
    visualizer = Visualizer(db)
    logger = Logger.get_logger("analysis_page")
    return db, visualizer, logger


def render_analysis_filters(db: Database):
    """渲染分析筛选器"""
    st.sidebar.header("🔍 分析参数")

    platforms = db.get_all_platforms()
    platform_options = {p['name']: p['id'] for p in platforms}

    selected_platform_names = st.sidebar.multiselect(
        "选择平台（可多选）",
        list(platform_options.keys()),
        default=list(platform_options.keys()),
        key="analysis_platform"
    )

    selected_platform_ids = [platform_options[name] for name in selected_platform_names] if selected_platform_names else None

    st.sidebar.markdown("---")

    users = db.get_all_users()
    user_identity_options = list(set([u['user_identity'] for u in users if u['user_identity']]))
    user_identity_options.sort()

    selected_user_identity = st.sidebar.selectbox(
        "选择用户标识",
        ["全部"] + user_identity_options,
        key="analysis_user_identity"
    )

    selected_user_id = None
    selected_username = None
    if selected_user_identity != "全部":
        user = next((u for u in users if u['user_identity'] == selected_user_identity), None)
        if user:
            selected_username = user['username'] or user['user_id']

    st.sidebar.markdown("---")

    days = st.sidebar.slider(
        "分析时间范围（天）",
        1, 90, 30,
        key="analysis_days"
    )

    st.sidebar.markdown("---")

    chart_type = st.sidebar.selectbox(
        "图表类型",
        ["趋势图", "增长率图", "每日汇总", "任务状态"],
        key="chart_type"
    )

    return {
        'platform_ids': selected_platform_ids,
        'user_id': selected_user_id,
        'user_identity': selected_user_identity if selected_user_identity != "全部" else None,
        'username': selected_username,
        'days': days,
        'chart_type': chart_type
    }


def render_trend_analysis(db: Database, visualizer: Visualizer, filters: dict):
    """渲染趋势分析"""
    title = "📈 粉丝量趋势分析"
    if filters.get('username'):
        title += f" - {filters['username']}"
    st.header(title)

    info_col1, info_col2, info_col3 = st.columns(3)
    
    with info_col1:
        if filters.get('platform_ids') and len(filters.get('platform_ids')) > 1:
            st.info(f"已选择 {len(filters.get('platform_ids'))} 个平台")
        elif filters.get('platform_ids'):
            platforms = db.get_all_platforms()
            platform_name = next((p['name'] for p in platforms if p['id'] == filters.get('platform_ids')[0]), '未知')
            st.info(f"平台: {platform_name}")
        else:
            st.warning("请选择平台")
    
    with info_col2:
        if filters.get('user_identity'):
            st.info(f"用户标识: {filters.get('user_identity')}")
        else:
            st.info("显示所有用户")
    
    with info_col3:
        st.info(f"时间范围: {filters['days']} 天")

    st.markdown("---")

    col1, col2 = st.columns(2)

    with col1:
        show_markers = st.checkbox("显示数据点", value=True, key="trend_show_markers")

    with col2:
        show_legend = st.checkbox("显示图例", value=True, key="trend_show_legend")

    fig = visualizer.create_trend_chart(
        user_id=filters.get('user_id'),
        platform_ids=filters.get('platform_ids'),
        user_identity=filters.get('user_identity'),
        days=filters['days']
    )

    if show_markers:
        fig.update_traces(mode='lines+markers')
    else:
        fig.update_traces(mode='lines')

    if not show_legend:
        fig.update_layout(showlegend=False)

    fig.update_layout(
        hovermode='x unified',
        hoverlabel=dict(
            bgcolor="rgba(255,255,255,0.9)",
            bordercolor="#1f77b4",
            font_size=12
        )
    )

    st.plotly_chart(fig, use_container_width=True, height=600)


def render_comparison_analysis(db: Database, visualizer: Visualizer, filters: dict):
    """渲染对比分析"""
    title = "📊 粉丝量对比分析"
    if filters.get('username'):
        title += f" - {filters['username']}"
    st.header(title)

    col1, col2 = st.columns(2)

    with col1:
        sort_order = st.selectbox(
            "排序方式",
            ["粉丝数量降序", "粉丝数量升序", "用户ID"],
            key="comparison_sort"
        )

    with col2:
        show_top = st.slider(
            "显示用户数",
            5, 50, 10,
            key="comparison_top"
        )

    fig = visualizer.create_comparison_chart(
        platform_id=filters.get('platform_id'),
        user_identity=filters.get('user_identity'),
        days=filters['days']
    )

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    records = db.get_follower_records(
        platform_id=filters.get('platform_id'),
        user_identity=filters.get('user_identity'),
        start_time=datetime.now() - timedelta(days=filters['days']),
        limit=10000
    )

    if records:
        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])

        platform_id = filters.get('platform_id')
        if platform_id is None and filters.get('platform_ids'):
            platform_id = filters['platform_ids'][0] if len(filters['platform_ids']) == 1 else None
        
        users = db.get_all_users(platform_id)
        user_map = {u['id']: u for u in users}
        latest_records = df.loc[df.groupby('user_id')['record_time'].idxmax()]
        latest_records['user_name'] = latest_records['user_id'].map(
            lambda x: user_map.get(x, {}).get('username') or 
                      user_map.get(x, {}).get('user_id', '')
        )
        latest_records['user_code'] = latest_records['user_id'].map(
            lambda x: user_map.get(x, {}).get('user_id', '')
        )
        latest_records['user_identity'] = latest_records['user_id'].map(
            lambda x: user_map.get(x, {}).get('user_identity', '')
        )

        if sort_order == "粉丝数量降序":
            latest_records = latest_records.sort_values('follower_count', ascending=False)
        elif sort_order == "粉丝数量升序":
            latest_records = latest_records.sort_values('follower_count', ascending=True)
        else:
            latest_records = latest_records.sort_values('user_id')

        display_df = latest_records.head(show_top)[['user_code', 'user_identity', 'user_name', 'follower_count']].copy()
        display_df.columns = ['用户识别码', '用户标识', '昵称', '粉丝数量']
        display_df['粉丝数量'] = display_df['粉丝数量'].apply(lambda x: f"{x:,}")

        st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_distribution_analysis(db: Database, visualizer: Visualizer, filters: dict):
    """渲染分布分析"""
    title = "📉 粉丝量分布分析"
    if filters.get('username'):
        title += f" - {filters['username']}"
    st.header(title)

    col1, col2 = st.columns(2)

    with col1:
        chart_style = st.selectbox(
            "图表样式",
            ["饼图", "柱状图"],
            key="distribution_style"
        )

    with col2:
        show_labels = st.checkbox("显示标签", value=True, key="distribution_labels")

    fig = visualizer.create_distribution_chart(
        platform_id=filters.get('platform_id'),
        user_identity=filters.get('user_identity')
    )

    if chart_style == "柱状图":
        fig.update_traces(type='bar')
        fig.update_layout(showlegend=False)

    if not show_labels:
        fig.update_traces(textinfo='none')

    st.plotly_chart(fig, use_container_width=True)


def render_growth_analysis(db: Database, visualizer: Visualizer, filters: dict):
    """渲染增长率分析"""
    title = "📈 增长率分析"
    if filters.get('username'):
        title += f" - {filters['username']}"
    st.header(title)

    col1, col2 = st.columns(2)

    with col1:
        show_zero_line = st.checkbox("显示零增长线", value=True, key="growth_zero_line")

    with col2:
        smoothing = st.slider(
            "平滑度（移动平均）",
            1, 10, 1,
            key="growth_smoothing"
        )

    fig = visualizer.create_growth_rate_chart(
        user_id=filters.get('user_id'),
        platform_ids=filters.get('platform_ids'),
        user_identity=filters.get('user_identity'),
        days=filters['days']
    )

    if not show_zero_line:
        fig.update_layout(shapes=[])

    st.plotly_chart(fig, use_container_width=True)


def render_platform_comparison(db: Database, visualizer: Visualizer, filters: dict):
    """渲染多平台对比"""
    st.header("🔄 多平台对比分析")

    col1, col2 = st.columns(2)

    with col1:
        show_avg = st.checkbox("显示平均值", value=True, key="platform_avg")

    with col2:
        show_markers = st.checkbox("显示数据点", value=True, key="platform_markers")

    fig = visualizer.create_platform_comparison_chart(days=filters['days'])

    if not show_avg:
        fig.update_traces()

    if show_markers:
        fig.update_traces(mode='lines+markers')
    else:
        fig.update_traces(mode='lines')

    st.plotly_chart(fig, use_container_width=True)


def render_daily_summary(db: Database, visualizer: Visualizer, filters: dict):
    """渲染每日汇总"""
    title = "📅 每日数据汇总"
    if filters.get('username'):
        title += f" - {filters['username']}"
    st.header(title)

    col1, col2 = st.columns(2)

    with col1:
        chart_type = st.selectbox(
            "图表类型",
            ["柱状图", "折线图"],
            key="daily_chart_type"
        )

    with col2:
        pass

    fig = visualizer.create_daily_summary_chart(
        days=filters['days'],
        user_identity=filters.get('user_identity')
    )

    if chart_type == "折线图":
        fig.update_traces(type='scatter', mode='lines+markers')

    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    records = db.get_follower_records(
        platform_id=filters.get('platform_id'),
        user_identity=filters.get('user_identity'),
        start_time=datetime.now() - timedelta(days=filters['days']),
        limit=10000
    )

    if records:
        df = pd.DataFrame(records)
        df['record_time'] = pd.to_datetime(df['record_time'])
        df['date'] = df['record_time'].dt.date

        daily_summary = df.groupby('date').agg({
            'follower_count': ['mean', 'min', 'max', 'count']
        }).reset_index()
        daily_summary.columns = ['日期', '平均粉丝', '最小粉丝', '最大粉丝', '记录数']

        st.dataframe(daily_summary, use_container_width=True, hide_index=True)


def render_task_status_analysis(db: Database, visualizer: Visualizer):
    """渲染任务状态分析"""
    st.header("📋 任务执行状态分析")

    col1, col2 = st.columns(2)

    with col1:
        show_details = st.checkbox("显示详细信息", value=False, key="task_details")

    with col2:
        limit_logs = st.slider(
            "显示日志数量",
            10, 100, 50,
            key="task_log_limit"
        )

    fig = visualizer.create_task_status_chart()
    st.plotly_chart(fig, use_container_width=True)

    st.markdown("---")

    task_logs = db.get_task_logs(limit=limit_logs)

    if task_logs:
        df = pd.DataFrame(task_logs)

        tasks = db.get_all_tasks()
        task_map = {t['id']: t['task_name'] for t in tasks}
        df['task_name'] = df['task_id'].map(task_map)

        df['start_time'] = pd.to_datetime(df['start_time'])
        df['end_time'] = pd.to_datetime(df['end_time'])
        df['duration'] = (df['end_time'] - df['start_time']).dt.total_seconds()

        display_df = df[[
            'start_time', 'task_name', 'status', 'records_count',
            'success_count', 'failed_count', 'duration'
        ]].copy()
        display_df.columns = ['开始时间', '任务名称', '状态', '总记录', '成功', '失败', '耗时(秒)']
        display_df['开始时间'] = display_df['开始时间'].dt.strftime('%Y-%m-%d %H:%M:%S')

        if show_details:
            st.dataframe(display_df, use_container_width=True, hide_index=True)
        else:
            summary_df = display_df.groupby('任务名称').agg({
                '总记录': 'sum',
                '成功': 'sum',
                '失败': 'sum',
                '耗时(秒)': 'mean'
            }).reset_index()
            st.dataframe(summary_df, use_container_width=True, hide_index=True)


def render_chart_export(visualizer: Visualizer, fig, chart_name: str):
    """渲染图表导出功能"""
    st.markdown("---")
    st.subheader("📥 图表导出")

    col1, col2, col3 = st.columns(3)

    with col1:
        if st.button("📷 导出PNG", key=f"export_png_{chart_name}"):
            try:
                filepath = visualizer.export_chart(fig, chart_name, 'png')
                st.success(f"图表已导出: {filepath}")
            except Exception as e:
                st.error(f"导出失败: {str(e)}")

    with col2:
        if st.button("📄 导出SVG", key=f"export_svg_{chart_name}"):
            try:
                filepath = visualizer.export_chart(fig, chart_name, 'svg')
                st.success(f"图表已导出: {filepath}")
            except Exception as e:
                st.error(f"导出失败: {str(e)}")

    with col3:
        if st.button("🌐 导出HTML", key=f"export_html_{chart_name}"):
            try:
                filepath = visualizer.export_chart(fig, chart_name, 'html')
                st.success(f"图表已导出: {filepath}")
            except Exception as e:
                st.error(f"导出失败: {str(e)}")


def main():
    """主函数"""
    db, visualizer, logger = init_page()

    st.title("📈 数据分析")
    st.markdown("---")

    filters = render_analysis_filters(db)

    if filters['chart_type'] == "趋势图":
        render_trend_analysis(db, visualizer, filters)

    elif filters['chart_type'] == "增长率图":
        render_growth_analysis(db, visualizer, filters)

    elif filters['chart_type'] == "每日汇总":
        render_daily_summary(db, visualizer, filters)

    elif filters['chart_type'] == "任务状态":
        render_task_status_analysis(db, visualizer)

    st.info("💡 提示：使用左侧边栏调整分析参数，图表会自动更新。")


if __name__ == "__main__":
    main()
