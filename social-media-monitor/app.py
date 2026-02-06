# 在app.py的最开始添加
import os
import time

# 设置时区为中国时区
os.environ['TZ'] = 'Asia/Shanghai'
time.tzset()  # Unix-like系统有效

# 对于Windows，需要额外的处理
if os.name == 'nt':
    import win32api
    import win32con
    import win32timezone
    # Windows特定的时区设置

import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import Database
from core.scheduler import TaskScheduler
from core.visualizer import Visualizer
from config import AppConfig, StreamlitConfig
from utils.logger import Logger


st.set_page_config(
    page_title=StreamlitConfig.TITLE,
    page_icon=StreamlitConfig.PAGE_ICON,
    layout="wide",
    initial_sidebar_state="expanded"
)


@st.cache_resource
def init_app():
    """初始化应用"""
    AppConfig.init()
    db = Database()
    scheduler = TaskScheduler(db)
    visualizer = Visualizer(db)
    logger = Logger.get_logger("app")
    return db, scheduler, visualizer, logger


def render_header():
    """渲染页面头部"""
    st.markdown("""
        <style>
        .main-header {
            background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
            padding: 2rem;
            border-radius: 10px;
            margin-bottom: 2rem;
            color: white;
        }
        .main-header h1 {
            margin: 0;
            font-size: 2.5rem;
            font-weight: 700;
        }
        .main-header p {
            margin: 0.5rem 0 0 0;
            opacity: 0.9;
        }
        </style>
    """, unsafe_allow_html=True)

    st.markdown("""
        <div class="main-header">
            <h1>📊 社交媒体粉丝量监控平台</h1>
            <p>实时监控微博、小红书、抖音平台粉丝数据变化</p>
        </div>
    """, unsafe_allow_html=True)


def render_scheduler_control(scheduler: TaskScheduler):
    """渲染调度器控制面板"""
    st.subheader("🕐 定时任务管理")

    col1, col2, col3, col4 = st.columns(4)

    with col1:
        if st.button("▶️ 启动调度器", type="primary", use_container_width=True):
            scheduler.start()
            st.success("调度器已启动！")
            st.rerun()

    with col2:
        if st.button("⏸️ 暂停调度器", use_container_width=True):
            scheduler.stop()
            st.success("调度器已暂停！")
            st.rerun()

    with col3:
        if st.button("🔄 立即执行所有任务", type="secondary", use_container_width=True):
            with st.spinner("正在执行所有任务..."):
                scheduler.execute_all_tasks()
            st.success("所有任务执行完成！")
            st.rerun()

    with col4:
        if st.button("📊 查看任务状态", use_container_width=True):
            st.rerun()


def render_task_monitor(scheduler: TaskScheduler, db: Database):
    """渲染任务监控面板"""
    st.subheader("📈 任务监控面板")

    status = scheduler.get_task_status()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.metric(
            "调度器状态",
            "🟢 运行中" if status['is_running'] else "🔴 已停止",
            delta=None
        )

    with col2:
        total_tasks = len(status['tasks'])
        st.metric("总任务数", total_tasks)

    with col3:
        enabled_tasks = sum(1 for t in status['tasks'] if t['is_enabled'])
        st.metric("启用任务", enabled_tasks)

    st.markdown("---")

    for task in status['tasks']:
        with st.expander(f"📋 {task['task_name']}", expanded=True):
            col1, col2, col3, col4 = st.columns(4)

            with col1:
                status_icon = "🟢" if task['is_enabled'] else "🔴"
                st.write(f"{status_icon} **状态**: {'启用' if task['is_enabled'] else '禁用'}")

            with col2:
                st.write(f"⏰ **调度时间**: {task['schedule_time']}")

            with col3:
                last_run = task['last_run_time']
                if last_run:
                    if isinstance(last_run, str):
                        last_run_str = last_run
                    else:
                        last_run_str = last_run.strftime('%Y-%m-%d %H:%M:%S')
                else:
                    last_run_str = "未执行"
                st.write(f"📅 **最后执行**: {last_run_str}")

            with col4:
                st.write(f"🔄 **重试次数**: {task['retry_count']}")

            if task['last_execution_status']:
                status_color = {
                    'success': '🟢',
                    'partial_success': '🟡',
                    'failed': '🔴',
                    'running': '🔵'
                }.get(task['last_execution_status'], '⚪')
                st.write(f"{status_color} **执行状态**: {task['last_execution_status']}")

            col5, col6 = st.columns(2)

            with col5:
                if st.button(f"立即执行", key=f"run_{task['task_name']}", use_container_width=True):
                    with st.spinner(f"正在执行 {task['task_name']}..."):
                        scheduler.run_now(task['task_name'])
                    st.success(f"{task['task_name']} 执行完成！")
                    st.rerun()

            with col6:
                new_time = st.text_input(
                    "修改调度时间",
                    value=task['schedule_time'],
                    key=f"time_{task['task_name']}",
                    max_chars=5
                )
                if st.button("更新时间", key=f"update_{task['task_name']}", use_container_width=True):
                    scheduler.update_task_schedule(task['task_name'], new_time)
                    st.success(f"{task['task_name']} 调度时间已更新为 {new_time}")
                    st.rerun()


def render_recent_records(db: Database):
    """渲染最近记录"""
    st.subheader("📝 最近采集记录")

    records = db.get_follower_records(limit=10)

    if records:
        df = pd.DataFrame(records)

        platforms = db.get_all_platforms()
        platform_map = {p['id']: p['name'] for p in platforms}
        df['platform_name'] = df['platform_id'].map(platform_map)

        users = db.get_all_users()
        user_map = {u['id']: u for u in users}
        df['nickname'] = df['user_id'].map(
            lambda x: user_map.get(x, {}).get('username') or 
                      user_map.get(x, {}).get('user_id', '')
        )
        df['user_code'] = df['user_id'].map(
            lambda x: user_map.get(x, {}).get('user_id', '')
        )
        df['user_identity'] = df['user_id'].map(
            lambda x: user_map.get(x, {}).get('user_identity', '')
        )

        display_df = df[['record_time', 'platform_name', 'user_code', 'user_identity', 'nickname', 'follower_count', 'status']].copy()
        display_df.columns = ['采集时间', '平台', '用户识别码', '用户标识', '昵称', '粉丝数量', '状态']
        
        def format_datetime(dt):
            if pd.isna(dt):
                return ''
            if isinstance(dt, datetime):
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            try:
                parsed = pd.to_datetime(dt, errors='coerce')
                if pd.notna(parsed):
                    return parsed.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
            return str(dt) if dt else ''
        
        display_df['采集时间'] = display_df['采集时间'].apply(format_datetime)
        display_df['粉丝数量'] = display_df['粉丝数量'].apply(lambda x: f"{x:,}")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("暂无采集记录")


def render_task_logs(db: Database):
    """渲染任务日志"""
    st.subheader("📋 任务执行日志")

    task_logs = db.get_task_logs(limit=5)

    if task_logs:
        df = pd.DataFrame(task_logs)

        tasks = db.get_all_tasks()
        task_map = {t['id']: t['task_name'] for t in tasks}
        df['task_name'] = df['task_id'].map(task_map)

        display_df = df[['start_time', 'task_name', 'status', 'records_count', 'success_count', 'failed_count']].copy()
        display_df.columns = ['开始时间', '任务名称', '状态', '总记录', '成功', '失败']
        
        def format_datetime(dt):
            if pd.isna(dt):
                return ''
            if isinstance(dt, datetime):
                return dt.strftime('%Y-%m-%d %H:%M:%S')
            try:
                parsed = pd.to_datetime(dt, errors='coerce')
                if pd.notna(parsed):
                    return parsed.strftime('%Y-%m-%d %H:%M:%S')
            except:
                pass
            return str(dt) if dt else ''
        
        display_df['开始时间'] = display_df['开始时间'].apply(format_datetime)

        status_colors = {
            'success': '🟢',
            'partial_success': '🟡',
            'failed': '🔴',
            'running': '🔵'
        }
        display_df['状态'] = display_df['状态'].map(lambda x: f"{status_colors.get(x, '⚪')} {x}")

        st.dataframe(
            display_df,
            use_container_width=True,
            hide_index=True
        )
    else:
        st.info("暂无任务日志")


def main():
    """主函数"""
    db, scheduler, visualizer, logger = init_app()

    render_header()

    tab1, tab2, tab3 = st.tabs(["🏠 主仪表板", "📊 任务监控", "📈 数据概览"])

    with tab1:
        render_scheduler_control(scheduler)
        st.markdown("---")
        render_recent_records(db)

    with tab2:
        render_task_monitor(scheduler, db)
        st.markdown("---")
        render_task_logs(db)

    with tab3:
        st.subheader("📊 数据概览")

        days = st.slider("显示天数", 1, 90, 30)
        fig_platform = visualizer.create_platform_comparison_chart(days=days)
        st.plotly_chart(fig_platform, use_container_width=True)


if __name__ == "__main__":
    main()
