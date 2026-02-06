import streamlit as st
import pandas as pd
from datetime import datetime, timedelta
from core.database import Database
from core.visualizer import Visualizer
from config import AppConfig
from utils.logger import Logger
import io


st.set_page_config(
    page_title="数据查看 - 社交媒体监控",
    page_icon="📊",
    layout="wide"
)


@st.cache_resource
def init_page():
    """初始化页面"""
    AppConfig.init()
    db = Database()
    visualizer = Visualizer(db)
    logger = Logger.get_logger("data_view_page")
    return db, visualizer, logger


def render_data_filters(db: Database):
    """渲染数据筛选器"""
    st.sidebar.header("🔍 数据筛选")

    platforms = db.get_all_platforms()
    platform_options = {p['name']: p['id'] for p in platforms}

    selected_platform_name = st.sidebar.selectbox(
        "选择平台",
        ["全部"] + list(platform_options.keys()),
        key="filter_platform"
    )

    selected_platform_id = None
    if selected_platform_name != "全部":
        selected_platform_id = platform_options[selected_platform_name]

    users = db.get_all_users(selected_platform_id)
    user_options = {u['username'] or u['user_id']: u['id'] for u in users}

    selected_user_name = st.sidebar.selectbox(
        "选择用户",
        ["全部"] + list(user_options.keys()),
        key="filter_user"
    )

    selected_user_id = None
    if selected_user_name != "全部":
        selected_user_id = user_options[selected_user_name]

    st.sidebar.markdown("---")

    date_range = st.sidebar.date_input(
        "日期范围",
        value=(
            datetime.now() - timedelta(days=30),
            datetime.now()
        ),
        key="filter_date_range"
    )

    if len(date_range) == 2:
        start_date = datetime.combine(date_range[0], datetime.min.time())
        end_date = datetime.combine(date_range[1], datetime.max.time())
    else:
        start_date = datetime.now() - timedelta(days=30)
        end_date = datetime.now()

    st.sidebar.markdown("---")

    status_filter = st.sidebar.multiselect(
        "状态筛选",
        ["success", "partial_success", "failed"],
        default=["success"],
        key="filter_status"
    )

    return {
        'platform_id': selected_platform_id,
        'user_id': selected_user_id,
        'start_time': start_date,
        'end_time': end_date,
        'status': status_filter
    }


def render_data_table(db: Database, filters: dict):
    """渲染数据表格"""
    st.header("📋 粉丝量数据")

    records = db.get_follower_records(
        platform_id=filters['platform_id'],
        user_id=filters['user_id'],
        start_time=filters['start_time'],
        end_time=filters['end_time'],
        limit=10000
    )

    if not records:
        st.info("暂无符合条件的数据")
        return

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

    df['record_time'] = pd.to_datetime(df['record_time'])
    df['date'] = df['record_time'].dt.date

    if filters['status']:
        df = df[df['status'].isin(filters['status'])]

    display_df = df[[
        'id', 'record_time', 'platform_name', 'user_code', 'user_identity', 'nickname', 'follower_count', 'status'
    ]].copy()
    display_df.columns = ['记录ID', '采集时间', '平台', '用户识别码', '用户标识', '昵称', '粉丝数量', '状态']
    display_df['采集时间'] = display_df['采集时间'].dt.strftime('%Y-%m-%d %H:%M:%S')
    display_df['粉丝数量'] = display_df['粉丝数量'].apply(lambda x: f"{x:,}")

    status_colors = {
        'success': '🟢',
        'partial_success': '🟡',
        'failed': '🔴'
    }
    display_df['状态'] = display_df['状态'].map(lambda x: f"{status_colors.get(x, '⚪')} {x}")

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=400
    )

    st.markdown("---")

    col1 = st.columns(1)[0]

    with col1:
        st.metric("总记录数", len(df))

    st.markdown("---")

    st.subheader("🗑️ 删除数据记录")

    col4, col5, col6 = st.columns([3, 1, 1])

    with col4:
        record_id_input = st.text_input("输入要删除的记录ID", key="delete_record_id", placeholder="输入记录ID")

    with col5:
        if st.button("删除记录", key="delete_record_btn", type="primary", use_container_width=True):
            if record_id_input and record_id_input.isdigit():
                record_id = int(record_id_input)
                if record_id in df['id'].values:
                    record_info = df[df['id'] == record_id].iloc[0]
                    st.session_state.delete_record_confirm = {
                        'record_id': record_id,
                        'nickname': record_info['nickname'],
                        'record_time': record_info['record_time']
                    }
                else:
                    st.error(f"记录ID {record_id} 不存在")
            else:
                st.error("请输入有效的记录ID")

    with col6:
        if st.button("清空输入", key="clear_record_input", use_container_width=True):
            st.session_state.delete_record_id = ""
            if 'delete_record_confirm' in st.session_state:
                del st.session_state.delete_record_confirm
            st.rerun()

    if 'delete_record_confirm' in st.session_state:
        confirm = st.session_state.delete_record_confirm
        st.error(f"⚠️ 确认要删除记录 (ID: {confirm['record_id']}) 吗？")
        st.error(f"⚠️ 用户: {confirm['nickname']}")
        st.error(f"⚠️ 采集时间: {confirm['record_time'].strftime('%Y-%m-%d %H:%M:%S') if isinstance(confirm['record_time'], datetime) else confirm['record_time']}")
        st.error("⚠️ 此操作不可恢复！")

        col_confirm, col_cancel = st.columns(2)

        with col_confirm:
            if st.button("✅ 确认删除", key="confirm_delete_record", type="primary", use_container_width=True):
                if db.delete_follower_record(confirm['record_id']):
                    st.success(f"记录 {confirm['record_id']} 已删除")
                    del st.session_state.delete_record_confirm
                    st.rerun()

        with col_cancel:
            if st.button("❌ 取消删除", key="cancel_delete_record", use_container_width=True):
                del st.session_state.delete_record_confirm
                st.rerun()

    st.warning("⚠️ 注意：删除操作不可恢复，请谨慎操作！")


def render_data_export(db: Database, filters: dict):
    """渲染数据导出功能"""
    st.header("📥 数据导出")

    records = db.get_follower_records(
        platform_id=filters['platform_id'],
        user_id=filters['user_id'],
        start_time=filters['start_time'],
        end_time=filters['end_time'],
        limit=10000
    )

    if not records:
        st.info("暂无数据可导出")
        return

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

    display_df = df[[
        'record_time', 'platform_name', 'user_code', 'user_identity', 'nickname', 'follower_count', 'status'
    ]].copy()
    display_df.columns = ['采集时间', '平台', '用户识别码', '用户标识', '昵称', '粉丝数量', '状态']

    col1, col2, col3 = st.columns(3)

    with col1:
        csv = display_df.to_csv(index=False, encoding='utf-8-sig')
        st.download_button(
            label="📄 导出CSV",
            data=csv,
            file_name=f"粉丝量数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.csv",
            mime="text/csv",
            use_container_width=True
        )

    with col2:
        excel_buffer = io.BytesIO()
        with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
            display_df.to_excel(writer, index=False, sheet_name='粉丝量数据')
        excel_buffer.seek(0)
        st.download_button(
            label="📊 导出Excel",
            data=excel_buffer.getvalue(),
            file_name=f"粉丝量数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx",
            mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            use_container_width=True
        )

    with col3:
        json_data = display_df.to_json(orient='records', force_ascii=False, indent=2)
        st.download_button(
            label="📋 导出JSON",
            data=json_data,
            file_name=f"粉丝量数据_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json",
            mime="application/json",
            use_container_width=True
        )


def render_user_list(db: Database):
    """渲染用户列表"""
    st.header("👥 监控用户列表")

    users = db.get_all_users()

    if not users:
        st.info("暂无监控用户")
        return

    df = pd.DataFrame(users)

    platforms = db.get_all_platforms()
    platform_map = {p['id']: p['name'] for p in platforms}
    df['platform_name'] = df['platform_id'].map(platform_map)
    if 'user_identity' not in df.columns:
        df['user_identity'] = '0'

    display_df = df[[
        'id', 'platform_name', 'user_id', 'username', 'user_identity', 'is_active', 'created_at'
    ]].copy()
    display_df.columns = ['用户ID', '平台', '用户识别码', '昵称', '用户标识', '状态', '添加时间']
    
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
    
    display_df['添加时间'] = display_df['添加时间'].apply(format_datetime)
    display_df['状态'] = display_df['状态'].apply(lambda x: '🟢 启用' if x else '🔴 禁用')

    st.dataframe(
        display_df,
        use_container_width=True,
        hide_index=True,
        height=300
    )

    st.markdown("---")

    st.subheader("🗑️ 删除用户")

    col1, col2, col3, col4 = st.columns([2, 1, 1, 1])

    with col1:
        user_id_input = st.text_input("输入要删除的用户ID", key="delete_user_id", placeholder="输入用户ID")

    with col2:
        delete_records = st.checkbox("同时删除粉丝记录", value=True, key="delete_user_records")

    with col3:
        if st.button("删除用户", key="delete_user_btn", type="primary", use_container_width=True):
            if user_id_input and user_id_input.isdigit():
                user_id = int(user_id_input)
                if user_id in df['id'].values:
                    user_info = df[df['id'] == user_id].iloc[0]
                    st.session_state.delete_user_confirm = {
                        'user_id': user_id,
                        'nickname': user_info['username'] or user_info['user_id'],
                        'delete_records': delete_records
                    }
                else:
                    st.error(f"用户ID {user_id} 不存在")
            else:
                st.error("请输入有效的用户ID")

    with col4:
        if st.button("清空输入", key="clear_user_input", use_container_width=True):
            st.session_state.delete_user_id = ""
            if 'delete_user_confirm' in st.session_state:
                del st.session_state.delete_user_confirm
            st.rerun()

    if 'delete_user_confirm' in st.session_state:
        confirm = st.session_state.delete_user_confirm
        st.error(f"⚠️ 确认要删除用户「{confirm['nickname']}」(ID: {confirm['user_id']}) 吗？")
        if confirm['delete_records']:
            st.error("⚠️ 同时将删除该用户的所有粉丝记录！")
        st.error("⚠️ 此操作不可恢复！")

        col_confirm, col_cancel = st.columns(2)

        with col_confirm:
            if st.button("✅ 确认删除", key="confirm_delete_user", type="primary", use_container_width=True):
                if db.delete_user(confirm['user_id'], delete_records=confirm['delete_records']):
                    st.success(f"用户 {confirm['user_id']} 已删除")
                    del st.session_state.delete_user_confirm
                    st.rerun()

        with col_cancel:
            if st.button("❌ 取消删除", key="cancel_delete_user", use_container_width=True):
                del st.session_state.delete_user_confirm
                st.rerun()

    st.warning("⚠️ 注意：删除操作不可恢复，请谨慎操作！")


def render_platform_summary(db: Database):
    """渲染平台汇总"""
    st.header("📊 平台数据汇总")

    platforms = db.get_all_platforms()

    for platform in platforms:
        with st.expander(f"📱 {platform['name']}", expanded=True):
            users = db.get_all_users(platform['id'])
            records = db.get_follower_records(platform_id=platform['id'], limit=1000)

            if not records:
                st.info(f"{platform['name']} 暂无数据")
                continue

            df = pd.DataFrame(records)
            df['record_time'] = pd.to_datetime(df['record_time'])

            col1, col2, col3, col4 = st.columns(4)

            with col1:
                st.metric("监控用户数", len(users))

            with col2:
                st.metric("数据记录数", len(df))

            with col3:
                latest_records = df.loc[df.groupby('user_id')['record_time'].idxmax()]
                total_followers = latest_records['follower_count'].sum()
                st.metric("总粉丝量", f"{total_followers:,}")

            with col4:
                avg_followers = latest_records['follower_count'].mean()
                st.metric("平均粉丝量", f"{avg_followers:,.0f}")

            st.markdown("---")

            st.subheader(f"{platform['name']} 用户排名")
            user_ranking = latest_records.sort_values('follower_count', ascending=False).head(10)

            users_map = {u['id']: u['username'] or u['user_id'] for u in users}
            user_ranking['user_name'] = user_ranking['user_id'].map(users_map)

            ranking_df = user_ranking[['user_name', 'follower_count']].copy()
            ranking_df.columns = ['用户', '粉丝数量']
            ranking_df['粉丝数量'] = ranking_df['粉丝数量'].apply(lambda x: f"{x:,}")

            st.dataframe(
                ranking_df,
                use_container_width=True,
                hide_index=True,
                height=300
            )


def render_data_statistics(db: Database, filters: dict):
    """渲染数据统计"""
    st.header("📈 数据统计分析")

    records = db.get_follower_records(
        platform_id=filters['platform_id'],
        user_id=filters['user_id'],
        start_time=filters['start_time'],
        end_time=filters['end_time'],
        limit=10000
    )

    if not records:
        st.info("暂无数据")
        return

    df = pd.DataFrame(records)
    df['record_time'] = pd.to_datetime(df['record_time'])
    df['date'] = df['record_time'].dt.date

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("每日采集量")
        daily_count = df.groupby('date').size().reset_index()
        daily_count.columns = ['日期', '记录数']

        st.bar_chart(daily_count.set_index('日期')['记录数'], use_container_width=True)

    with col2:
        st.subheader("粉丝量分布")
        import plotly.express as px
        fig_hist = px.histogram(df, x='follower_count', nbins=20, title='粉丝量分布')
        fig_hist.update_layout(height=300, margin=dict(l=20, r=20, t=40, b=40))
        st.plotly_chart(fig_hist, use_container_width=True)

    st.markdown("---")

    col3, col4 = st.columns(2)

    with col3:
        st.subheader("平台分布")
        platforms = db.get_all_platforms()
        platform_map = {p['id']: p['name'] for p in platforms}
        df['platform_name'] = df['platform_id'].map(platform_map)

        platform_count = df['platform_name'].value_counts()
        st.bar_chart(platform_count, use_container_width=True)

    with col4:
        st.subheader("状态分布")
        status_count = df['status'].value_counts()
        st.bar_chart(status_count, use_container_width=True)


def main():
    """主函数"""
    db, visualizer, logger = init_page()

    st.title("📊 数据查看")
    st.markdown("---")

    filters = render_data_filters(db)

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📋 数据表格",
        "📥 数据导出",
        "👥 用户列表",
        "📊 平台汇总",
        "📈 数据统计"
    ])

    with tab1:
        render_data_table(db, filters)

    with tab2:
        render_data_export(db, filters)

    with tab3:
        render_user_list(db)

    with tab4:
        render_platform_summary(db)

    with tab5:
        render_data_statistics(db, filters)


if __name__ == "__main__":
    main()
