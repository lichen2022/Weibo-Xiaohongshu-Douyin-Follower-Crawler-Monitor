import streamlit as st
import pandas as pd
from datetime import datetime
from core.database import Database
from core.scheduler import TaskScheduler
from core.crawlers.weibo_crawler import WeiboCrawler
from core.crawlers.xiaohongshu_crawler import XiaohongshuCrawler
from core.crawlers.douyin_crawler import DouyinCrawler
from core.cookie_database import CookieDatabase
from config import AppConfig, WeiboConfig, XiaohongshuConfig, DouyinConfig
from utils.logger import Logger


st.set_page_config(
    page_title="数据爬取 - 社交媒体监控",
    page_icon="🔍",
    layout="wide"
)


def init_page():
    """初始化页面"""
    AppConfig.init()
    db = Database()
    scheduler = TaskScheduler(db)
    cookie_db = CookieDatabase()
    logger = Logger.get_logger("crawler_page")
    return db, scheduler, cookie_db, logger


def render_cookie_management(cookie_db: CookieDatabase):
    """渲染Cookie管理模块"""
    st.header("🍪 Cookie管理")

    cookies = cookie_db.get_all_cookies()

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("微博Cookie")
        weibo_cookie = st.text_area(
            "输入微博Cookie",
            value=cookies.get('weibo', ''),
            height=100,
            key="weibo_cookie_input"
        )
        if st.button("保存微博Cookie", key="save_weibo_cookie"):
            if cookie_db.save_cookie('weibo', weibo_cookie):
                st.success("微博Cookie已保存！")
            else:
                st.error("保存失败，请重试")

    with col2:
        st.subheader("小红书Cookie")
        xhs_cookie = st.text_area(
            "输入小红书Cookie",
            value=cookies.get('xiaohongshu', ''),
            height=100,
            key="xhs_cookie_input"
        )
        if st.button("保存小红书Cookie", key="save_xhs_cookie"):
            if cookie_db.save_cookie('xiaohongshu', xhs_cookie):
                st.success("小红书Cookie已保存！")
            else:
                st.error("保存失败，请重试")

    with col3:
        st.subheader("抖音Cookie")
        douyin_cookie = st.text_area(
            "输入抖音Cookie",
            value=cookies.get('douyin', ''),
            height=100,
            key="douyin_cookie_input"
        )
        if st.button("保存抖音Cookie", key="save_douyin_cookie"):
            if cookie_db.save_cookie('douyin', douyin_cookie):
                st.success("抖音Cookie已保存！")
            else:
                st.error("保存失败，请重试")

    st.markdown("---")
    st.info("💡 提示：Cookie用于身份验证，请定期更新以确保爬取功能正常。")


def render_target_configuration(db: Database):
    """渲染目标配置界面"""
    st.header("🎯 目标配置")

    st.subheader("用户标识设置")
    st.info("💡 提示：输入用户标识后，可以为该用户添加多个平台的账号，系统会自动将所有平台的数据关联到同一用户")

    user_identity = st.text_input(
        "用户标识",
        value="",
        placeholder="例如：user001",
        key="user_identity_input",
        help="用于跨平台关联同一用户的不同账号"
    )

    st.markdown("---")

    platforms = db.get_all_platforms()
    platform_options = {p['name']: p['id'] for p in platforms}

    selected_platform_name = st.selectbox("选择平台", list(platform_options.keys()))
    selected_platform_id = platform_options[selected_platform_name]

    st.subheader(f"配置{selected_platform_name}目标")

    if selected_platform_name == "微博":
        uid_list = st.text_area(
            "输入微博用户ID（每行一个）",
            value="\n".join(WeiboConfig.UID_LIST),
            height=150,
            key="weibo_uid_list"
        )
        uid_list = [uid.strip() for uid in uid_list.split("\n") if uid.strip()]

        if st.button("保存微博用户列表", key="save_weibo_users"):
            for uid in uid_list:
                existing_user = db.get_user_by_platform_and_id(selected_platform_id, uid)
                if existing_user:
                    db.update_user_identity(selected_platform_id, uid, user_identity)
                else:
                    db.insert_user(selected_platform_id, uid, user_identity=user_identity)
            st.success(f"已保存 {len(uid_list)} 个微博用户！")

    elif selected_platform_name == "小红书":
        url_list = st.text_area(
            "输入小红书博主URL（每行一个）",
            value="\n".join(XiaohongshuConfig.URL_LIST),
            height=150,
            key="xhs_url_list"
        )
        url_list = [url.strip() for url in url_list.split("\n") if url.strip()]

        if st.button("保存小红书博主列表", key="save_xhs_users"):
            for url in url_list:
                user_id = url.split("/")[-1].split("?")[0]
                existing_user = db.get_user_by_platform_and_id(selected_platform_id, user_id)
                if existing_user:
                    db.update_user_identity(selected_platform_id, user_id, user_identity)
                else:
                    db.insert_user(selected_platform_id, user_id, user_identity=user_identity)
            st.success(f"已保存 {len(url_list)} 个小红书博主！")

    elif selected_platform_name == "抖音":
        sec_user_id_list = st.text_area(
            "输入抖音sec_user_id（每行一个）",
            value="\n".join(DouyinConfig.SEC_USER_ID_LIST),
            height=150,
            key="douyin_sec_user_id_list"
        )
        sec_user_id_list = [uid.strip() for uid in sec_user_id_list.split("\n") if uid.strip()]

        if st.button("保存抖音博主列表", key="save_douyin_users"):
            for uid in sec_user_id_list:
                existing_user = db.get_user_by_platform_and_id(selected_platform_id, uid)
                if existing_user:
                    db.update_user_identity(selected_platform_id, uid, user_identity)
                else:
                    db.insert_user(selected_platform_id, uid, user_identity=user_identity)
            st.success(f"已保存 {len(sec_user_id_list)} 个抖音博主！")

    st.markdown("---")

    users = db.get_all_users(selected_platform_id)
    if users:
        st.subheader(f"已配置的{selected_platform_name}用户")
        user_df = pd.DataFrame(users)
        if 'user_identity' not in user_df.columns:
            user_df['user_identity'] = '0'
        display_df = user_df[['user_id', 'username', 'user_identity', 'created_at']].copy()
        display_df.columns = ['用户识别码', '昵称', '用户标识', '添加时间']
        
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
        st.dataframe(display_df, use_container_width=True, hide_index=True)


def render_manual_crawl(db: Database, logger: Logger):
    """渲染手动爬取界面"""
    st.header("🔄 手动爬取")

    st.subheader("用户标识设置")
    st.info("💡 提示：输入用户标识后，手动爬取的数据会关联到该用户标识")
    manual_user_identity = st.text_input(
        "用户标识",
        value="",
        placeholder="例如：user001",
        key="manual_user_identity_input",
        help="用于跨平台关联同一用户的不同账号"
    )

    st.markdown("---")

    col1, col2, col3 = st.columns(3)

    with col1:
        st.subheader("微博")
        uid = st.text_input("输入微博用户ID", key="manual_weibo_uid")
        if st.button("立即爬取", key="crawl_weibo", type="primary"):
            if uid:
                with st.spinner(f"正在爬取微博用户 {uid}..."):
                    try:
                        crawler = WeiboCrawler()
                        user_info = crawler.get_user_info(uid)
                        if user_info:
                            platform = db.get_platform_by_code('weibo')
                            user_db_id = db.insert_user(
                                platform_id=platform['id'],
                                user_id=uid,
                                username=user_info.get('screen_name', ''),
                                user_identity=manual_user_identity
                            )
                            db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=manual_user_identity,
                                follower_count=user_info.get('follower_count', 0)
                            )
                            st.success(f"爬取成功！粉丝数: {user_info.get('follower_count', 0):,}")
                            logger.info(f"手动爬取微博用户 {uid} 成功")
                        else:
                            st.error("爬取失败，请检查用户ID或Cookie")
                    except Exception as e:
                        st.error(f"爬取异常: {str(e)}")
                        logger.error(f"手动爬取微博用户 {uid} 失败: {e}", exc_info=True)
            else:
                st.warning("请输入用户ID")

    with col2:
        st.subheader("小红书")
        url = st.text_input("输入小红书博主URL", key="manual_xhs_url")
        if st.button("立即爬取", key="crawl_xhs", type="primary"):
            if url:
                with st.spinner(f"正在爬取小红书博主 {url}..."):
                    try:
                        crawler = XiaohongshuCrawler()
                        user_info = crawler.get_user_info(url)
                        if user_info:
                            platform = db.get_platform_by_code('xiaohongshu')
                            user_db_id = db.insert_user(
                                platform_id=platform['id'],
                                user_id=user_info.get('user_id', ''),
                                username=user_info.get('nickname', ''),
                                user_identity=manual_user_identity
                            )
                            db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=manual_user_identity,
                                follower_count=user_info.get('follower_count', 0)
                            )
                            st.success(f"爬取成功！粉丝数: {user_info.get('follower_count', 0):,}")
                            logger.info(f"手动爬取小红书博主 {url} 成功")
                        else:
                            st.error("爬取失败，请检查URL或Cookie")
                    except Exception as e:
                        st.error(f"爬取异常: {str(e)}")
                        logger.error(f"手动爬取小红书博主 {url} 失败: {e}", exc_info=True)
            else:
                st.warning("请输入博主URL")

    with col3:
        st.subheader("抖音")
        sec_user_id = st.text_input("输入抖音sec_user_id", key="manual_douyin_uid")
        if st.button("立即爬取", key="crawl_douyin", type="primary"):
            if sec_user_id:
                with st.spinner(f"正在爬取抖音博主 {sec_user_id}..."):
                    try:
                        crawler = DouyinCrawler()
                        user_info = crawler.get_user_info(sec_user_id)
                        if user_info:
                            platform = db.get_platform_by_code('douyin')
                            user_db_id = db.insert_user(
                                platform_id=platform['id'],
                                user_id=sec_user_id,
                                username=user_info.get('nickname', ''),
                                user_identity=manual_user_identity
                            )
                            db.insert_follower_record(
                                user_id=user_db_id,
                                platform_id=platform['id'],
                                user_identity=manual_user_identity,
                                follower_count=user_info.get('follower_count', 0)
                            )
                            st.success(f"爬取成功！粉丝数: {user_info.get('follower_count', 0):,}")
                            logger.info(f"手动爬取抖音博主 {sec_user_id} 成功")
                        else:
                            st.error("爬取失败，请检查sec_user_id或Cookie")
                    except Exception as e:
                        st.error(f"爬取异常: {str(e)}")
                        logger.error(f"手动爬取抖音博主 {sec_user_id} 失败: {e}", exc_info=True)
            else:
                st.warning("请输入sec_user_id")


def render_batch_crawl(db: Database, scheduler: TaskScheduler, logger: Logger):
    """渲染批量爬取界面"""
    st.header("📦 批量爬取")

    platforms = db.get_all_platforms()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("选择平台")
        selected_platforms = []
        for platform in platforms:
            if st.checkbox(platform['name'], key=f"batch_{platform['code']}"):
                selected_platforms.append(platform)

    with col2:
        st.subheader("爬取选项")
        delay = st.slider("请求延迟（秒）", 1, 10, 2)
        retry = st.number_input("失败重试次数", 0, 5, 3)

    st.markdown("---")

    if st.button("🚀 开始批量爬取", type="primary", use_container_width=True):
        if selected_platforms:
            with st.spinner("正在批量爬取..."):
                success_count = 0
                failed_count = 0

                for platform in selected_platforms:
                    task_name = f"{platform['code']}_follower_crawler"
                    try:
                        scheduler.run_now(task_name)
                        success_count += 1
                        logger.info(f"批量爬取 {platform['name']} 成功")
                    except Exception as e:
                        failed_count += 1
                        logger.error(f"批量爬取 {platform['name']} 失败: {e}", exc_info=True)

                st.success(f"批量爬取完成！成功: {success_count}, 失败: {failed_count}")
        else:
            st.warning("请至少选择一个平台")


def render_crawl_settings():
    """渲染爬取设置"""
    st.header("⚙️ 爬取设置")

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("通用设置")
        timeout = st.slider("请求超时时间（秒）", 10, 60, 30)
        max_retries = st.number_input("最大重试次数", 0, 10, 3)
        user_agent = st.text_input(
            "User-Agent",
            value="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
            key="user_agent_setting"
        )

    with col2:
        st.subheader("高级设置")
        use_proxy = st.checkbox("使用代理", key="use_proxy")
        if use_proxy:
            proxy_url = st.text_input("代理地址", key="proxy_url")
        verify_ssl = st.checkbox("验证SSL证书", value=True, key="verify_ssl")
        follow_redirects = st.checkbox("跟随重定向", value=True, key="follow_redirects")

    if st.button("保存设置", use_container_width=True):
        st.success("设置已保存！")


def main():
    """主函数"""
    db, scheduler, cookie_db, logger = init_page()

    st.title("🔍 数据爬取")
    st.markdown("---")

    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "🍪 Cookie管理",
        "🎯 目标配置",
        "🔄 手动爬取",
        "📦 批量爬取",
        "⚙️ 爬取设置"
    ])

    with tab1:
        render_cookie_management(cookie_db)

    with tab2:
        render_target_configuration(db)

    with tab3:
        render_manual_crawl(db, logger)

    with tab4:
        render_batch_crawl(db, scheduler, logger)

    with tab5:
        render_crawl_settings()


if __name__ == "__main__":
    main()
