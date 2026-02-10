
import requests
import os
from dotenv import load_dotenv
from app.logger import logger

load_dotenv()

# 从环境变量读取 Server酱 Key (兼容 config.ini 的情况，但这里我们优先用 .env)
# 你可以在 .env 中添加 SERVERCHAN_SENDKEY=xxxx
SERVERCHAN_SENDKEY = os.getenv('SERVERCHAN_SENDKEY')
SERVERCHAN_API = "https://sctapi.ftqq.com/{SERVERCHAN_SENDKEY}.send"

def send_server_chan_notification(title: str, content: str):
    """
    发送 Server酱 通知

    Args:
        title: 标题
        content: 内容 (支持 Markdown)
    """
    if not SERVERCHAN_SENDKEY:
        logger.warning("未配置 SERVERCHAN_SENDKEY，跳过发送通知")
        return

    url = SERVERCHAN_API.format(SERVERCHAN_SENDKEY=SERVERCHAN_SENDKEY)
    data = {
        "title": title,
        "desp": content
    }

    try:
        # 使用简单的 requests，不依赖复杂的 Session/Proxy 配置
        # 如果需要代理，可以读取环境变量
        proxies = None
        http_proxy = os.getenv('HTTP_PROXY')
        https_proxy = os.getenv('HTTPS_PROXY')
        if http_proxy or https_proxy:
            proxies = {
                "http": http_proxy,
                "https": https_proxy
            }

        response = requests.post(url, data=data, proxies=proxies, timeout=10)
        response.raise_for_status()
        logger.info(f"通知已发送: {title}")
    except Exception as e:
        logger.error(f"发送通知失败: {e}")
