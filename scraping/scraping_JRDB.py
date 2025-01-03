import requests
from requests.exceptions import RequestException
import configparser
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_config():
    config = configparser.ConfigParser()
    config.read('config.ini')
    return config

def login_and_check(config):
    jrdb_username = config['login']['jrdb_username'].strip()
    jrdb_password = config['login']['jrdb_password'].strip()
    login_url = "http://www.jrdb.com/member/n_index.html"
    
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'ja,en-US;q=0.7,en;q=0.3',
    }
    
    try:
        with requests.Session() as session:
            session.auth = (jrdb_username, jrdb_password)
            response = session.get(login_url, headers=headers)
            response.raise_for_status()
            response.encoding = 'utf-8'
            
            if "過去重賞レース結果" in response.text:
                logger.info("ログインに成功しました。")
                return True
            else:
                logger.error("ログインに失敗しました。ユーザー名とパスワードを確認してください。")
                return False
    except RequestException as e:
        logger.error(f"ログイン処理中にエラーが発生しました: {e}")
        return False

if __name__ == "__main__":
    config = load_config()
    login_success = login_and_check(config)
    if login_success:
        logger.info("ログインの動作確認が完了しました。")
    else:
        logger.error("ログインの動作確認に失敗しました。設定を確認してください。")