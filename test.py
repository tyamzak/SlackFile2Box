import logging
logging.basicConfig()
import os
import time

from boxsdk import JWTAuth, Client
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from logging import getLogger, StreamHandler, DEBUG
# チャンネル一覧を取得する
# slack_token = os.environ.get('SLACK_TOKEN')
slack_token = 'xoxp-3593527370466-3578939893527-3712992586016-fefb502525c92fe35d7efeb660a264fa'
logger = getLogger(__name__)
client = WebClient(token=slack_token)

TERM=0
channel_id = 'C03HFFXSGUA'
latest = int(time.time() - TERM)  # 現在日時 - 2週間 の UNIX時間
cursor = None  # シーク位置。最初は None ページを指定して、次からは next_cursor が指し示す位置。


try:
    response = client.conversations_history(  # conversations_history ＝ チャット一覧を得る
        channel=channel_id,
        latest=latest,
        cursor=cursor  # チャンネルID、latest、シーク位置を指定。
        # latestに指定した時間よりも古いメッセージが得られる。latestはUNIX時間で指定する。
    )
except SlackApiError as e:
    exit

print('test')