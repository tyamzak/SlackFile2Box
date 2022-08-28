import json
import logging
import datetime
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from logging import getLogger, StreamHandler, DEBUG
import time
import sys

class SlackModule:

    def __init__(self,):

        with open('config.json','r',encoding='utf8') as f:
            js = json.load(f)
            slack_token = js["SLACK_TOKEN"]
            SLACK_IRAISYO_STR = js["SLACK_IRAISYO_STR"]
            SLACK_FEEDBACK_STR = js["SLACK_FEEDBACK_STR"]
            SLACK_CHANNEL_NAMES = js["SLACK_CHANNEL_NAMES"]
            TIMEOUT = float(js["TIMEOUT"])
        
        logging.basicConfig()
        self.logger = getLogger(__name__)
        self.handler = logging.StreamHandler(sys.stdout)
        self.handler.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.INFO)
        self.slack_client = WebClient(token=slack_token)

    def slack_filelist_for_download(self, channels:list[str],
                                ts_from,
                                ts_to,
                                page:int = 1) -> list:
        """_summary_
            全てのファイルリストの取得
        Args:
            channels (list[str], optional): _description_.
            ts_from (str, optional): _description_.
            ts_to (str, optional): _description_.

        Returns:
            list: _description_
        """
        file_ids = []
        self.slack_client
        
        
        if channels:
            for channel in channels:
                #チャンネルの下で重複チェックを行う
                names_set = set()
                for page in slack_client.files_list(channel=channel,ts_from=ts_from, ts_to=ts_to,page=page):
                    for file in page.get("files", []):
                        info = {
                                'file_id' : file["id"],
                                'channel_id' : file['channels'],
                                "file_name" : file["name"],
                                "user" : file["user"],
                                "download_url" : file["url_private_download"],
                                "timestamp" : file["timestamp"]
                            }
                        #同日付で同名ファイルがあった場合に、リネームする(1).(2)形式
                        #setは日付 + ファイル名形式
                        

                        # #既に存在したら、(1)形式でファイル名に追加
                        i = 0
                        t_delta = datetime.timedelta(hours=9)
                        JST = datetime.timezone(t_delta, 'JST')
                        datestr = datetime.datetime.fromtimestamp(info["timestamp"],tz=JST).strftime(format='%Y%m%d')
                        keystr = info["file_name"]
                        datekeyname = datestr + keystr

                        while True:
                            i += 1
                            if datekeyname in names_set:
                                str = info["file_name"]
                                keystr =str.split('.')[0] + f'({i}).' + str.split('.')[1]
                                datekeyname = datestr + keystr
                            else:
                                info["file_name"] = keystr
                                break
                        names_set.add(datekeyname)
                        file_ids.append(info)
                    #ページがまだあるなら、再帰する
                    paging = page.get("paging")
                    currentpage = paging["page"]
                    totalpage = paging["pages"]
                    if totalpage > currentpage:
                        file_ids.extend(self.slack_filelist_for_download(channels=channels,
                                        ts_from=ts_from,
                                        ts_to=ts_to,
                                        page=int(currentpage) + 1))
                    
        return file_ids
    ###################################全てのファイルリストの取得#################

    
    def get_channel_messages(self, channel_id, ts_to, ts_from) -> list:
    
        global TIMEOUT
        global STARTTIME
        global slack_client
        
        rtnmessages = []


        cursor = None  # シーク位置。最初は None ページを指定して、次からは next_cursor が指し示す位置。

        while True:
            try:
                response = slack_client.conversations_history(  # conversations_history ＝ チャット一覧を得る
                    channel=channel_id,
                    cursor=cursor,  # チャンネルID、latest、シーク位置を指定。
                    latest=ts_to,
                    oldest=ts_from
                    # latestに指定した時間よりも古いメッセージが得られる。latestはUNIX時間で指定する。
                )
            except SlackApiError as e:
                exit

            # response["messages"]が有るか？
            if "messages" in response:  
                #チャンネル情報を追加
                extenditems = []
                for item in response["messages"]:
                    item['channel_id'] = channel_id
                # rtnmessages[-1]['channel_id'] = channel_id
                # extenditems = [item['channel_id'] = channel_id for item in response["messages"]]
                rtnmessages.extend(response["messages"])
                # rtnmessages.extend(extenditems)


            if "has_more" not in response or response["has_more"] is not True:
                # conversations_historyのレスポンスに["has_more"]が無かったり、has_moreの値がFalseだった場合、終了する。
                break
            # conversations_historyのレスポンスに["response_metadata"]["next_cursor"]が有る場合、cursorをセット
            if (
                "response_metadata" in response
                and "next_cursor" in response["response_metadata"]
            ):  
                # （上に戻って、もう一度、conversations_history取得）
                cursor = response["response_metadata"]["next_cursor"]
            else:
                break
            
            #Timeout
            if time.time() - STARTTIME > TIMEOUT:
                self.logger.info("TIMEOUT")
                break
        
        return rtnmessages
        
    #チャンネル名からチャンネルIDリストを作成する########################
    def get_slack_channel_ids_names(self, channel_names:list):
        
        local_slack_ids_names = []
        #publicとprivateの両方を対象にする
        res = self.slack_client.conversations_list(types="public_channel, private_channel")
        if res:
            slack_channel_names = res['channels']


        #名前, id, 作成時のタイムスタンプを取得
        for chname in slack_channel_names:
            if chname['name'] in channel_names and chname["is_channel"]:
                local_slack_ids_names.append([chname['name'],chname['id'],str(chname['created'])])

        return local_slack_ids_names
        
        ################################################################