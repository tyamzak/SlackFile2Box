import logging
import os
import time
import pprint
import sys
from boxsdk import JWTAuth, Client
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError
from logging import getLogger, StreamHandler, DEBUG
import datetime
import json
STARTTIME = time.time()


with open('config.json','r',encoding='utf8') as f:
    js = json.load(f)
    slack_token = js["SLACK_TOKEN"]
    SLACK_IRAISYO_STR = js["SLACK_IRAISYO_STR"]
    SLACK_FEEDBACK_STR = js["SLACK_FEEDBACK_STR"]
    BOX_USER_ID = js["BOX_USER_ID"]
    SLACK_CHANNEL_NAMES = js["SLACK_CHANNEL_NAMES"]
    TIMEOUT = float(js["TIMEOUT"])

def get_tmp_folder():
    SAVEFOLDER = "/tmp"
    import platform
    pf = platform.system()
    if pf == 'Windows':
        if not os.path.exists("save_folder"):
            os.mkdir("save_folder")
        SAVEFOLDER = 'save_folder'
    elif pf == 'Darwin':
        SAVEFOLDER = "/tmp"
    elif pf == 'Linux':
        SAVEFOLDER = "/tmp"
    return SAVEFOLDER

if not TIMEOUT:
    TIMEOUT = 450
else:
    TIMEOUT = int(TIMEOUT)
    
logging.basicConfig()
logger = getLogger(__name__)
handler = logging.StreamHandler(sys.stdout)
handler.setLevel(logging.INFO)
logger.addHandler(handler)
logger.setLevel(logging.INFO)



slack_client = WebClient(token=slack_token)

SAVEFOLDER = get_tmp_folder()
t_delta = datetime.timedelta(hours=9)
JST = datetime.timezone(t_delta, 'JST')
now = datetime.datetime.now(JST)
TS_TODAY = datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST).timestamp()

TS_YESTERDAY = datetime.datetime(now.year,now.month,now.day - 1,0,0,0,tzinfo=JST).timestamp()
#デフォルトは昨日分アップロードなので、昨日分のフォルダを作る
DATEFOLDERNAME = datetime.datetime(now.year,now.month,now.day - 1,0,0,0,tzinfo=JST).strftime('%Y%m%d')
ROOT_FOLDER_NAME = 'SlackUpload'

# グローバル　Dict構造　フォルダ名 : {"id":フォルダID, "items" : [] } "itemsの下層に別フォルダが入る"
box_items = dict()

################BOXJWTクライアントを作成する#########################################jwt
auth = JWTAuth.from_settings_file(r'904005637_ry1r2xel_config.json')

client = Client(auth)
service_account = client.user().get()
logger.info('Box Service Account user ID is {0}'.format(service_account.id))
#別のユーザーとして処理を実行する
user_to_impersonate = client.user(user_id=BOX_USER_ID)
user_client = client.as_user(user_to_impersonate)

##################################################################################

###################################全てのファイルリストの取得

def slack_filelist_for_download(channels:list[str]=[],
                            ts_from = 0,
                            ts_to = TS_TODAY,
                            page:int = 1) -> list:
    """_summary_

    Args:
        channels (list[str], optional): _description_. Defaults to [].
        ts_from (str, optional): _description_. Defaults to 0.
        ts_to (str, optional): _description_. Defaults to TS_TODAY.

    Returns:
        list: _description_
    """
    file_ids = []
    global slack_client
    
    
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
                    #setはチャンネル + 日付 + ファイル名形式
                    

                    # #既に存在したら、(1)形式でファイル名に追加
                    i = 0
                    channelstr = info['channel_id'][0]
                    datestr = datetime.datetime.fromtimestamp(info["timestamp"],tz=JST).strftime(format='%Y%m%d')
                    keystr = info["file_name"]
                    datekeyname = channelstr + datestr + keystr

                    while True:
                        i += 1
                        if datekeyname in names_set:
                            str = info["file_name"]
                            keystr =str.split('.')[0] + f'({i}).' + str.split('.')[1]
                            datekeyname = channelstr + datestr + keystr
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
                    file_ids.extend(slack_filelist_for_download(channels=channels,
                                    ts_from=ts_from,
                                    ts_to=ts_to,
                                    page=int(currentpage) + 1))
                
    return file_ids
###################################全てのファイルリストの取得#################



def find_and_create_folder(parent_folder_id:str, child_name:str) -> str:

    """_summary_
    フォルダの作成(親フォルダid:str,子フォルダ名:str) -> str
    特定の名前のフォルダがあるかどうかの確認
    なかったらフォルダを作成する
    folder idを返す

    Args:
        parent_folder_id (str): _Parent's Box folder id_
        child_name (str): _Child's Box folder name_

    Returns:
        str: folder id
    """

    items = user_client.folder(folder_id=parent_folder_id).get_items()
    for item in items:
        if (item.name == child_name) and (item.type == "folder"):
            print(f'{item.type.capitalize()} {item.id} named "{item.name} was found"')
            return item.id

    #フォルダが無かった場合
    subfolder = user_client.folder(parent_folder_id).create_subfolder(child_name)
    print(f'Created subfolder with ID {subfolder.id}')
    return subfolder.id


def get_slack_channel_ids_names(channel_names:list):

    local_slack_ids_names = []
    global slack_client
    #チャンネル名からチャンネルIDリストを作成する########################
    res = slack_client.conversations_list()
    if res:
        slack_channel_names = res['channels']



    for chname in slack_channel_names:
        if chname['name'] in channel_names and chname["is_channel"]:
            local_slack_ids_names.append([chname['name'],chname['id']])

    return local_slack_ids_names
    
    ################################################################

###################################boxファイルのリストアップ##################################
def get_items_from_box_folder(channel_folder_name:str,date_folder_name:str="",root_folder_name:str='SlackUpload')->dict:
    """グローバル変数のbox_itemsを更新していく
        BOX内にroot-チャンネル名-日付-(アイテム)というフォルダ構造を作成し、
        既に存在する場合は、最下層フォルダ内のファイル情報を格納する
    Args:
        channel_folder_name (str): Slackのチャンネル名のフォルダ
        date_folder_name (str, optional): 最下層の日付フォルダの名前. Defaults to "".
        root_folder_name (str, optional): ルートフォルダの名前. Defaults to 'SlackUpload'.

    Returns:
        dict: box_itemsを返す
    """
    global box_items
    global DATEFOLDERNAME
    if not date_folder_name:
        date_folder_name = DATEFOLDERNAME

    #保存用の最上位フォルダ
    if not root_folder_name in box_items.keys():
        id_slackupload = find_and_create_folder(0,root_folder_name)
        box_items[root_folder_name] = {"id":id_slackupload, "items" : {}}

    if not channel_folder_name in box_items[root_folder_name]["items"].keys():
        id_channelname = find_and_create_folder(box_items[root_folder_name]["id"] ,channel_folder_name)
        box_items[root_folder_name]["items"][channel_folder_name] = {"id":id_channelname, "items" : {}}

    if not date_folder_name in box_items[root_folder_name]["items"][channel_folder_name]["items"].keys():
        id_date = find_and_create_folder(box_items[root_folder_name]["items"][channel_folder_name]["id"],date_folder_name)
        box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name] = {"id":id_date, "items" : {}}


    #フォルダ内アイテムを格納
    folder_items = user_client.folder(folder_id=box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name]["id"]).get_items()
    if folder_items:
        for item in folder_items:
            print(f'{item.type.capitalize()} {item.id} is named "{item.name}"')
            box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name]["items"][item.name] = item.id

    return box_items
###################################boxファイルのリストアップ##################################

def get_channel_messages(channel_ids) -> list:
    
    global TIMEOUT
    global STARTTIME
    global slack_client
    
    rtnmessages = []

    for channel_id in channel_ids:
        cursor = None  # シーク位置。最初は None ページを指定して、次からは next_cursor が指し示す位置。

        while True:
            try:
                response = slack_client.conversations_history(  # conversations_history ＝ チャット一覧を得る
                    channel=channel_id,
                    cursor=cursor  # チャンネルID、latest、シーク位置を指定。
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
                logger.info("TIMEOUT")
                break
    
    return rtnmessages

#mainエリア

#################テスト用
#TODO: 日ごとの実行にする
########################

TS_TODAY = datetime.datetime(now.year,now.month,now.day+1,0,0,0,tzinfo=JST).timestamp()

#対象チャンネル名とチャンネルIDの紐づけ
slack_ids_names = get_slack_channel_ids_names(SLACK_CHANNEL_NAMES)

#チャンネルidのみの取得
channel_ids = [row[1] for row in slack_ids_names]

#TODO: completed.jsonの確認
#boxからcompleted.jsonのダウンロード
#TS_TODAYが大きい、もしくはTS_YESTERDAYが小さい場合に処理

##############
#メイン実行エリア
##############

#TODO: completed.jsonの作成
#TS_TODAYとTS_YESTERDAYを保管しておく


#依頼書作成領域

slack_channel_messages = get_channel_messages(channel_ids)



#依頼書の格納リスト

iraisyolist = []
# if not SLACK_IRAISYO_STR:
#     SLACK_IRAISYO_STR = '*依頼書*を送信しました'


#フィードバックの格納リスト
feedbacklist = []
#フィードバック認識する文字列
# if not SLACK_FEEDBACK_STR:
#     SLACK_FEEDBACK_STR = '*フィードバック*を送信しました' 

for message in slack_channel_messages:

    if SLACK_IRAISYO_STR in message['text']:
        #認識文字列の検索　=> 項目検索中　=>　項目の値取得中　=>　項目検索中
        state = 0
        key = ''
        value = ''
        dictforiraicsv = dict()
        #チャンネル名とスレッド日時の入力
        message_channel_id = message['channel_id']
        message_channel_name =  [ row[0] for row in slack_ids_names if row[1] == message_channel_id ][0]
        dictforiraicsv["チャンネル名"] = message_channel_name
        dictforiraicsv["スレッド日時"] = datetime.datetime.fromtimestamp(float(message['ts']),tz=JST).isoformat()

        #項目の取得
        for line in message['text'].split("\n"):
            #認識文字列の検索
            if state == 0:
                if SLACK_IRAISYO_STR in line:
                    state = 1
            #項目検索中
            elif state == 1:
                if line != '':
                    key = line
                    dictforiraicsv[key] =""
                    state = 2
            #項目の値取得中
            elif state == 2:
                if line != '':
                    dictforiraicsv[key] = dictforiraicsv[key] + line
                else:
                    key = ''
                    value = ''
                    state = 1
        iraisyolist.append(dictforiraicsv)

    if SLACK_FEEDBACK_STR in message['text']:
        #認識文字列の検索　=> 項目検索中　=>　項目の値取得中　=>　項目検索中
        state = 0
        key = ''
        value = ''
        dictforfeedbackcsv = dict()
        dictforfeedbackcsv["チャンネル名"] = message_channel_name
        dictforfeedbackcsv["スレッド日時"] = datetime.datetime.fromtimestamp(float(message['ts']),tz=JST).isoformat()

        #項目の取得
        for line in message['text'].split("\n"):
            #認識文字列の検索
            if state == 0:
                if SLACK_FEEDBACK_STR in line:
                    state = 1
            #項目検索中
            elif state == 1:
                if line != '':
                    key = line
                    dictforfeedbackcsv[key] =""
                    state = 2
            #項目の値取得中
            elif state == 2:
                if line != '':
                    dictforfeedbackcsv[key] = dictforfeedbackcsv[key] + line
                else:
                    key = ''
                    value = ''
                    state = 1
        feedbacklist.append(dictforfeedbackcsv)

#####Excelの保存###############################################
import pandas as pd

if iraisyolist:
    ircsv = pd.DataFrame(iraisyolist)
    ircsv.to_excel('iraisyo.xlsx', sheet_name='new_sheet_name',index=False, header=True)

if feedbacklist:
    fdcsv = pd.DataFrame(feedbacklist)
    fdcsv.to_excel('feedback.xlsx', sheet_name='new_sheet_name',index=False, header=True)
##############################################################

#ファイルダウンロード領域

#SLACKからダウンロード候補リストを取得する
file_ids = slack_filelist_for_download(channels = channel_ids, ts_to = TS_TODAY, ts_from = 0)
######fileリストから重複を削除##################################
import requests
import codecs

uniq_filelist_for_download = []

for file in file_ids:

    channel_name = ""
    #file_ids内のチャンネルidがslack_ids_names内のチャンネルIDから見つかったら、チャンネル名を取得する
    channel_name = [cnl for cnl in slack_ids_names if cnl[1]==file['channel_id'][0]][0][0]
    print(f'file_name:{file["file_name"]}, channelname:{channel_name}')


    #file_ids内のタイムスタンプからdatefoldernameを作成する
    filedate = datetime.datetime.fromtimestamp(file['timestamp'],tz=JST)
    date_folder_name = datetime.datetime(filedate.year,filedate.month,filedate.day,0,0,0,tzinfo=JST).strftime('%Y%m%d')

    if not box_items:
        get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f"box_items is initially created")

    if not channel_name in box_items[ROOT_FOLDER_NAME]["items"].keys():
        get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f'not channel {channel_name}')

    #date_folder_nameの存在確認を行う
    if not date_folder_name in box_items[ROOT_FOLDER_NAME]["items"][channel_name]["items"].keys():
        #存在しなければ、取得もしくは作成を行う
        get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f'not datefolder {date_folder_name}')

    #ダウンロード候補ファイルの存在確認を行う
    if file['file_name'] in box_items[ROOT_FOLDER_NAME]["items"][channel_name]["items"][date_folder_name]["items"].keys():
        logger.info(f"file name:{file['file_name']} was found in box_items. delete from download list. ")
        #存在している場合は削除する
        # file_ids.remove(file)
    else:
        uniq_filelist_for_download.append(file)
        logger.info(f"file name:{file['file_name']} is new file")
    


for file in uniq_filelist_for_download:
###################################ファイルのダウンロード##################################
    file_url = file["download_url"]
    content = requests.get(
                file_url,
                allow_redirects=True,
                headers={'Authorization': f'Bearer {slack_token}'},
                stream=True
            ).content
    save_path = get_tmp_folder() + '/' + file["file_name"]
    
    # #既に存在したら、(1)形式でファイル名に追加
    # i = 0
    # while True:
    #     i += 1
    #     if os.path.exists(save_path):
    #         str = save_path
    #         save_path = str.split('.')[0] + f'({i}).' + str.split('.')[1]
    #     else:
    #         break

    target_file = codecs.open(save_path, 'wb')
    target_file.write(content)
    target_file.close()

###################################ファイルのダウンロード##################################
###################################ファイルのアップロード##################################
    #フォルダIDを取得する
    upload_folder_id = box_items[ROOT_FOLDER_NAME]["items"][channel_name]["items"][date_folder_name]["id"]
    new_file = user_client.folder(upload_folder_id).upload(save_path)
    logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')
    os.remove(save_path)
    