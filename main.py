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
COMPLETED_ID = ""
COMPLETED_DATE_SET = set()

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
                    #setは日付 + ファイル名形式
                    

                    # #既に存在したら、(1)形式でファイル名に追加
                    i = 0
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


#チャンネル名からチャンネルIDリストを作成する########################
def get_slack_channel_ids_names(channel_names:list):
    
    local_slack_ids_names = []
    global slack_client
    #publicとprivateの両方を対象にする
    res = slack_client.conversations_list(types="public_channel, private_channel")
    if res:
        slack_channel_names = res['channels']


    #名前, id, 作成時のタイムスタンプを取得
    for chname in slack_channel_names:
        if chname['name'] in channel_names and chname["is_channel"]:
            local_slack_ids_names.append([chname['name'],chname['id'],str(chname['created'])])

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

def get_channel_messages(channel_id, ts_to, ts_from) -> list:
    
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
            logger.info("TIMEOUT")
            break
    
    return rtnmessages

def is_yet_uploaded(TS_TODAY:str, TS_YESTERDAY:str):
    """_summary_

    Args:
        TS_TODAY (str): _description_
        TS_YESTERDAY (str): _description_

    Returns:
        str: box_file_id
    """
    global box_items
    global ROOT_FOLDER_NAME
    global COMPLETED_ID
    global COMPLETED_DATE_SET

    if  not COMPLETED_DATE_SET:

        #BOX上を探す
        if not COMPLETED_ID:
            #保存用の最上位フォルダ
            if not ROOT_FOLDER_NAME in box_items.keys():
                id_slackupload = find_and_create_folder(0,ROOT_FOLDER_NAME)
                box_items[ROOT_FOLDER_NAME] = {"id":id_slackupload, "items" : {}}

            items = user_client.folder(folder_id=id_slackupload).get_items()

        
            for item in items:
                if (item.name == 'completed.json') and (item.type == "file"):
                    print(f'{item.type.capitalize()} {item.id} named "{item.name} was found"')
                    COMPLETED_ID =  item.id
                    break

        #BOX上になければ作成する
        if not COMPLETED_ID:
            save_path = 'template_completed.json'
            new_file = user_client.folder(folder_id=id_slackupload).upload(save_path,'completed.json')
            logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')
            COMPLETED_ID =  new_file.id
        

        file_content = user_client.file(COMPLETED_ID).content()
        js = json.loads(file_content)
        PAST_TS_TODAY = js["TS_TODAY"]
        PAST_TS_YESTERDAY = js["TS_YESTERDAY"]
        COMPLETED_DATE_SET = set(list(js["COMPLETED_DATE_SET"]))


    yester = datetime.datetime.fromtimestamp(TS_YESTERDAY)
    #実行した日付リストに入っていたら実行しない
    if str(TS_YESTERDAY) in COMPLETED_DATE_SET:
        logger.info(f"{yester} have already uploaded")
        return False,""
    else:
        logger.info(f"{yester} is new date")
        return True, COMPLETED_ID

    
def update_timestamp(TS_TODAY:str, TS_YESTERDAY:str)->bool:
    #処理後のタイムスタンプをアップデートする
    global COMPLETED_ID
    global COMPLETED_DATE_SET
    tmp = SAVEFOLDER
    #savefolderの下に保存する
    file_path = 'template_completed.json'
    save_path = tmp + '/' + file_path
    COMPLETED_DATE_SET.add(str(TS_YESTERDAY))
    
    #テンプレートを読み込んで書き込む
    with open(file_path, 'r') as f:
        js = json.load(f)
        js["TS_TODAY"] = TS_TODAY
        js["TS_YESTERDAY"] = TS_YESTERDAY
        js["COMPLETED_DATE_SET"] = list(COMPLETED_DATE_SET)

    #テンプレートに保存する
    with open(save_path, 'w') as f:
        json.dump(js, f)

    if not COMPLETED_ID:
        #Boxの記録ファイルを取得する
        id_slackupload = find_and_create_folder(0,'SlackUpload')
        items = user_client.folder(folder_id=id_slackupload).get_items()
        COMPLETED_ID = ""

        for item in items:
            if (item.name == 'completed.json') and (item.type == "file"):
                print(f'{item.type.capitalize()} {item.id} named "{item.name} was found"')
                COMPLETED_ID =  item.id
                break

    #ファイルバージョンの更新を行う
    if COMPLETED_ID:
        new_file = user_client.file(COMPLETED_ID).update_contents(save_path)
        logger.info(f'File "{new_file.name}" updated to Box with file ID {new_file.id}')
    
        return True
    else:
        return False
    
def make_workflow_csv(slack_channel_messages, channel_id, TS_YESTERDAY,TS_TODAY):
    global box_items
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
            for line in message['text'].split(" *"):
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
                    # else:
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
            message_channel_id = message['channel_id']
            message_channel_name =  [ row[0] for row in slack_ids_names if row[1] == message_channel_id ][0]
            dictforfeedbackcsv["チャンネル名"] = message_channel_name
            dictforfeedbackcsv["スレッド日時"] = datetime.datetime.fromtimestamp(float(message['ts']),tz=JST).isoformat()

            #項目の取得
            for line in message['text'].split(" *"):
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
                    # else:
                        key = ''
                        value = ''
                        state = 1
            feedbacklist.append(dictforfeedbackcsv)

    import pandas as pd

    #channel_idからchannel_folder_nameを作成する

    channel_folder_name =  [ row[0] for row in slack_ids_names if row[1] == channel_id ][0]
    #TS_YESTERDAYのタイムスタンプからdatefoldernameを作成する
    filedate = datetime.datetime.fromtimestamp(TS_YESTERDAY,tz=JST)
    date_folder_name = datetime.datetime(filedate.year,filedate.month,filedate.day,0,0,0,tzinfo=JST).strftime('%Y%m%d')

    if not box_items:
        box_items = get_items_from_box_folder(channel_folder_name=channel_folder_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f"not box")

    if not channel_folder_name in box_items[ROOT_FOLDER_NAME]["items"].keys():
        box_items = get_items_from_box_folder(channel_folder_name=channel_folder_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f'not channel {channel_folder_name}')

    #date_folder_nameの存在確認を行う
    if not date_folder_name in box_items[ROOT_FOLDER_NAME]["items"][channel_folder_name]["items"].keys():
        #存在しなければ、取得もしくは作成を行う
        box_items = get_items_from_box_folder(channel_folder_name=channel_folder_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
        logger.info(f'not datefolder {date_folder_name}')


    id_datefolder = box_items['SlackUpload']['items'][channel_folder_name]['items'][date_folder_name]['id']
    # Windows用
    # if iraisyolist:
    #     ircsv = pd.DataFrame(iraisyolist)
    #     ircsv.to_excel(get_tmp_folder() + '/' + 'iraisyo.xlsx', sheet_name='new_sheet_name',index=False, header=True)
    #     new_file = user_client.folder(folder_id=id_datefolder).upload(get_tmp_folder() + '/' + 'iraisyo.xlsx')
    #     logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')

    # if feedbacklist:
    #     fdcsv = pd.DataFrame(feedbacklist)
    #     fdcsv.to_excel(get_tmp_folder() + '/' + 'feedback.xlsx', sheet_name='new_sheet_name',index=False, header=True)
    #     new_file = user_client.folder(folder_id=id_datefolder).upload(get_tmp_folder() + '/' + 'feedback.xlsx')
    #     logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')

    # Google Cloud Function用
    if iraisyolist:
        ircsv = pd.DataFrame(iraisyolist)
        ircsv.to_csv(get_tmp_folder() + '/' + 'iraisyo.csv',index=False, header=True)
        new_file = user_client.folder(folder_id=id_datefolder).upload(get_tmp_folder() + '/' + 'iraisyo.csv')
        logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')

    if feedbacklist:
        fdcsv = pd.DataFrame(feedbacklist)
        fdcsv.to_csv(get_tmp_folder() + '/' + 'feedback.csv',index=False, header=True)
        new_file = user_client.folder(folder_id=id_datefolder).upload(get_tmp_folder() + '/' + 'feedback.csv')
        logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')

def file_upload_slack2box(file_ids):
    ######fileリストからファイルのダウンロード##################################
    ####slackからのリストの時点で重複ファイル名の処理は終わっているので、ここでは同名ファイルは純粋に削除する
    import requests
    import codecs
    global box_items


    for file in file_ids:

        channel_name = ""
        #file_ids内のチャンネルidがslack_ids_names内のチャンネルIDから見つかったら、チャンネル名を取得する
        channel_name = [cnl for cnl in slack_ids_names if cnl[1]==file['channel_id'][0]][0][0]

        #file_ids内のタイムスタンプからdatefoldernameを作成する
        filedate = datetime.datetime.fromtimestamp(file['timestamp'],tz=JST)
        date_folder_name = datetime.datetime(filedate.year,filedate.month,filedate.day,0,0,0,tzinfo=JST).strftime('%Y%m%d')

        if not box_items:
            box_items = get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
            logger.info(f"not box")

        if not channel_name in box_items[ROOT_FOLDER_NAME]["items"].keys():
            box_items = get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
            logger.info(f'not channel {channel_name}')

        #date_folder_nameの存在確認を行う
        if not date_folder_name in box_items[ROOT_FOLDER_NAME]["items"][channel_name]["items"].keys():
            #存在しなければ、取得もしくは作成を行う
            box_items = get_items_from_box_folder(channel_folder_name=channel_name,date_folder_name=date_folder_name,root_folder_name=ROOT_FOLDER_NAME)
            logger.info(f'not datefolder {date_folder_name}')


        #ダウンロード候補ファイルの存在確認を行う
        #存在している場合はダウンロード処理に移らない
        if file['file_name'] in box_items[ROOT_FOLDER_NAME]["items"][channel_name]["items"][date_folder_name]["items"].keys():
            logger.info(f"file name {file['file_name']} was found. removed from upload list")
            continue



    ###################################ファイルのダウンロード##################################
        file_url = file["download_url"]
        content = requests.get(
                    file_url,
                    allow_redirects=True,
                    headers={'Authorization': f'Bearer {slack_token}'},
                    stream=True
                ).content
        save_path = get_tmp_folder() + '/' + file["file_name"]

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

#mainエリア
slack_ids_names = get_slack_channel_ids_names(SLACK_CHANNEL_NAMES)

def hello_pubsub(event, context):
    main()

def main():
    channel_ids = [row[1] for row in slack_ids_names]

    TS_TOMORROW = datetime.datetime(now.year,now.month,now.day + 1,0,0,0,tzinfo=JST).timestamp()
    TS_TODAY = datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST).timestamp()
    TS_YESTERDAY = datetime.datetime(now.year,now.month,now.day - 1,0,0,0,tzinfo=JST).timestamp()


    #本日分を実施　完了記録は残さない　ワークフローの集計を実施しない
    ts_to = TS_TOMORROW
    ts_from = TS_TODAY

    if is_yet_uploaded(ts_to,ts_from):

        #SLACKからダウンロード候補リストを取得する
        file_ids = slack_filelist_for_download(channels = channel_ids, ts_to = ts_to, ts_from = ts_from)
        #BOXにアップロードする
        file_upload_slack2box(file_ids)
        
        for channel_id in channel_ids:
            slack_channel_messages = get_channel_messages(channel_id, ts_to = ts_to, ts_from = ts_from)
            make_workflow_csv(slack_channel_messages,channel_id,ts_from,ts_to)





    #昨日以降分を実施 ワークフローの集計も実施していく
    past_index = 0
    while True:

        ts_to = (datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST) - datetime.timedelta(days=past_index)).timestamp()
        ts_from  = (datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST) - datetime.timedelta(days= 1 + past_index)).timestamp()
        box_file_id = is_yet_uploaded(ts_to,ts_from)

        if box_file_id[0]:

            #SLACKからダウンロード候補リストを取得する
            file_ids = slack_filelist_for_download(channels = channel_ids, ts_to = ts_to, ts_from = ts_from)
            #BOXにアップロードする
            file_upload_slack2box(file_ids)
            #outdatedcount
            outdatedcount = 0
            for channel_id in channel_ids:
                ts_oldest = [x[2] for x in slack_ids_names if x[1]==channel_id][0]
                #作成日以前は探さない
                if float(ts_oldest) <= float(ts_from) :
                    slack_channel_messages = get_channel_messages(channel_id, ts_to = ts_to, ts_from = ts_from)
                    make_workflow_csv(slack_channel_messages,channel_id,ts_from,ts_to)
                else:
                    outdatedcount += 1
            
            if outdatedcount == len(channel_ids):
                logger.info("All Files are upload completed")
                break

            update_timestamp(ts_to,ts_from)
        past_index += 1

        #Timeout
        if time.time() - STARTTIME > TIMEOUT:
            logger.info("TIMEOUT")
            break

if __name__ == "__main__":
    main()