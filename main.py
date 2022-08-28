import logging
import os
import time
import pprint
import sys
import json

from logging import getLogger, StreamHandler, DEBUG
import datetime

from src import boxmodule
from src import slackmodule

STARTTIME = time.time()
COMPLETED_ID = ""
COMPLETED_DATE_SET = set()

with open('config.json','r',encoding='utf8') as f:
    js = json.load(f)
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

    TS_TODAY = datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST).timestamp()

    #20220801バグ対処　dayへの直接の加減算を削除 timedeltaでの処理に変更###########################################################

    # TS_TOMORROW = datetime.datetime(now.year,now.month,now.day + 1,0,0,0,tzinfo=JST).timestamp()
    # TS_YESTERDAY = datetime.datetime(now.year,now.month,now.day - 1,0,0,0,tzinfo=JST).timestamp()


    TS_TOMORROW = (datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST) + datetime.timedelta(days=1)).timestamp()
    TS_YESTERDAY  = (datetime.datetime(now.year,now.month,now.day,0,0,0,tzinfo=JST) - datetime.timedelta(days=1)).timestamp()

    ###########################################################################################################################

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