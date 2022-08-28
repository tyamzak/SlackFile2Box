import json
import logging
import sys
from logging import getLogger, StreamHandler, DEBUG
from boxsdk import JWTAuth, Client
import datetime
from pathlib import Path

class BoxModule:

    def __init__(self):

        with open('config.json','r',encoding='utf8') as f:
            self.js = json.load(f)
            self.BOX_USER_ID = self.js["BOX_USER_ID"]
            self.TIMEOUT = float(self.js["TIMEOUT"])
            
        logging.basicConfig()
        self.logger = getLogger(__name__)
        self.handler = logging.StreamHandler(sys.stdout)
        self.handler.setLevel(logging.INFO)
        self.logger.addHandler(self.handler)
        self.logger.setLevel(logging.INFO)
        
        ################BOXJWTクライアントを作成する#########################################jwt
        authpath = Path(__file__).parent.parent / 'box_jwt_auth_config.json'
        self.auth = JWTAuth.from_settings_file(authpath)

        self.client = Client(self.auth)
        self.service_account = self.client.user().get()
        self.logger.info('Box Service Account user ID is {0}'.format(self.service_account.id))
        #別のユーザーとして処理を実行する
        self.user_to_impersonate = self.client.user(user_id=self.BOX_USER_ID)
        self.user_client = self.client.as_user(self.user_to_impersonate)

        ##################################################################################
        self.box_items
        self.DATEFOLDERNAME
        self.ROOT_FOLDER_NAME
        self.COMPLETED_ID
        self.COMPLETED_DATE_SET
    def find_and_create_folder(self, parent_folder_id:str, child_name:str) -> str:

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

        items = self.user_client.folder(self, folder_id=parent_folder_id).get_items()
        for item in items:
            if (item.name == child_name) and (item.type == "folder"):
                print(f'{item.type.capitalize()} {item.id} named "{item.name} was found"')
                return item.id

        #フォルダが無かった場合
        subfolder = self.user_client.folder(parent_folder_id).create_subfolder(child_name)
        print(f'Created subfolder with ID {subfolder.id}')
        return subfolder.id
    ###################################boxファイルのリストアップ##################################
    def get_items_from_box_folder(self, channel_folder_name:str,date_folder_name:str="",root_folder_name:str='SlackUpload')->dict:
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

        if not date_folder_name:
            date_folder_name = self.DATEFOLDERNAME

        #保存用の最上位フォルダ
        if not root_folder_name in self.box_items.keys():
            id_slackupload = self.find_and_create_folder(0,root_folder_name)
            self.box_items[root_folder_name] = {"id":id_slackupload, "items" : {}}

        if not channel_folder_name in self.box_items[root_folder_name]["items"].keys():
            id_channelname = self.find_and_create_folder(self.box_items[root_folder_name]["id"] ,channel_folder_name)
            self.box_items[root_folder_name]["items"][channel_folder_name] = {"id":id_channelname, "items" : {}}

        if not date_folder_name in self.box_items[root_folder_name]["items"][channel_folder_name]["items"].keys():
            id_date = self.find_and_create_folder(self.box_items[root_folder_name]["items"][channel_folder_name]["id"],date_folder_name)
            self.box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name] = {"id":id_date, "items" : {}}


        #フォルダ内アイテムを格納
        folder_items = self.user_client.folder(folder_id=self.box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name]["id"]).get_items()
        if folder_items:
            for item in folder_items:
                print(f'{item.type.capitalize()} {item.id} is named "{item.name}"')
                self.box_items[root_folder_name]["items"][channel_folder_name]["items"][date_folder_name]["items"][item.name] = item.id

        return self.box_items
    ###################################boxファイルのリストアップ##################################

    def is_yet_uploaded(self, TS_TODAY:str, TS_YESTERDAY:str):
        """_summary_

        Args:
            TS_TODAY (str): _description_
            TS_YESTERDAY (str): _description_

        Returns:
            str: box_file_id
        """


        if  not COMPLETED_DATE_SET:

            #BOX上を探す
            if not COMPLETED_ID:
                #保存用の最上位フォルダ
                if not self.ROOT_FOLDER_NAME in self.box_items.keys():
                    id_slackupload = self.find_and_create_folder(0,self.ROOT_FOLDER_NAME)
                    self.box_items[self.ROOT_FOLDER_NAME] = {"id":id_slackupload, "items" : {}}

                items = self.user_client.folder(folder_id=id_slackupload).get_items()

            
                for item in items:
                    if (item.name == 'completed.json') and (item.type == "file"):
                        print(f'{item.type.capitalize()} {item.id} named "{item.name} was found"')
                        COMPLETED_ID =  item.id
                        break

            #BOX上になければ作成する
            if not COMPLETED_ID:
                save_path = 'template_completed.json'
                new_file = self.user_client.folder(folder_id=id_slackupload).upload(save_path,'completed.json')
                self.logger.info(f'File "{new_file.name}" uploaded to Box with file ID {new_file.id}')
                COMPLETED_ID =  new_file.id
            

            file_content = self.user_client.file(COMPLETED_ID).content()
            self.js = json.loads(file_content)
            PAST_TS_TODAY = self.js["TS_TODAY"]
            PAST_TS_YESTERDAY = self.js["TS_YESTERDAY"]
            COMPLETED_DATE_SET = set(list(self.js["COMPLETED_DATE_SET"]))


        yester = datetime.datetime.fromtimestamp(TS_YESTERDAY)
        #実行した日付リストに入っていたら実行しない
        if str(TS_YESTERDAY) in COMPLETED_DATE_SET:
            self.logger.info(f"{yester} have already uploaded")
            return False,""
        else:
            self.logger.info(f"{yester} is new date")
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