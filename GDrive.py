from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
import json
import signal
import queue
import atexit
import time
import concurrent.futures.thread

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/drive.metadata.readonly'
          ]


class GetServiceError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class AuthenticateError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class DriveClient:

    def __init__(self, cache_dir: str):
        super().__init__()
        self.creds = None
        self.service = None
        self.cache_dir: str = cache_dir
        self.terminate:bool = False
        self.fileQueue = queue.Queue(20)
        self.threadPoolExecutor = concurrent.futures.thread.ThreadPoolExecutor(max_workers=8)        
        signal.signal(signal.SIGINT, self.__keyboardINT__)    
        atexit.register(self.__cleanup__) 

    def __cleanup__(self):
        self.terminate = True
        self.fileQueue.put(None, block=False)        
        self.threadPoolExecutor.shutdown(True)


    def writeToCache(self, cache_file_name:str = "drive_cache"):
        q = self.fileQueue
        with Path(self.cache_dir).joinpath(cache_file_name).open(mode="w", encoding="UTF-8") as f:
            f.write("[")
            while self.terminate != True:
                if q.qsize() == 0:
                    time.sleep(0.1)
                else:
                    t:str = q.get(block=True)
                    if t == None:
                        break
                    else:
                        
                        f.write(t)
                        q.task_done()
            f.write("]")

    def getService(self):
        if self.service != None:
            return self.service
        else:
            raise GetServiceError("Cannot get service")
        return None

    def authenticate(self, credentials: str = "credentials.json", tokens: str = "token.pickle"):
        """Shows basic usage of the Drive v3 API.
        Prints the names and ids of the first 10 files the user has access to.
        """

        # The file token.pickle stores the user's access and refresh tokens, and is
        # created automatically when the authorization flow completes for the first
        # time.
        if os.path.exists(tokens):
            with open(tokens, 'rb') as token:
                self.creds = pickle.load(token)
        # If there are no (valid) credentials available, let the user log in.
        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    credentials, SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(tokens, 'wb') as token:
                pickle.dump(self.creds, token)

        self.service = build('drive', 'v3', credentials=self.creds)

        # Call the Drive v3 API
    def listFiles(self, folder: str = 'root', trashed: bool = False, mimeType: str = "", **kwargs):
        trash_str = "true" if trashed else "false"
        query = "'{0}' in parents and trashed={1} and 'me' in owners".format(folder, trash_str)       

        results = self.getService().files().list(
            pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)",
            q=query, spaces="drive"
        ).execute()
        nextPageToken = results.get("nextPageToken", None)
        result: list = results.get("files", [])
        self.threadPoolExecutor.submit(self.writeToCache)
        r: str = json.dumps(result, ensure_ascii=False)
        print(r[1:-1]) # print the current result
        self.fileQueue.put(r[1:-1])
        
        while nextPageToken != None and self.terminate != True:
            results = self.getService().files().list(
                pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)",
                q=query, spaces="drive", pageToken=nextPageToken
            ).execute()
            nextPageToken = results.get("nextPageToken", None)
            result.extend(results.get("files", []))            
            r:str = json.dumps(results.get("files", []), ensure_ascii=False)
            self.fileQueue.put(",")
            self.fileQueue.put(r[1:-1])
            print(r[1:-1]) # print the current result
        self.fileQueue.put(None)
        return result

    def listAllFiles(self, trashed: bool = False, mimeType: str = "", **kwargs):
        trash_str = "true" if trashed else "false"
        query = "trashed={0} and 'me' in owners".format(trash_str)
        results = self.getService().files().list(
            pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)",
            q=query, spaces="drive"
        ).execute()
        nextPageToken = results.get("nextPageToken", None)
        result: list = results.get("files", [])
        self.threadPoolExecutor.submit(self.writeToCache)
        r: str = json.dumps(result, ensure_ascii=False)
        print(r[1:-1]) # print the current result
        self.fileQueue.put(r[1:-1], block=False)
        
        while nextPageToken != None and self.terminate != True:
            results = self.getService().files().list(
                pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)",
                q=query, spaces="drive", pageToken=nextPageToken
            ).execute()
            nextPageToken = results.get("nextPageToken", None)
            result.extend(results.get("files", []))            
            r:str = json.dumps(results.get("files", []), ensure_ascii=False)
            self.fileQueue.put(",", block=False)
            self.fileQueue.put(r[1:-1], block=False)
            print(r[1:-1]) # print the current result
        self.fileQueue.put(None, block=False)

        return result

    def upload(self, file):
        results = self.getService().files().list(
            pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType)",
            q="'root' in parents and trashed=false", spaces="drive"
        ).execute()

        items = results.get('files', [])

        if not items:
            print('No files found.')
        else:
            print('Files:')
            for item in items:
                print(u'{0} ({1}) md5:{2} mimeType:{3}'.format(item['name'], item['id'], item.get('md5Checksum', "None"),
                                                               item["mimeType"]))
    def download(self, fileID:str):
        pass
    def __keyboardINT__(self, signal, frame):
        print("keyboard interrupt received")
        self.terminate = True
    

class FileStruct:
    def __init__(self):
        super().__init__()

    def children(self):
        pass

class FileTask:
    def __init__(self, func, *args, **kwargs):
        super().__init__()
        self.kwargs = kwargs
        self.args = args
        self.run = func


if __name__ == "__main__":
    d = DriveClient("/home/kie/test/.sync_ignore")
    d.authenticate()
    d.listAllFiles()
    pass
