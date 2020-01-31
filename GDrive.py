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
import threading
import concurrent.futures.process
import hashlib
from errors import *


class DriveClient:

    SCOPES = ['https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/drive.metadata.readonly'
          ]

    fields = "nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)"

    def __init__(self, cache_dir: str):
        super().__init__()
        self.creds = None
        self.cache_dir: str = cache_dir
        self.terminate: bool = False
        self.threads: int = 1
        self.folderScanSleepTime: int = 0.5
        self.fileScanSleepTime: int = 0.5
        self.folderScanComplete: bool = False
        self.fileScanComplete: bool = False
        self.querySize: int = 100
        self.folderCount: int = 0
        self.fileCount: int = 0
        self.fileWriteQueue: queue.Queue = queue.Queue(1000)
        self.folderWriteQueue: queue.Queue = queue.Queue(1000)
        self.folderQueries: queue.Queue = queue.Queue(10000)
        self.fileQueries: queue.Queue = queue.Queue(10000)
        self.folderPageTokens: dict = dict()
        self.filePageTokens: dict = dict()
        
        self.searchFoldersQueue: queue.Queue = queue.Queue(10000)
        self.searchFileQueue: queue.Queue = queue.Queue(10000)
        self.threadPoolExecutor = concurrent.futures.thread.ThreadPoolExecutor(
            max_workers=8)

        self.threadnames: dict = dict()
        signal.signal(signal.SIGINT, self.__keyboardINT__)
        atexit.register(self.__cleanup__)

    def __cleanup__(self):
        self.terminate = True
        self.fileWriteQueue.put(None, block=False)
        self.folderWriteQueue.put(None, block=False)
        self.searchFoldersQueue.put(None, block=False)
        self.threadPoolExecutor.shutdown(True)

    def writeToCache(self, q: queue.Queue, cache_file_name: str = "drive_cache"):

        first: bool = True
        with Path(self.cache_dir).joinpath(cache_file_name).open(mode="w", encoding="UTF-8") as f:
            f.write("[")
            while self.terminate != True:
                if q.qsize() == 0:
                    time.sleep(0.1)
                else:
                    t: str = q.get(block=True)
                    if t == None:
                        q.task_done()
                        break
                    else:
                        if first:
                            first = False
                        else:
                            f.write(",")
                        f.write(t)
                        q.task_done()
            f.write("]")

    def getService(self, api_version: str = "v3"):
        threadName: str = threading.currentThread().getName()
        if self.creds == None:
            raise AuthenticateError("Not authenticated")
        if threadName not in self.threadnames:
            service = build('drive', api_version, credentials=self.creds)
            self.threadnames[threadName] = service
        else:
            service = self.threadnames[threadName]
        return service

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
                    credentials, DriveClient.SCOPES)
                self.creds = flow.run_local_server(port=0)
            # Save the credentials for the next run
            with open(tokens, 'wb') as token:
                pickle.dump(self.creds, token)

        # Call the Drive v3 API

    def listFiles(self, folder: str = 'root', trashed: bool = False, mimeType: str = "", **kwargs):
        trash_str = "true" if trashed else "false"
        query = "'{0}' in parents and trashed={1} and 'me' in owners and mimeType!='application/vnd.google-apps.folder'".format(
            folder, trash_str)
        fields = "nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)"
        service = self.getService()

        results = service.files().list(
            pageSize=1000, fields=fields,
            q=query, spaces="drive"
        ).execute()
        nextPageToken = results.get("nextPageToken", None)
        result: list = results.get("files", [])

        while nextPageToken != None and self.terminate != True:
            results = service.files().list(
                pageSize=1000, fields=fields,
                q=query, spaces="drive", pageToken=nextPageToken
            ).execute()
            nextPageToken = results.get("nextPageToken", None)
            result.extend(results.get("files", []))

        return result

    def listFolders(self, folder: str = 'root', trashed: bool = False, **kwargs):

        trash_str = "true" if trashed else "false"
        query = "'{0}' in parents and trashed={1} and 'me' in owners and mimeType='application/vnd.google-apps.folder'".format(
            folder, trash_str)
        fields = "nextPageToken, files(id, name, md5Checksum, mimeType, parents, modifiedTime)"
        service = self.getService()

        results = service.files().list(
            pageSize=1000, fields=fields,
            q=query, spaces="drive"
        ).execute()
        nextPageToken = results.get("nextPageToken", None)
        result: list = results.get("files", [])

        while nextPageToken != None and self.terminate != True:
            results = service.files().list(
                pageSize=1000, fields=fields,
                q=query, spaces="drive", pageToken=nextPageToken
            ).execute()
            nextPageToken = results.get("nextPageToken", None)
            result.extend(results.get("files", []))

        return result

    def _onBatchFileReceived(self, id, response, exception):
        if exception is not None:            
            self.searchFileQueue.task_done()
            print(type(exception))
            raise RequestError(exception)
            pass
        elif self.terminate == False:

            results = response.get("files", [])
            nextPage = response.get("nextPageToken", None)
            if nextPage != None:
                folder_str, _a = self.filePageTokens[id]
                self.filePageTokens[id] = (folder_str, nextPage)
                self.searchFileQueue.put(id, block=False)

            elif nextPage == None and id in self.folderPageTokens:
                self.filePageTokens.pop(id, "")

            r: str = json.dumps(results, ensure_ascii=False)[1:-1]
            if len(r) > 0:
                self.fileWriteQueue.put(r)
            self.fileCount += len(results)
            self.searchFileQueue.task_done()

    def batchFile(self, trashed: bool = False, **kwargs):

        trash_str = "true" if trashed else "false"
        q = self.searchFileQueue
        service = self.getService()
        retry: bool = False

        while self.terminate == False:
            count: int = 0
            if retry == False:
                batch = service.new_batch_http_request()
            folders: list = list()
            count: int = 0
            
            
            while count < 10 and q.qsize() > 0 and retry == False:                    
                
                id: str = q.get(block=False)                   
                folder_str, nextPageToken = self.filePageTokens[id]

                query = "( {0} ) and trashed={1} and 'me' in owners and" \
                    "mimeType!='application/vnd.google-apps.folder'".format(
                        folder_str, trash_str)
                l = service.files().list(
                    pageSize=1000, fields=DriveClient.fields,
                    q=query, spaces="drive", pageToken=nextPageToken
                )
                batch.add(request=l, callback=self._onBatchFileReceived,
                        request_id=id)
                count += 1
            try:
                batch.execute()
            except RequestError as e:
                if e.getCause() == RequestError.RATE_LIMIT_EXCEEDED:
                    time.sleep(3)
                    retry = True
            else:
                time.sleep(self.fileScanSleepTime)
                retry = False

    def _onBatchFolderReceived(self, id, response, exception):
        if exception is not None:            
            self.searchFoldersQueue.task_done()  # to check whether we've finished yet
            print(type(exception))
            raise RequestError("exception encountered", exception)
            pass
        elif self.terminate == False:

            results = response.get("files", [])
            nextPage = response.get("nextPageToken", None)

            print("received")
            print(id, response)
            print("pageToken", nextPage)

            if nextPage != None:
                folder_str, _a = self.folderPageTokens[id]
                self.folderPageTokens[id] = (folder_str, nextPage)
                self.searchFoldersQueue.put(id, block=False)

            elif nextPage == None and id in self.folderPageTokens:
                self.folderPageTokens.pop(id, "")
     
            for f in results:

                folder: str = "'{}' in parents".format(f["id"])
                self.folderQueries.put(folder, block=False)  
                self.folderCount += 1
            

            self.searchFoldersQueue.task_done() 
            r: str = str(json.dumps(results, ensure_ascii=False))[1:-1]
            if len(r) > 0:
                self.folderWriteQueue.put(r, block=False)
        else:
            self.searchFoldersQueue.task_done()

    def batchFolder(self, trashed: bool = False, **kwargs):

        trash_str = "true" if trashed else "false"
        sq = self.searchFoldersQueue
        fq = self.folderQueries
        service = self.getService()
        retry: bool = False

        while self.terminate == False:
            
            if retry == False:
                batch = service.new_batch_http_request()

            if fq.qsize() > self.querySize or (sq.qsize() < 2 and fq.qsize() > 0):
                queries: list = list()
                for _ in range(self.querySize):
                    if fq.qsize() > 0:
                        queries.append(fq.get(block=False))
                        fq.task_done()
                    else: 
                        break
                folder_str: str = " or ".join(queries)
                m = hashlib.md5(folder_str.encode("ascii"))
                id = m.hexdigest()
                self.searchFoldersQueue.put(id, block=False)
                # self.searchFileQueue.put(id, block=False)
                self.folderPageTokens[id] = (folder_str, None)
                # self.filePageTokens[id] = (folder_str, None)
                queries.clear()
            
            count: int = 0
            while sq.qsize() > 0 and count < 10 and retry == False:
                id: str = sq.get(block=False)
                folder_str, nextPageToken = self.folderPageTokens[id]

                query = "( {0} ) and trashed={1} and 'me' in owners and" \
                    "mimeType='application/vnd.google-apps.folder'".format(
                        folder_str, trash_str)

                l = service.files().list(
                    pageSize=1000, fields=DriveClient.fields,
                    q=query, spaces="drive", pageToken=nextPageToken
                )

                batch.add(request=l, callback=self._onBatchFolderReceived,
                          request_id=id)

                count += 1

            try:
                if count > 0:
                    batch.execute()
            except RequestError as e:
                if e.getCause() == RequestError.RATE_LIMIT_EXCEEDED:
                    time.sleep(3)
                    retry = True
            else:
                time.sleep(self.folderScanSleepTime)
                retry = False
                
        
        while sq.qsize() > 0:
            sq.get(block=False)
            sq.task_done()
        while fq.qsize() > 0:
            fq.get(block=False)
            fq.task_done()

    def listAll(self, folder: str = "root", trashed: bool = False, mimeType: str = "", **kwargs):
        self.threadPoolExecutor.submit(
            self.writeToCache, self.fileWriteQueue, cache_file_name="drive_cache_files")
        self.threadPoolExecutor.submit(
            self.writeToCache, self.folderWriteQueue, cache_file_name="drive_cache_folder")

        folder_str: str = "'{0}' in parents".format(folder)
        m = hashlib.md5(folder_str.encode("ascii"))
        id: str = m.hexdigest()
        self.searchFoldersQueue.put(id, block=False)
        # self.searchFileQueue.put(id, block=False)
        self.folderPageTokens[id] = (folder_str, None)
        # self.filePageTokens[id] = (folder_str, None)

        self.threadPoolExecutor.submit(
                self.batchFolder, folder=folder)
        # self.threadPoolExecutor.submit(
        #         self.batchFile, folder=folder)
        
        while self.terminate == False and (self.folderQueries.qsize() > 0 or self.searchFoldersQueue.qsize() > 0):
            time.sleep(1)
            self.searchFoldersQueue.join()
        
        # self.searchFileQueue.join()

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

    def download(self, fileID: str):
        pass

    def __keyboardINT__(self, signal, frame):
        print("keyboard interrupt received")
        self.terminate = True


class FileStruct:
    def __init__(self):
        super().__init__()

    def children(self):
        pass


if __name__ == "__main__":
    back = time.time()
    d = DriveClient("/home/kie/test/.sync_ignore")
    d.authenticate()
    # d.listAll("1QHfx3xUyKMvzPxqrI1AbEXadPnZjFs4Z")
    d.listAll("root")
    print(d.folderCount)
    print(d.fileCount)
    front = time.time()
    print(front-back)
    pass
