from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload
from googleapiclient.http import MediaFileUpload

from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from pathlib import Path
import json
import signal
import queue
import atexit
import time
import concurrent.futures as Futures
from threadExecutor import ThreadPoolExecutorStackTraced as ThreadPoolExecutor
import threading
import hashlib
from errors import *
import googleapiclient.errors as apiErrors
import io
import pyrfc3339
import pytz
import datetime
import asyncio
import codecs

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
        self.folder_threads: int = 0
        self.file_threads: int = 0
        self.folderScanSleepTime: int = 0.5
        self.fileScanSleepTime: int = 0.5
        self.folderScanComplete: bool = False
        self.fileScanComplete: bool = False
        self.files: list = list()
        self.folders: list = list()
        self.folderPaths: dict() = dict()
        self.querySize: int = 100
        self.folderCount: int = 0
        self.fileCount: int = 0
        self.unfinished_folder_req: int = 0
        self.unfinished_file_req: int = 0
        self.fileWriteQueue: queue.Queue = queue.Queue(1000)
        self.folderWriteQueue: queue.Queue = queue.Queue(1000)
        self.folderQueries: queue.Queue = queue.Queue(10000)
        self.fileQueries: queue.Queue = queue.Queue(10000)
        self.searchFolderQueue: queue.Queue = queue.Queue(10000)
        self.searchFileQueue: queue.Queue = queue.Queue(10000)
        self.folderPageTokens: dict = dict()
        self.filePageTokens: dict = dict()

        self.threadPoolExecutor = ThreadPoolExecutor(
            max_workers=16)

        self.threadnames: dict = dict()
        signal.signal(signal.SIGINT, self.__keyboardINT__)
        signal.signal(signal.SIGTERM, self.__terminate__)
        atexit.register(self.__cleanup__)

    def __terminate__(self, signal, frame):
        self.__cleanup__()

    def stop(self):
        self.__cleanup__()

    def __cleanup__(self):
        self.terminate = True
        self.threadPoolExecutor.shutdown(True)
        try:
            while 1:
                self.searchFolderQueue.task_done()
        except ValueError:
            pass
        try:
            while 1:
                self.searchFileQueue.task_done()
        except ValueError:
            pass
        try:
            while 1:
                self.fileWriteQueue.task_done()
        except ValueError:
            pass
        try:
            while 1:
                self.folderWriteQueue.task_done()
        except ValueError:
            pass
        print("clean up called")

    def __keyboardINT__(self, signal, frame):
        print("keyboard interrupt received")
        self.__cleanup__()

    def writeToCache(self):

        first1: bool = True
        first2: bool = True

        try:

            fileopen = Path(self.cache_dir).joinpath(
                "remote_cache_files").open(mode="w", encoding="UTF-8")
            folderopen = Path(self.cache_dir).joinpath(
                "remote_cache_folders").open(mode="w", encoding="UTF-8")

            fileopen.write("[")
            folderopen.write("[")
            while self.terminate != True:

                try:
                    results: list = self.folderWriteQueue.get(block=False)
                    

                    if first1:
                        first1 = False
                    else:
                        folderopen.write(",")
                    t: str = json.dumps(results, ensure_ascii=False)[1:-1]
                    folderopen.write(t)
                except queue.Empty:
                    pass
                except Exception:
                    self.folderwriteQueue.task_done()
                    raise
                else:
                    self.folderWriteQueue.task_done()

                try:
                    results: list = self.fileWriteQueue.get(block=False)
                    
                    self.files.extend(results)
                    if first2:
                        first2 = False
                    else:
                        fileopen.write(",")
                    t: str = json.dumps(results, ensure_ascii=False)[1:-1]
                    fileopen.write(t)
                except queue.Empty:
                    pass
                except Exception:
                    self.fileWriteQueue.task_done()
                    raise
                else:
                    self.fileWriteQueue.task_done()
                

                if self.folderWriteQueue.qsize() == 0 and self.fileWriteQueue.qsize() == 0:
                    time.sleep(0.5)

            fileopen.write("]")
            folderopen.write("]")
        except IOError as e:

            raise
        except OSError as o:
            raise
        finally:
            fileopen.close()
            folderopen.close()

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

    def _onFileReceived(self, id, response):
        if self.terminate == False:

            results = response.get("files", [])
            nextPage = response.get("nextPageToken", None)

            if nextPage != None:
                folder_str, _a = self.filePageTokens[id]
                self.filePageTokens[id] = (folder_str, nextPage)
                self.unfinished_file_req += 1
                self.searchFileQueue.put(id, block=False)

            elif nextPage == None and id in self.filePageTokens:
                self.filePageTokens.pop(id, "")

            self.fileCount += len(results)

            for f in results:
                modifiedTime: str = f["modifiedTime"]
                newTime: str = str(pyrfc3339.parse(modifiedTime)
                                   .replace(tzinfo=None))  # modified time is always in UTC
                f["modifiedTime"] = newTime
                f["isDir"] = False

            if len(results) > 0:
                self.fileWriteQueue.put(results, block=False)

    def _generateFileQuery(self):

        sq = self.searchFileQueue
        fq = self.fileQueries

        if fq.qsize() > self.querySize or (sq.qsize() < 20 and fq.qsize() > 0):
            queries: list = list()
            for _ in range(self.querySize):
                if fq.qsize() > 0:
                    try:
                        queries.append(fq.get(block=False))
                        fq.task_done()
                    except queue.Empty as empty:
                        pass
                else:
                    break
            if len(queries) > 0:
                folder_str: str = " or ".join(queries)
                m = hashlib.md5(folder_str.encode("ascii"))
                id = m.hexdigest()
                self.unfinished_file_req += 1
                sq.put(id, block=False)
                # self.searchFileQueue.put(id, block=False)
                self.filePageTokens[id] = (folder_str, None)
                # self.filePageTokens[id] = (folder_str, None)

    def _listFiles(self, trashed: bool = False, **kwargs):

        trash_str = "true" if trashed else "false"
        sq = self.searchFileQueue
        fq = self.fileQueries
        service = self.getService()
        retry: bool = False
        self.file_threads += 1

        while self.terminate == False:

            sq_size = sq.qsize()

            try:
                if sq_size > 0:
                    id: str = sq.get(block=False)

                    folder_str, nextPageToken = self.filePageTokens[id]
                    query = "( {0} ) and trashed={1} and 'me' in owners and" \
                        "mimeType!='application/vnd.google-apps.folder'".format(
                            folder_str, trash_str)
                    l = service.files().list(
                        pageSize=1000, fields=DriveClient.fields,
                        q=query, spaces="drive", pageToken=nextPageToken
                    )

                    response = l.execute()
                    self._onFileReceived(id, response)
                self._generateFileQuery()
            except queue.Empty as empty:

                pass
            except Exception as exc:

                if isinstance(exc, apiErrors.HttpError):
                    cause = RequestError(exc).getCause()
                    if cause == RequestError.RATE_LIMIT_EXCEEDED:
                        print("user rate limit exceeded")
                        time.sleep(3)
                        retry = True
                        self.filePageTokens[id] = (folder_str, nextPageToken)
                        self.unfinished_file_req += 1
                        sq.put(id, block=False)
                    self.unfinished_file_req -= 1
                    sq.task_done()
                else:
                    
                    self.unfinished_file_req -= 1
                    sq.task_done()
                    raise
            else:
                if sq_size > 0:
                    self.unfinished_file_req -= 1
                    sq.task_done()
                time.sleep(self.fileScanSleepTime)
                retry = False

        while sq.qsize() > 0:
            sq.get(block=False)
            sq.task_done()
        while fq.qsize() > 0:
            fq.get(block=False)
            fq.task_done()

        self.file_threads -= 1
        return self.files

    def _generateFolderQuery(self):

        sq = self.searchFolderQueue
        fq = self.folderQueries

        if fq.qsize() > self.querySize or (sq.qsize() < 20 and fq.qsize() > 0):
            queries: list = list()
            for _ in range(self.querySize):
                if fq.qsize() > 0:
                    try:
                        queries.append(fq.get(block=False))
                        fq.task_done()
                    except queue.Empty as empty:
                        pass
                else:
                    break
            if len(queries) > 0:
                folder_str: str = " or ".join(queries)
                m = hashlib.md5(folder_str.encode("ascii"))
                id = m.hexdigest()
                self.unfinished_folder_req += 1
                sq.put(id, block=False)

                # self.searchFileQueue.put(id, block=False)
                self.folderPageTokens[id] = (folder_str, None)
                # self.filePageTokens[id] = (folder_str, None)

    def _onFolderReceived(self, id, response):
        if self.terminate == False:

            results = response.get("files", [])
            nextPage = response.get("nextPageToken", None)

            if nextPage != None:
                folder_str, _a = self.folderPageTokens[id]
                self.folderPageTokens[id] = (folder_str, nextPage)
                self.unfinished_folder_req += 1
                self.searchFolderQueue.put(id, block=False)

            elif nextPage == None and id in self.folderPageTokens:
                self.folderPageTokens.pop(id, "")

            for f in results:

                folder: str = "'{}' in parents".format(f["id"])
                self.folderQueries.put(folder, block=False)
                self.fileQueries.put(folder, block=False)
                self.folderCount += 1
                modifiedTime: str = f["modifiedTime"]
                newTime: str = str(pyrfc3339.parse(modifiedTime)
                                   .replace(tzinfo=None))  # modified time is always in UTC
                f["modifiedTime"] = newTime
                f["isDir"] = True

            if len(results) > 0:
                self.folderWriteQueue.put(results, block=False)

    def _listFolders(self, trashed: bool = False, **kwargs):

        trash_str = "true" if trashed else "false"
        sq = self.searchFolderQueue
        fq = self.folderQueries
        service = self.getService()
        retry: bool = False
        
        self.folder_threads += 1

        while self.terminate == False:

            sq_size = sq.qsize()

            try:
                if sq_size > 0:
                    id: str = sq.get(block=False)

                    folder_str, nextPageToken = self.folderPageTokens[id]
                    query = "( {0} ) and trashed={1} and 'me' in owners and" \
                        "mimeType='application/vnd.google-apps.folder'".format(
                            folder_str, trash_str)
                    l = service.files().list(
                        pageSize=1000, fields=DriveClient.fields,
                        q=query, spaces="drive", pageToken=nextPageToken
                    )

                    response = l.execute()
                    self._onFolderReceived(id, response)
                self._generateFolderQuery()
            except queue.Empty as empty:

                pass
            except Exception as exc:

                if isinstance(exc, apiErrors.HttpError):
                    cause = RequestError(exc).getCause()
                    if cause == RequestError.RATE_LIMIT_EXCEEDED:
                        print("user rate limit exceeded")
                        time.sleep(3)
                        retry = True
                        self.folderPageTokens[id] = (folder_str, nextPageToken)
                        self.unfinished_folder_req += 1
                        sq.put(id, block=False)
                    self.unfinished_folder_req -= 1
                    sq.task_done()
                else:
                    
                    self.unfinished_folder_req -= 1
                    sq.task_done()
                    raise
            else:
                if sq_size > 0:
                    self.unfinished_folder_req -= 1
                    sq.task_done()
                time.sleep(self.folderScanSleepTime)
                retry = False

        while sq.qsize() > 0:
            sq.get(block=False)
            sq.task_done()
        while fq.qsize() > 0:
            fq.get(block=False)
            fq.task_done()

        self.folder_threads -= 1
        return self.folders

    def listAll(self, folder: str = "root", trashed: bool = False, mimeType: str = "", **kwargs):
        '''Args: folder: search under this directory, trashed: show trashed \n
        return tuple (folderCount, fileCount)
        '''

        self.terminate = False
        writeCacheFuture = self.threadPoolExecutor.submit(self.writeToCache)

        folder_str: str = "'{0}' in parents".format(folder)
        m = hashlib.md5(folder_str.encode("ascii"))
        id: str = m.hexdigest()
        self.unfinished_file_req = 1
        self.unfinished_folder_req = 1
        self.searchFolderQueue.put(id, block=False)
        self.searchFileQueue.put(id, block=False)
        self.folderPageTokens[id] = (folder_str, None)
        self.filePageTokens[id] = (folder_str, None)
        folder_futures: list = list()
        file_futures: list = list()
        for i in range(8):
            folder_futures.append(self.threadPoolExecutor.submit(
                self._listFolders, folder=folder))
        for i in range(8):
            file_futures.append(self.threadPoolExecutor.submit(
                self._listFiles, folder=folder))

        while self.terminate == False and \
            (self.folderQueries.qsize() > 0 or self.searchFolderQueue.qsize() > 0
             or self.fileQueries.qsize() > 0 or self.searchFileQueue.qsize() > 0
             or self.unfinished_file_req > 0 or self.unfinished_folder_req > 0
             or self.fileWriteQueue.qsize() > 0 or self.folderWriteQueue.qsize() > 0):
            try:
                for i in folder_futures:
                    if i.done():
                        exc = i.exception(timeout=1)
                        raise exc
                for i in file_futures:
                    if i.done():
                        exc = i.exception(timeout=1)
                        raise exc
                if writeCacheFuture.done():
                    exc  = writeCacheFuture.exception(timeout=1)
                    raise exc
            except Exception as e:
                msg = e.args[0]
                msg = str(codecs.decode(msg, "ascii"))
                print(msg, end='')
            time.sleep(1)

        
        self.terminate = True
        return self.folderCount, self.fileCount

    def upload(self, file):
        pass

    def download(self, fileID: str):
        service = self.getService()

        request = service.files().get_media(fileId=fileID)
        fh = io.BytesIO()
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while done is False:
            status, done = downloader.next_chunk()
            print("Download {}.".format(int(status.progress() * 100)))

    def watchChanges(self):
        self.threadPoolExecutor.submit(self._startWatching)

    def _startWatching(self):
        pass


class FileStruct:
    def __init__(self):
        super().__init__()

    def children(self):
        pass


if __name__ == "__main__":
    back = time.time()
    d = DriveClient("/home/kie/test/.sync_ignore")
    d.authenticate()
    # folders, files = d.listAll("1QHfx3xUyKMvzPxqrI1AbEXadPnZjFs4Z")
    folders, files = d.listAll("root")
    print(folders)
    print(files)

    front = time.time()
    print(front-back)

    pass
