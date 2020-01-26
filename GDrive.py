from __future__ import print_function
import pickle
import os.path
from googleapiclient.discovery import build
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

# If modifying these scopes, delete the file token.pickle.
SCOPES = ['https://www.googleapis.com/auth/drive.readonly',
          'https://www.googleapis.com/auth/drive.metadata.readonly'
          ]
class GetServiceError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
    

class DriveClient:

    def __init__(self):
        super().__init__()
        self.creds = None
        self.service = None
    def getService(self):
        if self.service != None:
            return self.service
        else:
            raise GetServiceError()
        return None

    def authenticate(self, credentials: str = "credentials.json", tokens:str = "token.pickle"):
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
    def listFiles(self, folder:str='root', trashed:bool=False, mimeType:str="",**kwargs):
        trash_str = "true" if trashed else "false"
        query ="'{0}' in parents and trashed={1}".format(folder, trash_str, )

        results = self.getService().files().list(
            pageSize=1000, fields="nextPageToken, files(id, name, md5Checksum, mimeType)",
            q=query, spaces="drive"
        ).execute()

        return result

    def upload(self):
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

class FileStruct:
    def __init__(self):
        super().__init__()
    def children(self):
        pass



if __name__ == "__main__":
    d = DriveClient()
    d.authenticate()
    d.listFiles()
    pass
