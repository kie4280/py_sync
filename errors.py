import re
import googleapiclient.errors as apiErrors

class GetServiceError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class AuthenticateError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class RequestError:

    RATE_LIMIT_EXCEEDED = "User Rate Limit Exceeded"
    BATCH_ERROR = "Batch Error"
    INVALID_VALUE = "Invalid Value"

    def __init__(self, *args, **kwargs):
        
        if len(args) > 0:
            self.res = args[0]
        else:
            self.res = None

    def getCause(self):
        if self.res != None and isinstance(self.res, apiErrors.HttpError):
            error: apiErrors.HttpError = self.res
            
            responseCode: str = error.resp.status

            user_rate = re.search(r"User Rate Limit Exceeded.", error._get_reason())
            invalid_value = re.search(r"Invalid Value", error._get_reason())
            if user_rate != None:
                return RequestError.RATE_LIMIT_EXCEEDED
            elif invalid_value != None:
                return RequestError.INVALID_VALUE
        elif self.res != None and isinstance(self.res, apiErrors.BatchError):

            return RequestError.BATCH_ERROR
class DriveError(Exception):

    INVALID_SYNC_FOLDER = "Invalid sync folder"
    NO_PERMISSION = "No permission"
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._cause = args[0]
    def getCause(self):
        return self._cause