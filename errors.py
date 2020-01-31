import re
import googleapiclient.errors as apiErrors

class GetServiceError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)


class AuthenticateError(Exception):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class RequestError(Exception):

    RATE_LIMIT_EXCEEDED = "User Rate Limit Exceeded"
    BATCH_ERROR = "Batch Error"


    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        if len(args) > 0:
            self.res = args[0]
        else:
            self.res = None

    def getCause(self):
        if self.res != None and isinstance(self.res, apiErrors.HttpError):
            error: apiErrors.HttpError = self.res
            
            responseCode: str = error.resp.status

            match = re.search(r"User Rate Limit Exceeded.", error._get_reason())
            if match != None:
                return RequestError.RATE_LIMTT_EXCEEDED
        elif self.res != None and isinstance(self.res, apiErrors.BatchError):

            return RequestError.BATCH_ERROR
            