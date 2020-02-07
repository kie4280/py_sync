from threadExecutor import ThreadPoolExecutorStackTraced as ThreadPoolExecutor
import time

from pathlib import Path
import threading
import queue



class Datastash:
    def __init__(self, cacheFolder: Path, filename: str):
        super().__init__()
        self.cacheFolder: Pathh = cacheFolder
        self.filename: str = filename
        self.__enable_write__: bool = False
        self.threadPoolExecutor = ThreadPoolExecutor(max_workers=1)
        self.fileWriteQueue: queue.Queue = queue.Queue(10000)
        self.exceptionQueue: queue.Queue = queue.Queue(200)
        self.future = None
        self.index: int = 0
    def start(self):
        self.future = self.threadPoolExecutor.submit(self._starWriting)
        
        return self.future

    def stop(self):
        self.__enable_write__ = False
        self.threadPoolExecutor.shutdown(True)
        pass
    def _startWriting(self):
        fq = self.fileWriteQueue
        # filewrite = None
        filewrite = self.cacheFolder.joinpath(
                self.filename+"_"+self.index)
        try:
            fileopen = filewrite.open("w+", encoding="UTF-8")

        except Exception as e:
            raise
        while self.__enable_write__:
            
            try:
                if filewrite.stat().st_size > 4000000:
                    pass
                data = fq.get(block=False)

            except queue.Empty:
                pass
            except IOError as e:
                raise
            except OSError as e:
                raise
            except Exception as e:
                raise
        
    def write(string: str):
        pass