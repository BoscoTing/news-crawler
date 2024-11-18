class ScraperException(Exception):
    pass

class DownloadException(ScraperException):
    def __init__(self, message: str, url: str):
        super().__init__(message)
        self.url = url

class ParseException(ScraperException):
    pass

class StorageException(ScraperException):
    pass