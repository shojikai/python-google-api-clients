class AlreadyExistsError(Exception):
    pass

class BigQueryError(Exception):
    pass

class DatasetIsNotEmptyError(Exception):
    pass

class Http4xxError(Exception):
    pass

class Http5xxError(Exception):
    pass

class InsertError(Exception):
    pass

class InvalidRowError(Exception):
    pass

class JobWaitTimeoutError(Exception):
    pass

class LoadError(Exception):
    pass

class NoSuchFieldError(Exception):
    pass

class NotFoundError(Exception):
    pass

class ParameterError(Exception):
    pass
