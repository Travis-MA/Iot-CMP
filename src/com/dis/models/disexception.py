#!/usr/bin/env python
# encoding: utf-8

import json


class DisException(Exception):
    """The Exception of the log request & response.
    
    :type errorType: string
    :param errorType: log service error code 
    
    :type errorMessage: string
    :param errorMessage: detailed information for the exception
    
    """

    def __init__(self, errorCode, errorMessage, resp_status=0, serviceErrCode = "", serviceErrMsg = ""):
        self._errorCode = errorCode
        self._errorMessage = errorMessage
        self.respStatus = resp_status
        self.serviceErrCode = serviceErrCode
        self.serviceErrMsg = serviceErrMsg.replace('\\','')


    def __str__(self):
        # from src.com.dis.models.base_model import IS_PYTHON2
        # if IS_PYTHON2:
        #     return self.serviceErrMsg
        # else:
        #     try:
        #         return self.serviceErrMsg.lstrip('b')
        #     except:
        #         return self.serviceErrMsg

        return json.dumps({
            "errorCode": self._errorCode,
            "errorMessage": self._errorMessage,
            "respStatus":self.respStatus,
            "serviceErrCode": self.serviceErrCode,
            "serviceErrMsg": self.serviceErrMsg
        }, sort_keys=True)

    def getErrorType(self):
        """ return error code of exception
        
        :return: string, error code of exception.
        """
        return self._errorCode

    def getErrorMessage(self):
        """ return error message of exception
        
        :return: string, error message of exception.
        """
        return self._errorMessage
