#!/usr/bin/python
# -*- coding:utf-8 -*-

from src.com.dis.models.base_model import BaseModel, BASESTRING
class Logging(BaseModel):
    allowedAttr = {'targetBucket': BASESTRING, 'targetPrefix': BASESTRING, 'targetGrants': list}

    def __init__(self, targetBucket=None, targetPrefix=None, targetGrants=None):
        self.targetBucket = targetBucket
        self.targetPrefix = targetPrefix
        self.targetGrants = targetGrants
