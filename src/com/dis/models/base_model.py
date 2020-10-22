#!/usr/bin/python
# -*- coding:utf-8 -*-

import sys
import platform
import os

IS_WINDOWS = platform.system() == 'Windows' or os.name == 'nt'

IS_PYTHON2 = sys.version_info.major == 2 or sys.version < '3'
IS_PYTHON35_UP = sys.version >= '3.5'
BASESTRING = basestring if IS_PYTHON2 else str

UNICODE = unicode if IS_PYTHON2 else str

LONG = long if IS_PYTHON2 else int

if IS_PYTHON2:
    from src.com.dis.models.model_python2 import _BaseModel
else:
    from src.com.dis.models.model_python3 import _BaseModel

BaseModel = _BaseModel






