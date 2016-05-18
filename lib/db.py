#!/usr/bin/env python
# encoding: utf-8
"""
Created by Ben Scott on '17/05/2016'.
"""

import os
import sqlite3

def db_connect():
    """
    Connect to the SQLite DB
    :return:
    """

    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'data')

    db = os.path.join(data_dir, 'stats.db')

    if not os.path.isfile(db):
        raise IOError('Stats.db does not exist')

    return sqlite3.connect(db)