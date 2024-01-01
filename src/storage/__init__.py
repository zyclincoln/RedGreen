# coding: utf-8
import sqlite3 as sqlite
from src.define import Singleton


@Singleton
class Storage(object):
    def __init__(self):
        self.conn = sqlite.connect('data.db')

