import os
from typing import TypeAlias
from io import TextIOWrapper
import csv
from csv import Dialect
from dataclasses import dataclass

from .pbar_protocol import PbarProtocol

_DialectLike: TypeAlias = str | Dialect | type[Dialect]
_QuotingType: TypeAlias = int


class PbarCSV:
    
    def __init__(self, csvfile: TextIOWrapper,
        pbar: PbarProtocol,
        update_rate: int = 8196,
        dialect: _DialectLike = "excel",
        *,
        delimiter: str = ",",
        quotechar: str | None = '"',
        escapechar: str | None = None,
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: str = "\r\n",
        quoting: _QuotingType = 0,
        strict: bool = False) -> None:

        self.fd = csvfile.fileno()
        self.pbar = pbar
        self.update_rate = update_rate
        self.rows = 0
        self.last_position = 0
        self.reader = csv.reader(csvfile, 
                                 dialect, 
                                 delimiter= delimiter,
                                 quotechar= quotechar,
                                 escapechar= escapechar,
                                 doublequote= doublequote,
                                 skipinitialspace= skipinitialspace,
                                 lineterminator = lineterminator,
                                 quoting= quoting,
                                 strict= strict)
        
    def __next__(self):
        self.rows += 1
        if self.rows == self.update_rate:
            pos = os.lseek(self.fd, 0, os.SEEK_CUR)
            last_pos = self.last_position
            if last_pos != pos:
                self.pbar.update(pos - last_pos)
                self.last_position = pos
                self.rows = 0
        try:
            return next(self.reader)
        except StopIteration:
            pos = os.lseek(self.fd, 0, os.SEEK_CUR)
            last_pos = self.last_position
            if last_pos != pos:
                self.pbar.update(pos - last_pos)
            raise
        
    def __iter__(self):
        return self
        
        