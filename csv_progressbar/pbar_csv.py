import os
from typing import TypeAlias
from io import TextIOWrapper
import csv
from csv import Dialect
from dataclasses import dataclass

from .pbar_protocol import PbarProtocol

_DialectLike: TypeAlias = str | Dialect | type[Dialect]
_QuotingType: TypeAlias = int

def iter_csv(csvfile: TextIOWrapper,
        pbar: PbarProtocol,
        update_rate: int = 32768,
        dialect: _DialectLike = "excel",
        *,
        delimiter: str = ",",
        quotechar: str | None = '"',
        escapechar: str | None = None,
        doublequote: bool = True,
        skipinitialspace: bool = False,
        lineterminator: str = "\r\n",
        quoting: _QuotingType = 0,
        strict: bool = False):
    
    fd = csvfile.fileno()
    readed_rows = 0
    last_position = 0
    reader = csv.reader(csvfile, 
                                 dialect, 
                                 delimiter= delimiter,
                                 quotechar= quotechar,
                                 escapechar= escapechar,
                                 doublequote= doublequote,
                                 skipinitialspace= skipinitialspace,
                                 lineterminator = lineterminator,
                                 quoting= quoting,
                                 strict= strict)

    for row in reader:
        readed_rows += 1
        if readed_rows == update_rate:
            pos = os.lseek(fd, 0, os.SEEK_CUR)
            last_pos = last_position
            if last_pos != pos:
                pbar.update(pos - last_pos)
                last_position = pos
                readed_rows = 0
        yield row
        
    if readed_rows != 0:
        pos = os.lseek(fd, 0, os.SEEK_CUR)
        last_pos = last_position
        if last_pos != pos:
            pbar.update(pos - last_pos)

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

        self.reader = iter_csv(csvfile,
                                pbar,
                                update_rate,
                                dialect, 
                                delimiter= delimiter,
                                quotechar= quotechar,
                                escapechar= escapechar,
                                doublequote= doublequote,
                                skipinitialspace= skipinitialspace,
                                lineterminator = lineterminator,
                                quoting= quoting,
                                strict= strict)
        # self.reader = csv.reader(csvfile, 
        #                          dialect, 
        #                          delimiter= delimiter,
        #                          quotechar= quotechar,
        #                          escapechar= escapechar,
        #                          doublequote= doublequote,
        #                          skipinitialspace= skipinitialspace,
        #                          lineterminator = lineterminator,
        #                          quoting= quoting,
        #                          strict= strict)
        
    def __next__(self):
        return next(self.reader)
        
    def __iter__(self):
        return self.reader
        
        