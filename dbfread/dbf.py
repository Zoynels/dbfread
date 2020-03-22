"""
Class to read DBF files.
"""
import os
import sys
import io
import datetime
import collections

from .ifiles import ifind
from .struct_parser import StructParser
from .field_parser import FieldParser
from .memo import find_memofile, open_memofile, FakeMemoFile, BinaryMemo
from .codepages import guess_encoding
from .dbversions import get_dbversion_string
from .exceptions import *
from .util_files import io_uni

DBFHeader = StructParser(
    'DBFHeader',
    '<BBBBLHHHBBLLLBBH',
    ['dbversion',
     'year',
     'month',
     'day',
     'numrecords',
     'headerlen',
     'recordlen',
     'reserved1',
     'incomplete_transaction',
     'encryption_flag',
     'free_record_thread',
     'reserved2',
     'reserved3',
     'mdx_flag',
     'language_driver',
     'reserved4',
     ])

DBFField = StructParser(
    'DBFField',
    '<11scLBBHBBBB7sB',
    ['name',
     'type',
     'address',
     'length',
     'decimal_count',
     'reserved1',
     'workarea_id',
     'reserved2',
     'reserved3',
     'set_fields_flag',
     'reserved4',
     'index_field_flag',
     'name_real',
     ])


def expand_year(year):
    """Convert 2-digit year to 4-digit year."""
    
    if year < 80:
        return 2000 + year
    else:
        return 1900 + year


class RecordIterator(object):
    def __init__(self, table, record_type):
        self._record_type = record_type
        self._table = table

    def __iter__(self):
        return self._table._iter_records(self._record_type)
 
    def __len__(self):
        return self._table._count_records(self._record_type)


class DBF(object):
    """DBF table."""
    def __init__(self, filename,
                 encoding=None,
                 ignorecase=True,
                 lowernames=False,
                 parserclass=FieldParser,
                 recfactory=collections.OrderedDict,
                 load=False,
                 raw=False,
                 ignore_missing_memofile=False,
                 char_decode_errors='strict',
                 fields_use_combined_name=False,
                 rename_fields_with_same_name=True,
                 read_files_to_memory=True):

        self.encoding = encoding
        self.ignorecase = ignorecase
        self.lowernames = lowernames
        self.parserclass = parserclass
        self.raw = raw
        self.ignore_missing_memofile = ignore_missing_memofile
        self.char_decode_errors = char_decode_errors
        self.fields_use_combined_name = fields_use_combined_name
        self.rename_fields_with_same_name = rename_fields_with_same_name
        self.read_files_to_memory = read_files_to_memory
        
        try:
            zfile = None
            self.fname = {}
            if isinstance(filename, (tuple, list, set)):
                self.fname["filename"] = filename[0]

                if len(filename) >= 2:
                    self.fname["dbf"] = filename[1]
                else:
                    self.fname["dbf"] = filename[0]

                if len(filename) >= 3:
                    self.fname["memo"] = filename[2]
                else:
                    self.fname["memo"] = None
            elif isinstance(filename, (dict)):
                pass
            else:
                self.fname["filename"] = filename
                self.fname["dbf"] = filename
                self.fname["memo"] = ""

            # Define zip-archive
            if self.fname["filename"].lower().endswith(".zip"):
                #from util_archive import ZipPackage
                zfile = io_uni(io, self.fname["filename"])
                zfile.to_ZipPackage()

                t = zfile.io.get_fname_by_case_insensitive(self.fname["dbf"])
                if t is not None:
                    self.fname["dbf"] = t
                else:
                    t = zfile.io.get_fname_by_regex("dbf")
                    if t is not None:
                        self.fname["dbf"] = t
                    else:
                        raise ValueError(f"Can't find dbf-file for input: {filename}")

            if self.fname["filename"].lower().endswith(".zip"):
                zfile.to_zipfile_ZipExtFile(self.fname["dbf"])
                #self.io = zfile.copy()
                self.io = zfile
                if self.read_files_to_memory:
                    self.io.to_bytesIO()
            else:
                self.io = io_uni(io, self.fname["dbf"])
                if self.read_files_to_memory:
                    self.io.to_bytesIO()



            if recfactory is None:
                self.recfactory = lambda items: items
            else:
                self.recfactory = recfactory
    
            # Name part before .dbf is the table name
            self.name = os.path.basename(filename)
            self.name = os.path.splitext(self.name)[0].lower()
            self._records = None
            self._deleted = None
    
            if ignorecase:
                self.filename = ifind(filename)
                if not self.filename:
                    raise DBFNotFound('could not find file {!r}'.format(filename))
            else:
                self.filename = filename
    
            # Filled in by self._read_headers()
            # self.memofilename = None
            self.header = None
            self.fields = []       # namedtuples
            self.field_names = []  # strings


            self._read_header(self.io)
            self._read_field_headers(self.io)
            self._check_headers()
            
            try:
                self.date = datetime.date(expand_year(self.header.year),
                                          self.header.month,
                                          self.header.day)
            except ValueError:
                # Invalid date or '\x00\x00\x00'.
                self.date = None

            # Find memo file and load it if necessary
            if self.fname["filename"].lower().endswith(".zip"):
                zfile = io_uni(io, self.fname["filename"])
                zfile.to_ZipPackage()
                self.fname["memo"] = self._get_memofilename(zfile.io.get_names())

                zfile.to_zipfile_ZipExtFile(self.fname["memo"])
                self.io_memo = zfile
                #self.io_memo = zfile.copy()
                if self.read_files_to_memory:
                    self.io_memo.to_bytesIO()
            else:
                self.fname["memo"] = self._get_memofilename()
                self.io_memo = io_uni(io, self.fname["memo"])
                if self.read_files_to_memory:
                    self.io_memo.to_bytesIO()


            if load:
                self.load()
        finally:
            pass
     #       if zfile is not None:
      #          zfile.close()
        
    @property
    def dbversion(self):
        return get_dbversion_string(self.header.dbversion)

    def _get_memofilename(self, fnames=None):
        # Does the table have a memo field?
        field_types = [field.type for field in self.fields]
        if not set(field_types) & set('MGPB'):
            # No memo fields.
            return None

        path = find_memofile(self.fname, fnames)
        if path is None:
            if self.ignore_missing_memofile:
                return None
            else:
                raise MissingMemoFile('missing memo file for {}'.format(
                    self.filename))
        else:
            return path

    @property
    def loaded(self):
        """``True`` if records are loaded into memory."""
        return self._records is not None

    def load(self, load_deleted=False, columns=None, nrows=None, convert_float=True):
        """Load records into memory.

        This loads both records and deleted records. The ``records``
        and ``deleted`` attributes will now be lists of records.

        """
        if not self.loaded:
            self._records = list(self._iter_records(b' ', columns=columns, nrows=nrows, convert_float=convert_float))
        if load_deleted:
            self._deleted = list(self._iter_records(b'*', columns=columns, nrows=nrows, convert_float=convert_float))

    def unload(self):
        """Unload records from memory.

        The records and deleted attributes will now be instances of
        ``RecordIterator``, which streams records from disk.
        """
        self._records = None
        self._deleted = None

    @property
    def records(self):
        """Records (not included deleted ones). When loaded a list of records,
        when not loaded a new ``RecordIterator`` object.
        """
        if self.loaded:
            return self._records
        else:
            return RecordIterator(self, b' ')

    @property
    def deleted(self):
        """Deleted records. When loaded a list of records, when not loaded a
        new ``RecordIterator`` object.
        """
        if self._deleted is not None:
            return self._deleted
        else:
            return RecordIterator(self, b'*')

    def _read_header(self, infile):
        # Todo: more checks?
        self.header = DBFHeader.read(infile)

        if self.encoding is None:
            try:
                self.encoding = guess_encoding(self.header.language_driver)
            except LookupError as err:
                self.encoding = 'ascii'

    def _decode_text(self, data):
        return data.decode(self.encoding, errors=self.char_decode_errors)

    def _read_field_headers(self, infile):
        colNames = []
        while True:
            sep = infile.read(1)
            if sep in (b'\r', b'\n', b''):
                # End of field headers
                break

            field = DBFField.unpack(sep + infile.read(DBFField.size - 1))

            field.type = chr(ord(field.type))

            # For character fields > 255 bytes the high byte
            # is stored in decimal_count.
            if field.type in 'C':
                field.length |= field.decimal_count << 8
                field.decimal_count = 0

            # Field name is b'\0' terminated.
            field.name_real = self._decode_text(field.name.split(b'\0')[0])
            field.name = field.name_real
            if self.fields_use_combined_name:
                field.name = f"{field.name},{field.type},{field.length},{field.decimal_count}"

            if self.lowernames:
                field.name = field.name.lower()

            # Rename fields if it is the same
            if self.rename_fields_with_same_name:
                if field.name in colNames:
                    i = 0
                    while True:
                        i += 1
                        colName = f"{field.name}.{i}"
                        if colName not in colNames:
                            field.name = colName
                            break
                colNames.append(field.name)

            self.field_names.append(field.name)

            self.fields.append(field)

    def _open_memofile(self):
        if self.fname["memo"] and not self.raw:
            return open_memofile(self.fname["memo"], self.io_memo.io, self.header.dbversion)
        else:
            return FakeMemoFile("")

    def _check_headers(self):
        field_parser = self.parserclass(self)

        """Check headers for possible format errors."""
        for field in self.fields:

            if field.type == 'I' and field.length != 4:
                message = 'Field type I must have length 4 (was {})'
                raise ValueError(message.format(field.length))

            elif field.type == 'L' and field.length != 1:
                message = 'Field type L must have length 1 (was {})'
                raise ValueError(message.format(field.length))

            elif not field_parser.field_type_supported(field.type):
                # Todo: return as byte string?
                raise ValueError('Unknown field type: {!r}'.format(field.type))

    def _skip_record(self, infile):
        # -1 for the record separator which was already read.
        infile.seek(self.header.recordlen - 1, 1)

    def _count_records(self, record_type=b' '):
        count = 0

        infile = self.io
        infile.seek(0)
        # Skip to first record.
        infile.seek(self.header.headerlen, 0)

        while True:
            sep = infile.read(1)
            if sep == record_type:
                count += 1
                self._skip_record(infile)
            elif sep in (b'\x1a', b''):
                # End of records.
                break
            else:
                self._skip_record(infile)

        return count

    def _iter_records(self, record_type=b' ', columns=None, nrows=None, convert_float=False):
        infile = self.io
        memofile = self._open_memofile()
        infile.seek(0)
        memofile._seek(0)


        # Skip to first record.
        infile.seek(self.header.headerlen, 0)

        if not self.raw:
            field_parser = self.parserclass(self, memofile)
            parse = field_parser.parse

        # Shortcuts for speed.
        skip_record = self._skip_record
        read = infile.read

        self.columns = [f.name for f in self.fields] if columns is None else columns

        while True:
            sep = read(1)

            if sep == record_type:
                if nrows is not None:
                    if nrows > 0:
                        nrows -= 1
                    else:
                        break

                if self.raw:
                    d = {}
                    for field in self.fields:
                        d[(field.name, field.name_real, field.type)] = (field.name, read(field.length))
                else:
                    d = {}
                    for field in self.fields:
                        d[(field.name, field.name_real, field.type)] = (field.name, parse(field, read(field.length)))

                items = []
                for name, name_real, field_type in d:
                    if (name not in self.columns) and (name_real not in self.columns):
                        continue

                    val = d[(name, name_real, field_type)]
                    if (convert_float) and (field_type == "N"):
                        if val[1] is not None:
                            if val[1] == int(val[1]):
                                val = tuple([val[0], int(val[1])])
                    items.append(val)

                yield self.recfactory(items)

            elif sep in (b'\x1a', b''):
                # End of records.
                break
            else:
                skip_record(infile)

    def __iter__(self):
        if self.loaded:
            return list.__iter__(self._records)
        else:
            return self._iter_records()

    def __len__(self):
        return len(self.records)

    def __repr__(self):
        if self.loaded:
            status = 'loaded'
        else:
            status = 'unloaded'
        return '<{} DBF table {!r}>'.format(status, self.filename)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.io.close()
        self.io_memo.close()
        self.unload()
        return False
