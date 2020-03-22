import io
import zipfile
from .util_archive import ZipPackage
from shutil import copyfileobj as shutil_copyfileobj
import copy

class io_uni(object):
    def __init__(self, input_io, fname=None):
        self.io = input_io

        if isinstance(self.io, io.BytesIO):
            self.type = "io.BytesIO"
            self.f = self.io
        elif isinstance(self.io, io.BufferedReader):
            self.f = self.io
            self.type = "io.BufferedReader"
        elif isinstance(self.io, zipfile.ZipFile):
            self.type = "zipfile.ZipFile"
            self.f = self.io.open(fname, mode = "r")
        elif isinstance(self.io, zipfile.ZipExtFile):
            self.f = self.io.open(fname, mode = "r")
            self.type = "zipfile.ZipExtFile"
        elif isinstance(self.io, ZipPackage):
            self.type = "ZipPackage"
            self.f = self.io._zf
        elif input_io.__name__ == "io":
            self.io = io
            if fname == "":
                self.f = io.BytesIO()
            else:
                self.f = self.io.open(fname, mode = "rb")
            self.type = "io"


    def to_bytesIO(self):
        tf = io.BytesIO()
        if isinstance(self.io, zipfile.ZipFile):
            try:
                tf.write(self.f.read())
                tf.seek(0)
                self.io = tf
                self.f = tf
            except KeyError:
                tf.close()
        else:
            try:
                self.f.seek(0)
                shutil_copyfileobj(self.f, tf, -1)
                tf.seek(0)
                self.io = tf
                self.f = tf
            except KeyError:
                tf.close()
        return None

    def to_ZipPackage(self):
        if isinstance(self.io, io.BytesIO):
            self.seek(0)
            self.io = ZipPackage(self.read())
        elif self.io.__name__ == "io":
            self.io = ZipPackage(self.f.name)

        self.f = self.io._zf

    def to_zipfile_ZipExtFile(self, fname):
        if fname is None:
            self.io = io.BytesIO()
            self.f = self.io
        else:
            self.io = self.io._zf
            self.f = self.io.open(fname, 'r')

    def read(self, size=-1):
        return self.f.read(size)

    def seek(self, offset=0, whence=0):
        self.f.seek(offset, whence)

    def copy(self):
        return copy.deepcopy(self)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()

    def close(self):
        self.f.close()
        self.io.close()