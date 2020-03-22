import io
import os
import re
from shutil import copyfileobj as shutil_copyfileobj
from zipfile import ZipFile


class ZipPackage(object):
    def __init__(self, name_or_buffer):
        if isinstance(name_or_buffer, str):
            self._zf = ZipFile(name_or_buffer, mode='r')
            self.archive_name = os.path.abspath(name_or_buffer)
        elif isinstance(name_or_buffer, bytes):
            tf = io.BytesIO()
            tf.write(name_or_buffer)
            tf.seek(0)
            self._zf = ZipFile(tf)
            self.archive_name = ""
        elif isinstance(name_or_buffer, BytesIO):
            self._zf = ZipFile(name_or_buffer)
            self.archive_name = ""

        self.found_file = self.get_names()[0]
        
    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.close()
        
    def close(self):
        self._zf.close()
        
    def get_names(self):
        L = self._zf.namelist()
        return L

    def relpath(self, path):
        """
        Relative path with delimiter /
        """
        return os.path.relpath(path).replace("\\", "/")
    
    def get_file(self, name=""):
        if name is None:
            return io.BytesIO()
        if name == "":
            name = self.found_file

        get_name = name
        if name in self.get_names():
            get_name = name
        else:
            if isinstance(name, (int, float)):
                get_name = self.get_fname_by_index(name)
            else:
                raise ValueError(f"Can't find file in archive with name: {name} or {get_name}")
            
        tf = io.BytesIO()
        try:
            with self._zf.open(get_name, 'r') as zf:
                shutil_copyfileobj(zf, tf, -1)
            tf.seek(0)
            return tf
        except KeyError:
            tf.close()
            return None

    def get_fname_by_index(self, index):
        try:
            return self.get_names()[index]
        except:
            return self.get_names()[0]
            
    def get_fname_by_case_insensitive(self, name):
        name = self.relpath(name).lower()
        for fname in self.get_names():
            if name == self.relpath(fname).lower():
                self.found_file = fname
                return fname
        return None
    
    def get_fname_by_basename(self, name):
        name = os.path.basename(name).lower()
        for fname in self.get_names():
            if name == os.path.basename(fname).lower():
                self.found_file = fname
                return fname
        return None

    def get_fname_by_relpath(self, name):
        name =self.relpath(name).lower()
        for fname in self.get_names():
            if ("/"+ self.relpath(fname).lower()).endswith("/"+ name):
                self.found_file = fname
                return fname
        return None
    
    def get_fname_by_regex(self, regex):
        for fname in self.get_names():
            if re.search(regex.lower(), self.relpath(fname).lower()):
                self.found_file = fname
                return fname
        return None