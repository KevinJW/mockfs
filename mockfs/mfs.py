"""mockfs: A simple mock filesystem for unit tests."""

import os
import copy
import errno
import fnmatch
import glob
import shutil

from mockfs import util

# Python functions to replace
builtins = {
        'os.chdir': os.chdir,
        'os.getcwd': os.getcwd,
        'os.getcwdu': os.getcwdu,
        'os.path.abspath': os.path.abspath,
        'os.path.exists': os.path.exists,
        'os.path.getsize': os.path.getsize,
        'os.path.islink': os.path.islink,
        'os.path.isdir': os.path.isdir,
        'os.path.isfile': os.path.isfile,
        'os.walk': os.walk,
        'os.listdir': os.listdir,
        'os.makedirs': os.makedirs,
        'os.remove': os.remove,
        'os.rmdir': os.rmdir,
        'glob.glob': glob.glob,
        'shutil.rmtree': shutil.rmtree,
}

# We use the original abspath()
_abspath_builtin = builtins['os.path.abspath']


def _OSError(err, path):
    """Return an OSError with an appropriate error string"""
    return OSError(err, os.strerror(err) + ": '%s'" % path)


class MockFS(object):
    """
    MockFS implementation object

    Provides stubs for functions in :mod:`os`, :mod:`os.path`, and :mod:`glob`.

    """

    def __init__(self):
        self.cwd = Cwd(self)
        self._entries = {}

    def add_entries(self, entries):
        """Add new entries to mockfs."""
        new_entries = util.build_nested_dict(entries)
        util.merge_dicts(new_entries, self._entries)

    def exists(self, path):
        """
        Return True if path exists

        Implements the :func:`os.path.exists` interface.

        """
        path = self.abspath(path)
        dirent = self._direntry(os.path.dirname(path))
        if path == '/':
            return bool(dirent)
        return bool(dirent) and os.path.basename(path) in dirent

    def getsize(self, path):
        """Return the size of a file, reported by os.stat()."""
        entry = self._direntry(path)
        if entry is None:
            raise _OSError(errno.ENOENT, path)
        return len(entry)

    def isdir(self, path):
        """
        Return True if path is a directory

        Implements the :func:`os.path.isdir` interface.

        """
        return util.is_dir(self._direntry(path))

    def isfile(self, path):
        """
        Return True if path is a file

        Implements the :func:`os.path.isfile` interface.

        """
        return util.is_file(self._direntry(path))

    def islink(self, path):
        """
        Return True if path is a symlink

        .. note::

            Currently hard-wired to return False

        """
        return False

    def makedirs(self, path):
        """Create directory entries for a path"""
        path = self.abspath(path)
        new_entries = util.build_nested_dir_dict(path)
        util.merge_dicts(new_entries, self._entries)

    def abspath(self, path):
        if os.path.isabs(path):
            # Folds '////' into '/'
            return _abspath_builtin(path)
        curdir = self.cwd.getcwd()
        return _abspath_builtin(os.path.join(curdir, path))

    def listdir(self, path):
        """
        Return the directory contents of 'path'

        Implements the :func:`os.listdir` interface.
        :param path: filesystem path

        """
        direntry = self._direntry(path)
        if direntry is None:
            raise _OSError(errno.ENOENT, path)
        if util.is_file(direntry):
            raise _OSError(errno.ENOTDIR, path)
        if util.is_dir(direntry):
            return list(sorted(direntry.keys()))
        raise _OSError(errno.EINVAL, path)

    def walk(self, path):
        """
        Walk a filesystem path

        Implements the :func:`os.walk` interface.

        """
        path = self.abspath(path)
        inspect = [path]
        while True:
            dirstack = []
            for entry in inspect:
                dirent = self._direntry(entry)
                dirs = []
                files = []
                if dirent:
                    for e in dirent:
                        if type(dirent[e]) is dict:
                            dirs.append(e)
                        else:
                            files.append(e)
                yield (entry, dirs, files)
                dirstack.extend([os.path.join(entry, d) for d in dirs])
            inspect = dirstack
            if not inspect:
                raise StopIteration

    def remove(self, fspath):
        """Remove the entry for a file path

        Implements the :func:`os.remove` interface.

        """
        path = self.abspath(fspath)
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        entry = self._direntry(dirname)
        if not util.is_dir(entry):
            raise _OSError(errno.EPERM, path)

        try:
            fsentry = entry[basename]
        except KeyError:
            raise _OSError(errno.ENOENT, path)

        if not util.is_file(fsentry):
            raise _OSError(errno.EPERM, path)

        del entry[basename]

    def rmdir(self, fspath):
        """Remove the entry for a directory path

        Implements the :func:`os.rmdir` interface.

        """
        path = self.abspath(fspath)
        dirname = os.path.dirname(path)
        basename = os.path.basename(path)
        entry = self._direntry(dirname)
        if not util.is_dir(entry):
            raise _OSError(errno.ENOENT, path)

        try:
            direntry = entry[basename]
        except KeyError:
            raise _OSError(errno.ENOENT, fspath)

        if not util.is_dir(direntry):
            raise _OSError(errno.ENOTDIR, fspath)

        if len(direntry) != 0:
            raise _OSError(errno.ENOTEMPTY, fspath)

        del entry[basename]

    def copytree(self, src, dst):
        """Copy a directory subtree

        Implements the :func:`shutil.copytree` interface.

        """
        src_d = self._direntry(src)
        if src_d is None:
            raise _OSError(errno.ENOENT, src)
        dst = self.abspath(dst)
        dst_d_parent = self._direntry(os.path.dirname(dst))
        dst_d_parent[os.path.basename(dst)] = copy.deepcopy(src_d)

    def rmtree(self, path):
        abspath = self.abspath(path)
        entry = self._direntry(abspath)
        if entry is None:
            raise _OSError(errno.ENOENT, entry)

        if not self.isdir(path):
            raise _OSError(errno.ENOTDIR, path)

        dirname = os.path.dirname(abspath)
        dirent = self._direntry(dirname)
        if dirent is None:
            raise _OSError(errno.ENOENT, dirname)

        if dirname == '/':
            # Do not allow removing the root
            raise _OSError(errno.ENOPERM, '/')

        basename = os.path.basename(path)
        if basename not in dirent:
            raise _OSError(errno.ENOENT, path)

        # Remove the directory
        del dirent[basename]

    def glob(self, pattern):
        """Implementation of :py:func:`glob.glob`"""
        # Keep relative glob paths relative
        if os.path.isabs(pattern):
            prefix = None
        else:
            prefix = self.cwd.getcwd()
            if prefix != '/':
                prefix += '/'

        pattern = self.abspath(pattern)
        if pattern == '/':
            return ['/']

        # Keep track of current likely candidate paths.
        # Each time we filter down, take the new candidates
        # and append their names to create new candidates paths.
        patterns = pattern.split('/')[1:]
        entries = [('', self._entries)]
        match = fnmatch.fnmatch
        path_stack = []
        pattern_stack = ['']
        paths = []

        for idx, subpattern in enumerate(patterns):
            pattern_stack.append(subpattern)
            pattern = '/'.join(pattern_stack)

            new_entries = []
            new_paths = []

            for subdir, entry in entries:
                path_stack.append(subdir)
                for path in sorted(entry):
                    path_stack.append(path)
                    abspath = '/'.join(path_stack)
                    if match(abspath, pattern):
                        new_entries.append((abspath, entry[path]))
                        new_paths.append(abspath)
                    path_stack.pop()
                path_stack.pop()

            entries = new_entries
            paths = new_paths

        if prefix is None:
            return paths
        else:
            return [p[len(prefix):] for p in paths]

    ## Internal Methods
    def _direntry(self, fspath):
        """Return the directory "dict" entry for a path"""
        path = self.abspath(fspath)
        if path == '/':
            return self._entries
        elts = path.split('/')[1:]
        current = self._entries
        retval = None
        for elt in elts:
            if elt in current:
                retval = current[elt]
                current = current[elt]
            else:
                return None
        return retval


class Cwd(object):
    def __init__(self, mfs):
        self._cwd = '/'
        self._mfs = mfs

    def chdir(self, path):
        # Make it absolute
        if os.path.isabs(path):
            cdpath = path
        else:
            cdpath = os.path.join(self._cwd, path)

        entry = self._mfs._direntry(path)
        if entry is None:
            raise _OSError(errno.ENOENT, path)
        elif not util.is_dir(entry):
            raise _OSError(errno.ENOTDIR, path)

        self._cwd = _abspath_builtin(cdpath)

    def getcwd(self):
        return self._cwd

    def getcwdu(self):
        return self._cwd
