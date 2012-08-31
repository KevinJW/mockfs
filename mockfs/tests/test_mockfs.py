import unittest
# subjects under test
import glob
import os
import shutil

import mockfs


class MockFSTestCase(unittest.TestCase):
    def setUp(self):
        self.mfs = mockfs.install()

    def tearDown(self):
        mockfs.uninstall()

    def _mkfs(self):
        filesystem = {
            '/a/a/a': {}, '/a/a/b': '',
            '/a/b/a': {}, '/a/b/b': '',
            '/b/a/a': {}, '/b/a/b': '',
            '/b/b/a': {}, '/b/b/b': '',
        }
        self.mfs.add_entries(filesystem)

    def test_empty(self):
        """Test an empty filesystem"""
        self.assertFalse(os.path.exists('/'))

    def test_first_level_subdir(self):
        """Test files at the root of the filesystem"""
        self.mfs.add_entries({'/foo': 'bar'})
        self.assertTrue(os.path.exists('/'))
        self.assertTrue(os.path.isdir('/'))
        self.assertTrue(os.path.exists('/foo'))

    def test_nested_directories(self):
        """Test files in subdirectories."""
        filesystem = {
            '/a/a/a': '',
            '/a/a/b': '',
            '/a/b/a': '',
            '/a/b/b': '',
            '/b/a/a': '',
            '/b/a/b': '',
            '/b/b/a': '',
            '/b/b/b': '',
        }
        self.mfs.add_entries(filesystem)

        for path in filesystem:
            self.assertTrue(os.path.isdir(os.path.dirname(path)))
            self.assertTrue(os.path.exists(path))
            self.assertTrue(os.path.isfile(path))

    def test_unsanitized_entries(self):
        """Test add_entries() with unsantized paths"""
        self.mfs.add_entries({'///just////another////pythonista': ''})

        self.assertTrue(os.path.isdir('/just'))
        self.assertTrue(os.path.isdir('/just/another'))
        self.assertTrue(os.path.exists('/just/another/pythonista'))
        self.assertTrue(os.path.isfile('/just/another/pythonista'))

    def test_predicates_on_unsanitized_paths(self):
        """Test isdir(), isfile() and exists() on unsanitized paths"""
        self.mfs.add_entries({'/just/another/pythonista': ''})

        self.assertTrue(os.path.isdir('///just'))
        self.assertTrue(os.path.isdir('///just/////another'))
        self.assertTrue(os.path.exists('///just////another////////pythonista'))
        self.assertTrue(os.path.isfile('///just////another////////pythonista'))

    def test_os_mkdir(self):
        filesystem = {
            '/file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertEqual(os.listdir('/'), ['file'])

        os.mkdir('directory')

        self.assertTrue(os.path.lexists('/directory'))
        self.assertTrue(os.path.exists('/directory'))
        self.assertTrue(os.path.isdir('/directory'))
        self.assertEqual(os.listdir('/directory'), [])

        os.mkdir('directory/nested')
        self.assertTrue(os.path.isdir('/directory/nested'))
 
        os.rmdir('/directory/nested')
        self.assertEqual(os.listdir('/directory'), [])

    def test_os_remove(self):
        filesystem = {
            '/a/a': '',
            '/a/b': '',
            '/a/c': '',
        }
        self.mfs.add_entries(filesystem)
        self.assertTrue(os.path.isfile('/a/a'))
        self.assertTrue(os.path.isfile('/a/b'))
        self.assertTrue(os.path.isfile('/a/c'))

        os.remove('/a/a')
        self.assertFalse(os.path.isfile('/a/a'))

        os.remove('/a/c')
        self.assertFalse(os.path.isfile('/a/c'))

        os.remove('/a/b')
        self.assertFalse(os.path.isfile('/a/b'))

        self.assertEqual(os.listdir('/a'), [])

    def test_os_remove_does_not_exist(self):
        filesystem = {
            '/a/a': '',
            '/a/b': '',
            '/a/c': '',
        }
        self.mfs.add_entries(filesystem)
        self.assertRaises(OSError, os.remove, '/a/does-not-exist')

    def test_os_remove_dir(self):
        self._mkfs()
        self.assertRaises(OSError, os.remove, '/a')

    def test_remove_from_subdir(self):
        self._mkfs()
        os.chdir('/a/a')
        os.remove('b')

        entries = os.listdir('/a/a')
        self.assertEqual(entries, ['a'])

        entries = os.listdir('.')
        self.assertEqual(entries, ['a'])

    def test_os_rmdir(self):
        filesystem = {
            '/a/a': '',
            '/a/b': '',
            '/a/c': '',
        }
        self.mfs.add_entries(filesystem)
        self.assertRaises(OSError, os.rmdir, '/a')

        os.remove('/a/a')
        os.remove('/a/c')
        os.remove('/a/b')
        os.rmdir('/a')
        self.assertFalse(os.path.isdir('/a'))

    def test_os_rmdir_file(self):
        self._mkfs()
        self.assertRaises(OSError, os.rmdir, '/a/a/b')

    def test_os_rmdir_nonempty(self):
        self._mkfs()
        os.remove('/a/a/b')
        self.assertRaises(OSError, os.rmdir, '/a/a')

        os.rmdir('/a/b/a')
        self.assertRaises(OSError, os.rmdir, '/a/b')

    def test_os_makedirs(self):
        os.makedirs('/foo/bar/baz')
        self.assertTrue(os.path.isdir('/'))
        self.assertTrue(os.path.isdir('/foo'))
        self.assertTrue(os.path.isdir('/foo/bar'))
        self.assertTrue(os.path.isdir('/foo/bar/baz'))

    def test_os_readlink_does_not_exist(self):
        self.assertFalse(os.path.exists('/not_exist'))
        self.assertRaises(OSError, os.readlink, '/not_exist')

    def test_os_readlink(self):
        filesystem = {
            '/link': ['destination'],
            'destination': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertTrue(os.path.exists('/link'))
        self.assertEqual(os.readlink('/link'), 'destination')
        values = os.listdir('/')
        self.assertEqual(values, ['destination', 'link'])
        values = os.listdir('.')
        self.assertEqual(values, ['destination', 'link'])

    def test_symlink_to_directory(self):
        filesystem = {
            '/link': ['dir'],
            '/dir/file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertTrue(os.path.isdir('/dir'))
        self.assertTrue(os.path.exists('/link'))
        self.assertEqual(os.readlink('/link'), 'dir')
        self.assertTrue(os.path.islink('/link'))
        self.assertEqual(os.path.realpath('/link'), '/dir')
        self.assertEqual(os.path.realpath('/link/file'), '/dir/file')
        self.assertTrue(os.path.isdir('/link'))
        self.assertTrue(os.path.isfile('/link/file'))
        values = os.listdir('.')
        self.assertEqual(values, ['dir', 'link'])
        os.chdir('/link')
        self.assertEqual(os.getcwd(), '/dir')
        values = os.listdir('.')
        self.assertEqual(values, ['file'])
        values = os.listdir(os.getcwd())
        self.assertEqual(values, ['file'])
        values = glob.glob('*')
        self.assertEqual(values, ['file'])

    def test_os_symlink(self):
        filesystem = {
            '/dir/file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertTrue(os.path.isdir('/dir'))
        self.assertFalse(os.path.exists('/link'))

        os.symlink('dir/file', '/link')

        self.assertTrue(os.path.exists('/link'))
        self.assertEqual(os.readlink('/link'), 'dir/file')
        self.assertTrue(os.path.islink('/link'))
        self.assertEqual(os.path.realpath('/link'), '/dir/file')
        self.assertTrue(os.path.isfile('/link'))

    def test_os_symlink_create_dangling_link(self):
        filesystem = {
            '/file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertEqual(os.listdir('/'), ['file'])

        os.symlink('contents', '/link')

        self.assertTrue(os.path.lexists('/link'))
        self.assertFalse(os.path.exists('/link'))
        self.assertEqual(os.readlink('/link'), 'contents')
        self.assertTrue(os.path.islink('/link'))
        self.assertEqual(os.path.realpath('/link'), '/contents')
        self.assertFalse(os.path.isfile('/link'))

    def test_os_symlink_fails_non_existing_dir(self):
        filesystem = {
            'file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertRaises(OSError, os.symlink, 'anything', 'file')
        self.assertRaises(OSError, os.symlink, 'file', 'not-a-dir/link')

    def test_symlink_to_file(self):
        filesystem = {
            '/link': ['dir/file'],
            '/dir/file': ''
        }
        self.mfs.add_entries(filesystem)
        self.assertTrue(os.path.isdir('/dir'))
        self.assertTrue(os.path.exists('/link'))
        self.assertEqual(os.readlink('/link'), 'dir/file')
        self.assertTrue(os.path.islink('/link'))
        self.assertEqual(os.path.realpath('/link'), '/dir/file')
        self.assertTrue(os.path.isfile('/link'))

    def test_dangling_symlink(self):
        filesystem = {
            '/link-to-file': ['file'],
            '/link-to-inside-dir': ['dir/file'],
            '/dir': {}
        }
        self.mfs.add_entries(filesystem)
        self.assertEqual(os.path.realpath('/link-to-file'), '/file')
        self.assertEqual(os.path.realpath('/link-to-inside-dir'), '/dir/file')
        self.assertTrue(os.path.isdir('/dir'))
        self.assertTrue(os.path.islink('/link-to-file'))
        self.assertTrue(os.path.islink('/link-to-inside-dir'))
        self.assertTrue(os.path.lexists('/link-to-file'))
        self.assertTrue(os.path.lexists('/link-to-inside-dir'))
        self.assertFalse(os.path.exists('/link-to-file'))
        self.assertFalse(os.path.exists('/link-to-inside-dir'))
        
    def test_os_readlink_not_a_link(self):
        self._mkfs()
        self.assertTrue(os.path.isdir('/a'))
        self.assertFalse(os.path.islink('/a'))
        self.assertRaises(OSError, os.readlink, '/a')
        
        self.assertTrue(os.path.isfile('/a/a/b'))
        self.assertFalse(os.path.islink('/a/a/b'))
        self.assertRaises(OSError, os.readlink, '/a/a/b')

    def test_glob_head(self):
        self._mkfs()
        values = glob.glob('/*/a/a')
        self.assertEqual(values, ['/a/a/a', '/b/a/a'])

    def test_glob_mid(self):
        self._mkfs()
        values = glob.glob('/a/*/a')
        self.assertEqual(values, ['/a/a/a', '/a/b/a'])

    def test_glob_tail(self):
        """Test files in subdirectories."""
        self._mkfs()
        values = glob.glob('/a/a/*')
        self.assertEqual(values, ['/a/a/a', '/a/a/b'])

    def test_glob_multi(self):
        self._mkfs()
        values = glob.glob('/*/a/*')
        self.assertEquals(values, ['/a/a/a', '/a/a/b', '/b/a/a', '/b/a/b'])

    def test_chdir(self):
        self._mkfs()
        os.chdir('/a/a/a')
        self.assertEqual(os.getcwd(), '/a/a/a')

    def test_chdir_file(self):
        self._mkfs()
        self.assertRaises(OSError, os.chdir, '/a/a/b')

    def test_chdir_bogus(self):
        self._mkfs()
        self.assertRaises(OSError, os.chdir, '/does/not/exist')

    def test_cwd(self):
        self._mkfs()
        os.chdir('/a')
        self.assertEqual(os.getcwd(), '/a')

    def test_cwd_rogue(self):
        self._mkfs()
        os.chdir('/////a/////')
        self.assertEqual(os.getcwd(), '/a')

    def test_chdir_and_relative_glob_from_root(self):
        self._mkfs()
        os.chdir('/')
        globs = glob.glob('a/*/a')
        self.assertEqual(globs, ['a/a/a', 'a/b/a'])

    def test_chdir_and_relative_glob_from_subdir(self):
        self._mkfs()
        os.chdir('/a')
        globs = glob.glob('*/a')
        self.assertEqual(globs, ['a/a', 'b/a'])

    def test_listdir_from_subdir(self):
        filesystem = {
                '/a/a': '',
                '/a/b': '',
                '/a/c/c': '',
        }
        self.mfs.add_entries(filesystem)
        os.chdir('/a')
        entries = os.listdir('.')
        self.assertEqual(entries, ['a', 'b', 'c'])

    def test_os_getsize(self):
        filesystem = {
                '/a/a': '',
                '/a/b': '{}',
        }
        self.mfs.add_entries(filesystem)

        a_size = os.path.getsize('/a/a')
        self.assertEqual(a_size, 0)

        b_size = os.path.getsize('/a/b')
        self.assertEqual(b_size, 2)

    def test_os_getsize_directory(self):
        filesystem = {
                '/a/a': '',
                '/a/b': '',
        }
        self.mfs.add_entries(filesystem)
        dir_size = os.path.getsize('/a')
        self.assertEqual(dir_size, 2)

    def test_os_getsize_subdir(self):
        filesystem = {
                '/a/a': '',
                '/a/b': '{}',
        }
        self.mfs.add_entries(filesystem)
        os.chdir('/a')
        a_size = os.path.getsize('a')
        self.assertEqual(a_size, 0)

        b_size = os.path.getsize('b')
        self.assertEqual(b_size, 2)

    def test_shutil_rmtree(self):
        filesystem = {
                '/a/a': '',
                '/a/b': '',
        }
        self.mfs.add_entries(filesystem)

        shutil.rmtree('/a')
        self.assertEqual(os.listdir('/'), [])

def suite():
    suite = unittest.TestSuite()
    suite.addTest(unittest.makeSuite(MockFSTestCase))
    return suite


if __name__ == '__main__':
    unittest.main()
