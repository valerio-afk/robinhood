from filesystem import NTAbstractPath, MissingAbsolutePathException, PathOutsideRootException
import unittest

class TestNTPathManager(unittest.TestCase):
    def test_basic(this):
        path = NTAbstractPath("c:/")
        this.assertEqual(path.absolute_path,"c:/")
        this.assertEqual(path.relative_path,".")

        path = NTAbstractPath("c:/a/b", "c:/")
        this.assertEqual(path.absolute_path, "c:/a/b")
        this.assertEqual(path.relative_path, "a/b")


    def test_basic_trailing_slash(this):
        path = NTAbstractPath("c:/a/b/", "c:/")
        this.assertEqual(path.absolute_path, "c:/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path = NTAbstractPath("c:/a/b/", "c:/a")
        this.assertEqual(path.absolute_path, "c:/a/b")
        this.assertEqual(path.relative_path, "b")

        path = NTAbstractPath("c:/a/b/", "c:/a/")
        this.assertEqual(path.absolute_path, "c:/a/b")
        this.assertEqual(path.relative_path, "b")

    def test_parent_dirs(this):
        path = NTAbstractPath("c:/a/../b", "c:/")
        this.assertEqual(path.absolute_path, "c:/b")
        this.assertEqual(path.relative_path, "b")

        path = NTAbstractPath("c:/a/../b/../", "c:/")
        this.assertEqual(path.absolute_path, "c:/")
        this.assertEqual(path.relative_path, ".")

        path = NTAbstractPath("c:/a/../b", "c:/..")
        this.assertEqual(path.absolute_path, "c:/b")
        this.assertEqual(path.relative_path, "b")

        path = NTAbstractPath("c:/a/../b/../", "c:/..")
        this.assertEqual(path.absolute_path, "c:/")
        this.assertEqual(path.relative_path, ".")

        path = NTAbstractPath("c:/a/../b", "c:/a/..")
        this.assertEqual(path.absolute_path, "c:/b")
        this.assertEqual(path.relative_path, "b")

        path = NTAbstractPath("c:/a/../b/../", "c:/a/..")
        this.assertEqual(path.absolute_path, "c:/")
        this.assertEqual(path.relative_path, ".")

    def test_change_dirs_relative(this):
        path = NTAbstractPath("c:/root/a/", "c:/root")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        path.cd("b")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd(".")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd("..")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        path = NTAbstractPath("c:/root/a/", "c:/root")
        path.cd("b/")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd(".")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd("..")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        path = NTAbstractPath("c:/root/a", "c:/root")

        path.cd("./b/c")
        this.assertEqual(path.absolute_path, "c:/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.cd("./d")
        this.assertEqual(path.absolute_path, "c:/root/a/b/c/d")
        this.assertEqual(path.relative_path, "a/b/c/d")

        path = NTAbstractPath("c:/root/a/", "c:/root")

        path.cd("./b/c/")
        this.assertEqual(path.absolute_path, "c:/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.cd("./d/")
        this.assertEqual(path.absolute_path, "c:/root/a/b/c/d")
        this.assertEqual(path.relative_path, "a/b/c/d")

    def test_change_dirs_absolute(this):
        path = NTAbstractPath("c:/root/a", "c:/root")

        path.cd("c:/root/a/b")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")


        path.cd("c:/root/a")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        path = NTAbstractPath("c:/root/a/", "c:/root")

        path.cd("c:/root/a/b/")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("c:/root/a/")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        #abs path starting with /

        path = NTAbstractPath("c:/root/a", "c:/root")

        path.cd("/a/b")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("/a")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")

        path = NTAbstractPath("c:/root/a/", "c:/root")

        path.cd("/a/b/")
        this.assertEqual(path.absolute_path, "c:/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("/a/")
        this.assertEqual(path.absolute_path, "c:/root/a")
        this.assertEqual(path.relative_path, "a")



    def test_change_above_root(this):
        path = NTAbstractPath("c:/some/path/root")
        path.cd("a")
        path.cd("b/")
        path.cd("c/")

        this.assertEqual(path.absolute_path, "c:/some/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.cd("../../../")
        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path, "c:/some/path/root")

        path = NTAbstractPath("a/b/c", "c:/some/path/root")
        this.assertEqual(path.absolute_path, "c:/some/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")
        path.cd("../../../../")

        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path, "c:/some/path/root")

        path.cd("c:/windows")
        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path,"c:/some/path/root")

    def test_wrong_init(this):
        with this.assertRaises(MissingAbsolutePathException):
            path = NTAbstractPath(".", "path/root") #basepath is relative

        with this.assertRaises(PathOutsideRootException):
            path = NTAbstractPath("c:/a/x", "c:/a/b") #path and basepath are not in the same root

        path = NTAbstractPath("c:")
        this.assertEqual(path.absolute_path,"c:/")

    def test_class_methods(this):

        this.assertTrue(NTAbstractPath.is_absolute("c:/a/b/c"))
        this.assertTrue(NTAbstractPath.is_absolute("c:/a/b/c/"))
        this.assertTrue(NTAbstractPath.is_absolute("/a/b/c/"))
        this.assertFalse(NTAbstractPath.is_absolute("a/b/c/"))
        this.assertFalse(NTAbstractPath.is_absolute("a/b/c"))

        this.assertEqual(NTAbstractPath.get_volume("c:/a/b/c"), "c:")
        this.assertEqual(NTAbstractPath.get_volume("test:/a/b/c"), "test:")
        this.assertEqual(NTAbstractPath.get_volume("c:/a/b/c/"), "c:")
        this.assertEqual(NTAbstractPath.get_volume("test:/a/b/c/"), "test:")

    def test_root_change(this):
        path = NTAbstractPath("a/b/c", "c:/path/root")
        this.assertEqual(path.absolute_path,"c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.root = "d:/"
        this.assertEqual(path.absolute_path, "d:/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

    def test_double_slashes(this):
        path = NTAbstractPath("a/b//c", "c:/path/root")
        this.assertEqual(path.absolute_path,"c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a/b/c", "c:/path//root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a//b/c", "c:/path//root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a//b///c", "c:///path//root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

    def test_windows_slashes(this):
        path = NTAbstractPath("a\\b\\c", "c:/path/root")
        this.assertEqual(path.absolute_path,"c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a/b/c", "c:\\path\\root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a\\b/c", "c:/path\\root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = NTAbstractPath("a\\\\b\\c", "c:\\path\\\\root")
        this.assertEqual(path.absolute_path, "c:/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")












if __name__ == '__main__':
    unittest.main()