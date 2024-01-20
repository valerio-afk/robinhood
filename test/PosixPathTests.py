from filesystem import PosixAbstractPath, MissingAbsolutePathException, PathOutsideRootException
import unittest

class TestPosixPathManager(unittest.TestCase):
    def test_basic(this):
        path = PosixAbstractPath("/")
        this.assertEqual(path.absolute_path,"/")
        this.assertEqual(path.relative_path,".")

        path = PosixAbstractPath("/a/b", "/")
        this.assertEqual(path.absolute_path, "/a/b")
        this.assertEqual(path.relative_path, "a/b")

    def test_basic_trailing_slash(this):
        path = PosixAbstractPath("/a/b/", "/")
        this.assertEqual(path.absolute_path, "/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path = PosixAbstractPath("/a/b/", "/a")
        this.assertEqual(path.absolute_path, "/a/b")
        this.assertEqual(path.relative_path, "b")

        path = PosixAbstractPath("/a/b/", "/a/")
        this.assertEqual(path.absolute_path, "/a/b")
        this.assertEqual(path.relative_path, "b")

    def test_parent_dirs(this):
        path = PosixAbstractPath("/a/../b", "/")
        this.assertEqual(path.absolute_path, "/b")
        this.assertEqual(path.relative_path, "b")

        path = PosixAbstractPath("/a/../b/../", "/")
        this.assertEqual(path.absolute_path, "/")
        this.assertEqual(path.relative_path, ".")

        path = PosixAbstractPath("/a/../b", "/..")
        this.assertEqual(path.absolute_path, "/b")
        this.assertEqual(path.relative_path, "b")

        path = PosixAbstractPath("/a/../b/../", "/..")
        this.assertEqual(path.absolute_path, "/")
        this.assertEqual(path.relative_path, ".")

        path = PosixAbstractPath("/a/../b", "/a/..")
        this.assertEqual(path.absolute_path, "/b")
        this.assertEqual(path.relative_path, "b")

        path = PosixAbstractPath("/a/../b/../", "/a/..")
        this.assertEqual(path.absolute_path, "/")
        this.assertEqual(path.relative_path, ".")

    def test_change_dirs_relative(this):
        path = PosixAbstractPath("/root/a/", "/root")
        this.assertEqual(path.absolute_path, "/root/a")
        this.assertEqual(path.relative_path, "a")

        path.cd("b")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd(".")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd("..")
        this.assertEqual(path.absolute_path, "/root/a")
        this.assertEqual(path.relative_path, "a")

        path = PosixAbstractPath("/root/a/", "/root")
        path.cd("b/")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd(".")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")
        path.cd("..")
        this.assertEqual(path.absolute_path, "/root/a")
        this.assertEqual(path.relative_path, "a")

        # rel path starting with ./

        path = PosixAbstractPath("/root/a", "/root")

        path.cd("./b/c")
        this.assertEqual(path.absolute_path, "/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = PosixAbstractPath("/root/a", "/root")
        path.cd("./b")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path = PosixAbstractPath("/root/a/", "/root")

        path.cd("./b/c/")
        this.assertEqual(path.absolute_path, "/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = PosixAbstractPath("/root/a", "/root")

        path.cd("./b/")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

    def test_change_dirs_absolute(this):
        path = PosixAbstractPath("/root/a", "/root")

        path.cd("/root/a/b")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")


        path.cd("/root/a")
        this.assertEqual(path.absolute_path, "/root/a")
        this.assertEqual(path.relative_path, "a")

        path = PosixAbstractPath("/root/a/", "/root")

        path.cd("/root/a/b/")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("/root/a/")
        this.assertEqual(path.absolute_path, "/root/a")
        this.assertEqual(path.relative_path, "a")

        #abs path starting with /

        path = PosixAbstractPath("/root/a", "/root")

        path.cd("/root/a/b")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("/a")
        this.assertEqual(path.absolute_path, "/root")
        this.assertEqual(path.relative_path, ".")

        path = PosixAbstractPath("/root/a/", "/root")

        path.cd("/root/a/b/")
        this.assertEqual(path.absolute_path, "/root/a/b")
        this.assertEqual(path.relative_path, "a/b")

        path.cd("/a/")
        this.assertEqual(path.absolute_path, "/root")
        this.assertEqual(path.relative_path, ".")



    def test_change_above_root(this):
        path = PosixAbstractPath("/some/path/root")
        path.cd("a")
        path.cd("b/")
        path.cd("c/")

        this.assertEqual(path.absolute_path, "/some/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.cd("../../../")
        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path, "/some/path/root")

        path = PosixAbstractPath("a/b/c", "/some/path/root")
        this.assertEqual(path.absolute_path, "/some/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")
        path.cd("../../../../")

        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path, "/some/path/root")

        path.cd("/windows")
        this.assertEqual(path.relative_path, ".")
        this.assertEqual(path.absolute_path,"/some/path/root")

    def test_wrong_init(this):
        with this.assertRaises(MissingAbsolutePathException):
            path = PosixAbstractPath(".", "path/root") #basepath is relative

        with this.assertRaises(PathOutsideRootException):
            path = PosixAbstractPath("/a/x", "/a/b") #path and basepath are not in the same root

    def test_class_methods(this):
        this.assertTrue(PosixAbstractPath.is_absolute("/a/b/c"))
        this.assertTrue(PosixAbstractPath.is_absolute("/a/b/c/"))
        this.assertTrue(PosixAbstractPath.is_absolute("/a/b/c/"))
        this.assertFalse(PosixAbstractPath.is_absolute("a/b/c/"))
        this.assertFalse(PosixAbstractPath.is_absolute("a/b/c"))

    def test_root_change(this):
        path = PosixAbstractPath("a/b/c", "/path/root")
        this.assertEqual(path.absolute_path,"/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path.root = "/new/dir/root"
        this.assertEqual(path.absolute_path, "/new/dir/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")


    def test_double_slashes(this):
        path = PosixAbstractPath("a/b//c", "/path/root")
        this.assertEqual(path.absolute_path,"/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = PosixAbstractPath("a/b/c", "/path//root")
        this.assertEqual(path.absolute_path, "/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = PosixAbstractPath("a//b/c", "/path//root")
        this.assertEqual(path.absolute_path, "/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")

        path = PosixAbstractPath("a//b///c", "///path//root")
        this.assertEqual(path.absolute_path, "/path/root/a/b/c")
        this.assertEqual(path.relative_path, "a/b/c")


if __name__ == '__main__':
    unittest.main()