package util

import (
	"testing"
)

import (
	"moetang.info/go/test"
)

func TestGroupPathCheckAndSplit(t *testing.T) {
	test.RunTest(t, func() {
		test.AssertTrue(CheckGroupPath("/"))
		test.AssertFalse(CheckGroupPath("//"))
		test.AssertTrue(CheckGroupPath("/heloworld/new.hello/buffer"))
		test.AssertFalse(CheckGroupPath(""))
		test.AssertFalse(CheckGroupPath("/bbb/ccc//ddd"))
		test.AssertFalse(CheckGroupPath("/new/path/"))
		test.AssertFalseWithMsg("test blank", CheckGroupPath("/ /d"))

		t.Log(SplitGroupPath("/"), len(SplitGroupPath("/")))
		t.Log(SplitGroupPath("/hello"), len(SplitGroupPath("/hello")))
		t.Log(SplitGroupPath("/buffer/buffer2/buffer3"), len(SplitGroupPath("/buffer/buffer2/buffer3")))
	})
}

func TestMakeGroup(t *testing.T) {
	test.RunTest(t, func() {
		test.AssertStringEquals("/a/b/c", MakeGroupPath([]string{"a", "b", "c"}))
		test.AssertStringEquals("/a", MakeGroupPath([]string{"a"}))
		test.AssertStringEquals("/", MakeGroupPath([]string{}))
	})
}
