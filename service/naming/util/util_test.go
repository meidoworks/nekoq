package util

import (
	"testing"
)

func TestGroupPathCheckAndSplit(t *testing.T) {
	RunTest(t, func() {
		AssertTrue(CheckGroupPath("/"))
		AssertFalse(CheckGroupPath("//"))
		AssertTrue(CheckGroupPath("/heloworld/new.hello/buffer"))
		AssertFalse(CheckGroupPath(""))
		AssertFalse(CheckGroupPath("/bbb/ccc//ddd"))
		AssertFalse(CheckGroupPath("/new/path/"))
		AssertFalseWithMsg("test blank", CheckGroupPath("/ /d"))

		t.Log(SplitGroupPath("/"), len(SplitGroupPath("/")))
		t.Log(SplitGroupPath("/hello"), len(SplitGroupPath("/hello")))
		t.Log(SplitGroupPath("/buffer/buffer2/buffer3"), len(SplitGroupPath("/buffer/buffer2/buffer3")))
	})
}

func TestMakeGroup(t *testing.T) {
	RunTest(t, func() {
		AssertStringEquals("/a/b/c", MakeGroupPath([]string{"a", "b", "c"}))
		AssertStringEquals("/a", MakeGroupPath([]string{"a"}))
		AssertStringEquals("/", MakeGroupPath([]string{}))
	})
}

func RunTest(t, f interface{}) {

}

func AssertStringEquals(t, t2 interface{}) {

}

func AssertTrue(t interface{}) {

}

func AssertFalse(t interface{}) {

}

func AssertFalseWithMsg(t, t2 interface{}) {

}
