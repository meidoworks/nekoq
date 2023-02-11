package discovery

import "testing"

func TestValidateName(t *testing.T) {
	if !validateName("/hello/world/a.txt") {
		t.Fatal("validation fail")
	}
	if !validateName("com.example.service.Service01") {
		t.Fatal("validation fail")
	}
	if !validateName("service01-area01.example.com") {
		t.Fatal("validation fail")
	}
	if !validateName(`\\127.0.0.1\demo.txt`) {
		t.Fatal("validation fail")
	}
	if validateName("$") {
		t.Fatal("validation fail")
	}
	if validateName("你好") {
		t.Fatal("validation fail")
	}
}

func BenchmarkValidateName(b *testing.B) {
	for i := 0; i < b.N; i++ {
		validateName("com.example.service.Service01")
	}
}
