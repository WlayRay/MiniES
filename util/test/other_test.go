package utiltest

import (
	"bytes"
	"fmt"
	"testing"

	"github.com/WlayRay/ElectricSearch/config"
	"github.com/WlayRay/ElectricSearch/util"
)

// TestIntToBytes 测试 IntToBytes 函数
func TestIntToBytes(t *testing.T) {
	tests := []struct {
		input    int
		expected []byte
	}{
		{0, []byte{0, 0, 0, 0, 0, 0, 0, 0}},
		{1, []byte{0, 0, 0, 0, 0, 0, 0, 1}},
		{16, []byte{0, 0, 0, 0, 0, 0, 0, 16}},
		{65535, []byte{0, 0, 0, 0, 0, 0, 255, 255}},
	}

	for _, test := range tests {
		actual := util.IntToBytes(test.input)
		if !bytes.Equal(actual, test.expected) {
			t.Errorf("IntToBytes(%d) = %v; expected %v", test.input, actual, test.expected)
		}
	}
}

// TestBytesToInt 测试 BytesToInt 函数
func TestBytesToInt(t *testing.T) {
	tests := []struct {
		input    []byte
		expected int
	}{
		{[]byte{0, 0, 0, 0, 0, 0, 0, 0}, 0},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 1}, 1},
		{[]byte{0, 0, 0, 0, 0, 0, 0, 16}, 16},
		{[]byte{0, 0, 0, 0, 0, 0, 255, 255}, 65535},
	}

	for _, test := range tests {
		actual := util.BytesToInt(test.input)
		if actual != test.expected {
			t.Errorf("BytesToInt(%v) = %d; expected %d", test.input, actual, test.expected)
		}
	}
}

// TestCombineUint32 测试 CombineUint32 函数
func TestCombineUint32(t *testing.T) {
	tests := []struct {
		a        uint32
		b        uint32
		expected uint64
	}{
		{0, 0, 0},
		{1, 0, 1 << 32},
		{0, 1, 1},
		{65535, 65535, (65535 << 32) | 65535},
	}

	for _, test := range tests {
		actual := util.CombineUint32(test.a, test.b)
		if actual != test.expected {
			t.Errorf("CombineUint32(%d, %d) = %d; expected %d", test.a, test.b, actual, test.expected)
		}
	}
}

// TestDisassembleUint64 测试 DisassembleUint64 函数
func TestDisassembleUint64(t *testing.T) {
	tests := []struct {
		input     uint64
		expectedA uint32
		expectedB uint32
	}{
		{0, 0, 0},
		{1 << 32, 1, 0},
		{1, 0, 1},
		{(65535 << 32) | 65535, 65535, 65535},
	}

	for _, test := range tests {
		a, b := util.DisassembleUint64(test.input)
		if a != test.expectedA || b != test.expectedB {
			t.Errorf("DisassembleUint64(%d) = (%d, %d); expected (%d, %d)", test.input, a, b, test.expectedA, test.expectedB)
		}
	}
}

func TestGetLocalIP(t *testing.T) {
	fmt.Println(util.GetLocalIP())
}

func TestInit(t *testing.T) {
	fmt.Printf("rootpath: %v\n", config.RootPath)
	fmt.Printf("ConfigMap:\n")
	for k, v := range config.ConfigMap {
		fmt.Printf(" %v: %v\n", k, v)
	}
}
