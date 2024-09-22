package main

import (
	"errors"
	"net"

	// "log"
	"bytes"
	// "bufio"
	"fmt"
	"strconv"
)

// Reference: https://redis-doc-test.readthedocs.io/en/latest/topics/protocol/#:~:text=RESP%20protocol%20description,-The%20RESP%20protocol&text=This%20is%20the%20protocol%20you,Integers%2C%20Bulk%20Strings%20and%20Arrays.

const (
	CLRF = "\r\n"
)

type Array []interface{}
type BulkString struct {
	Value  string
	IsNull bool
}
type SimpleString string
type Integer int

type Replica struct {
	conn      net.Conn
	offset    int
	ackOffset int
}

func Parse(data []byte) (interface{}, []byte, error) {
	dataType := data[0]
	var (
		parsed interface{}
		err    error
	)
	switch dataType {
	case '*':
		parsed, data, err = ParseArray(data)
	case '$':
		// BulkString
		parsed, data, err = ParseBulkString(data)
	case '+':
		parsed, data, err = ParseString(data)
	case ':':
		parsed, data, err = ParseInteger(data)
	default:
		return nil, data, fmt.Errorf("unknown data type %b", dataType)
	}

	if err != nil {
		return nil, data, fmt.Errorf("error while parsing data %s", err)
	}
	return parsed, data, err
}

func ParseInteger(data []byte) (Integer, []byte, error) {
	if len(data) < 4 {
		return 0, data, errors.New("Integer needs at least 4 characters")
	}
	data = data[1:]
	hasSign := data[1] == '+' || data[1] == '-'
	isNeg := data[1] == '-'
	if hasSign {
		data = data[1:]
	}
	integerData := bytes.SplitN(data, []byte(CLRF), 2)
	if len(integerData) != 2 {
		return 0, data, errors.New("integer needs end delimiter")
	}
	data = integerData[1]
	integer, err := strconv.Atoi(string(integerData[0]))
	if err != nil {
		return 0, data, fmt.Errorf("value is not an integer: %s", integerData[0])
	}
	if isNeg {
		integer = -integer
	}
	return Integer(integer), data, err
}

func ParseString(data []byte) (string, []byte, error) {
	if len(data) < 4 {
		return "", data, errors.New("simple string needs atleast 4 characters")
	}
	data = data[1:]
	strData := bytes.SplitN(data, []byte(CLRF), 2)
	if len(strData) != 2 {
		return "", data, errors.New("simple string needs end delimiter")
	}
	data = strData[1]
	return string(strData[0]), data, nil
}

func ParseBulkString(data []byte) (BulkString, []byte, error) {
	bulkString := BulkString{
		Value:  "",
		IsNull: false,
	}
	if len(data) < 6 {
		return bulkString, data, errors.New("string needs at least 6 chracters")
	}
	data = data[1:]
	stringData := bytes.SplitN(data, []byte(CLRF), 2)
	if len(stringData) != 2 {
		return bulkString, data, errors.New("string needs a delimiter")
	}
	data = stringData[1]
	length, err := strconv.Atoi(string(stringData[0]))
	if err != nil {
		return bulkString, data, fmt.Errorf("length is not an integer: %s", stringData[0])
	}
	if length == -1 {
		bulkString.IsNull = true
		return bulkString, data, nil
	}
	stringData = bytes.SplitN(data, []byte(CLRF), 2)
	if len(stringData) != 2 {
		return bulkString, data, errors.New("string needs a delimiter")
	}
	data = stringData[1]
	bulkString.Value = string(stringData[0])
	return bulkString, data, nil
}

func ParseArray(data []byte) (Array, []byte, error) {
	if len(data) < 4 {
		return nil, data, errors.New("array needs at least 4 chracter")
	}
	data = data[1:]
	arraydata := bytes.SplitN(data, []byte(CLRF), 2)
	if len(arraydata) != 2 {
		return nil, data, errors.New("array needs start delimiter")
	}
	data = arraydata[1]
	length, err := strconv.Atoi(string(arraydata[0]))
	if err != nil {
		return nil, data, fmt.Errorf("length is not an integer: %s", arraydata[0])
	}
	array := Array{}
	for i := 0; i < length; i++ {
		arrayItem, dataLeft, err := Parse(data)
		if err != nil {
			return nil, data, fmt.Errorf("failed to parse array item: %s, due to %w", data, err)
		}
		array = append(array, arrayItem)
		data = dataLeft
	}
	return array, data, nil
}
