package test

import (
	"com.github/robin0909/fos/src/resource"
	"testing"
)

func TestObjExists(t *testing.T) {
	dataDir := "/tmp/data3"
	bucket := "media"
	obj := "demo.pdf"

	// resource.IsExistResourceObj(dataDir, bucket, obj)
	if resource.IsExistResourceObj(dataDir, bucket, obj) {
		t.Log("obj exists")
	} else {
		t.Error("obj not exists")
	}

}

func TestObjNotExists(t *testing.T) {
	dataDir := "/tmp/data3"
	bucket := "media"
	obj := "demo1.pdf"

	// resource.IsExistResourceObj(dataDir, bucket, obj)
	if resource.IsExistResourceObj(dataDir, bucket, obj) {
		t.Error("obj exists")
	} else {
		t.Log("obj not exists")
	}

}
