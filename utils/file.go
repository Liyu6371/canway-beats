package utils

import (
	"fmt"
	"os"
)

// 检查文件路径是否可写
func CheckPathWritable(path string) error {
	fmt.Println(path)
	// 当文件存在的时候 默认为可写
	if FileExists(path) {
		return nil
	}
	// 尝试通过创建文件进行判断是否可写
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	file.Close()
	return os.Remove(path)
}

func FileExists(filename string) bool {
	info, err := os.Stat(filename)
	if os.IsNotExist(err) {
		return false
	}
	return !info.IsDir()
}
