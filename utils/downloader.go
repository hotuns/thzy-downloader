package utils

import (
	"database/sql"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
	"github.com/bytedance/sonic"
	"github.com/xuri/excelize/v2"

	_ "github.com/go-sql-driver/mysql"
)

type Downloader struct {
	Auth        *oss.Client
	Bucket      *oss.Bucket
	DB          *sql.DB
	RootPath    string
	Time        string
	DirPath     string
	ZipfilePath string
}

func getEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

func NewDownloader() *Downloader {
	ossEndpoint := getEnv("OSS_ENDPOINT", "https://oss-cn-beijing.aliyuncs.com")
	ossAccessKeyId := getEnv("OSS_ACCESS_KEY_ID", "")
	ossSecretAccessKey := getEnv("OSS_SECRET_ACCESS_KEY", "")
	dbConnectionStr := getEnv("DB_CONNECTION_STR", "root:@tcp(localhost:3306)/database")

	client, err := oss.New(ossEndpoint, ossAccessKeyId, ossSecretAccessKey)
	if err != nil {
		panic(err)
	}
	bucket, err := client.Bucket("iot-datas")
	if err != nil {
		panic(err)
	}
	db, err := sql.Open("mysql", dbConnectionStr)
	if err != nil {
		panic(err)
	}
	err = db.Ping()
	if err != nil {
		panic(err.Error()) // proper error handling instead of panic in your app
	}

	rootPath := "./output/"
	timeStr := time.Now().Format("2006-01-02-15-04-05")
	dirPath := filepath.Join(rootPath, timeStr)
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		panic(err)
	}
	return &Downloader{
		Auth:        client,
		Bucket:      bucket,
		DB:          db,
		RootPath:    rootPath,
		Time:        timeStr,
		DirPath:     dirPath,
		ZipfilePath: dirPath + ".zip",
	}
}

// Close 关闭下载器
func (d *Downloader) Close() {
	d.DB.Close()
}

// 获取path
func (d *Downloader) GetDevicePath(deviceID string) string {
	target := filepath.Join(d.DirPath, deviceID)
	// 判断文件夹是否存在
	if _, err := os.Stat(target); os.IsNotExist(err) {
		os.Mkdir(target, 0755)
	}
	return target
}

// 获取设备配置
func (d *Downloader) GetDeviceConfig(deviceID string) (*DeviceConfig, error) {
	var config DeviceConfig
	// 获取设备配置
	query := "SELECT id, device_id, data, image, control, version FROM device_config  WHERE device_id = ? ORDER BY created_at DESC LIMIT 1"
	err := d.DB.QueryRow(query, deviceID).Scan(&config.ID, &config.DeviceID, &config.Data, &config.Image, &config.Control, &config.Version)
	if err != nil {
		return nil, err
	}
	return &config, nil
}

// 查询数据库
// 根据设备id，开始时间，结束时间获取数据
func (d *Downloader) QueryForDevice(deviceID string, startTime string, endTime string, downloadType string) ([]map[string]string, []map[string]string, error) {
	// 格式化时间
	sTime, err := time.Parse("2006-01-02", startTime)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}
	eTime, err := time.Parse("2006-01-02", endTime)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}

	// sTime的1号 00:00:00
	tableStart := sTime.Format("2006-01-02")[:8] + "01 00:00:00"
	// eTime当月最后一天 23:59:59
	tableEnd := eTime.AddDate(0, 1, -eTime.Day()).Format("2006-01-02") + " 23:59:59"

	// 获取设备数据表
	query := "SELECT tb_name FROM device_data_index WHERE  start_at >= ? AND end_at <= ?"
	rows, err := d.DB.Query(query, tableStart, tableEnd)
	if err != nil {
		fmt.Println(err)
		return nil, nil, err
	}
	defer rows.Close()

	var tableNameList []string
	for rows.Next() {
		var tableName string
		err := rows.Scan(&tableName)
		if err != nil {
			fmt.Println(err)
			return nil, nil, err
		}
		tableNameList = append(tableNameList, tableName)
	}

	var allData []map[string]string
	var allImage []map[string]string
	// 查询数据
	for _, tableName := range tableNameList {
		query = "SELECT ts, data, type FROM " + tableName + " WHERE device_id = ? AND created_at >= ? AND created_at <= ?"
		if downloadType == "data" {
			query += " AND type = 'data'"
		}
		if downloadType == "image" {
			query += " AND type = 'image'"
		}

		rows, err := d.DB.Query(query, deviceID, startTime, endTime)
		if err != nil {
			fmt.Println(err)
			return nil, nil, err
		}
		defer rows.Close()
		for rows.Next() {
			var ts string
			var data string
			var dataType string
			err := rows.Scan(&ts, &data, &dataType)
			if err != nil {
				fmt.Println(err)
				return nil, nil, err
			}
			if dataType == "data" {
				allData = append(allData, map[string]string{"ts": ts, "data": data})
			}
			if dataType == "image" {
				allImage = append(allImage, map[string]string{"ts": ts, "data": data})
			}

		}
	}

	return allData, allImage, nil
}

// 开始任务
func (d *Downloader) StartJob(deviceID string, startTime string, endTime string, downloadType string) {
	fmt.Println("Start job for device:", deviceID)

	// 获取设备配置
	config, err := d.GetDeviceConfig(deviceID)
	if err != nil {
		fmt.Println(err)
		return
	}
	// 查询数据库
	allData, allImage, err := d.QueryForDevice(deviceID, startTime, endTime, downloadType)
	if err != nil {
		fmt.Println(err)
		return
	}

	if downloadType == "data" || downloadType == "all" {
		d.parseDatas(allData, config, deviceID)
	}

	if downloadType == "image" || downloadType == "all" {
		d.parseImages(allImage, config, deviceID)
	}
}

// 解析数据，并且下载成excel文件
func (d *Downloader) parseDatas(datas []map[string]string, config *DeviceConfig, deviceID string) {
	// 解析config
	var configArray []DataConfig
	if err := sonic.Unmarshal([]byte(config.Data), &configArray); err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}

	header_key := []string{"time"}
	header_name := []string{"时间"}
	for _, item := range configArray {
		for _, c := range item.Params.Contents {
			header_key = append(header_key, c.Key)
			header_name = append(header_name, c.Info.Name+c.Info.Unit)
		}
	}

	f := excelize.NewFile()
	defer func() {
		if err := f.Close(); err != nil {
			fmt.Println(err)
		}
	}()
	sw, err := f.NewStreamWriter("Sheet1")
	if err != nil {
		fmt.Println(err)
		return
	}

	// 写入表头
	row := []interface{}{}
	for _, name := range header_name {
		row = append(row, name)
	}
	if err := sw.SetRow("A1", row); err != nil {
		fmt.Println("获取流式写入器失败: ", err)
		return
	}

	for index, data := range datas {
		ts, data := data["ts"], data["data"]

		// 解析data
		var dat map[string]interface{}
		if err := sonic.Unmarshal([]byte(data), &dat); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return
		}
		// 根据header_key的顺序，写入数据
		var row []interface{}
		// 时间
		row = append(row, ts)

		for _, key := range header_key[1:] {
			// 如果key不存在，写入空字符串
			if dat[key] == nil {
				row = append(row, "")
			} else {
				v := dat[key].(map[string]interface{})
				row = append(row, v["value"])
			}
		}

		// 按行写入
		if err := sw.SetRow("A"+strconv.Itoa(index+2), row); err != nil {
			fmt.Println("写入数据失败: ", err)
			return
		}

	}

	// 调用 Flush 函数来结束流式写入过程
	if err = sw.Flush(); err != nil {
		fmt.Println("Flush 失败: ", err)
		return
	}

	// 保存文件
	filename := d.GetDevicePath(deviceID) + "/" + deviceID + ".xlsx"
	if err := f.SaveAs(filename); err != nil {
		fmt.Println(err)
		return
	}

	fmt.Println("save the file:", filename)

	// 调用系统打开文件夹
	exec.Command("open", d.GetDevicePath(deviceID)).Run()
}

func (d *Downloader) parseImages(images []map[string]string, config *DeviceConfig, deviceID string) {
	// 解析config
	var configArray []map[string]interface{}
	if err := sonic.Unmarshal([]byte(config.Image), &configArray); err != nil {
		fmt.Println("Error parsing JSON:", err)
		return
	}

	for _, image := range images {
		_, data := image["ts"], image["data"]

		// 解析image
		var img map[string]interface{}
		if err := sonic.Unmarshal([]byte(data), &img); err != nil {
			fmt.Println("Error parsing JSON:", err)
			return
		}

		for _, item := range configArray {

			var imgkeys []string
			for key := range img {
				imgkeys = append(imgkeys, key)
			}
			theImgData := img[imgkeys[0]].(map[string]interface{})

			// oss的地址
			ossPath := theImgData["value"].(string)
			if ossPath[0] == '/' {
				ossPath = ossPath[1:]
			}
			ossFileName := filepath.Base(ossPath)
			parts := strings.Split(ossFileName, "_")
			if len(parts) < 2 {
				fmt.Println("文件名格式不正确")
				return
			}
			timestampStr := parts[1] // 提取时间戳部分
			// 将字符串时间戳转换为int64
			timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
			if err != nil {
				fmt.Println("时间戳转换错误:", err)
				return
			}
			t := time.Unix(timestamp, 0)
			ossFileName = t.Format("2006-01-02-15-04-05") + "_" + parts[2]

			// 本地保存的地址
			var local string
			if item["name"] != nil {
				local = d.GetDevicePath(deviceID) + "/" + item["name"].(string)
			} else {
				local = d.GetDevicePath(deviceID) + "/" + item["key"].(string)
			}
			if _, err := os.Stat(local); os.IsNotExist(err) {
				os.Mkdir(local, 0755)
			}
			localFileName := filepath.Join(local, ossFileName)

			// 下载图片
			osserr := d.Bucket.GetObjectToFile(ossPath, localFileName)
			if osserr != nil {
				fmt.Println(osserr)
				return
			}

			fmt.Println("save image:", localFileName)
		}

	}

	// 调用系统打开文件夹
	exec.Command("open", d.GetDevicePath(deviceID)).Run()
}
