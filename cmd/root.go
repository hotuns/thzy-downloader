package cmd

import (
	"fmt"
	"os"
	"strings"
	"sync"

	"thzy/downloader/utils"

	"github.com/spf13/cobra"
	"github.com/spf13/viper"
)

var cfgFile string

func init() {
	cobra.OnInitialize(initConfig)

	rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "配置文件地址 (默认为 ./config.yaml)")

	rootCmd.Flags().StringP("deviceIds", "d", "", "设备id (用逗号分隔)")
	rootCmd.Flags().StringP("startTime", "s", "", "开始时间 (格式: YYYY-MM-DD)")
	rootCmd.Flags().StringP("endTime", "e", "", "结束时间 (格式: YYYY-MM-DD)")
	rootCmd.Flags().StringP("downloadType", "t", "data", "下载类型 (data, image, or all)")

	viper.BindPFlag("deviceIds", rootCmd.Flags().Lookup("deviceIds"))
	viper.BindPFlag("startTime", rootCmd.Flags().Lookup("startTime"))
	viper.BindPFlag("endTime", rootCmd.Flags().Lookup("endTime"))
	viper.BindPFlag("downloadType", rootCmd.Flags().Lookup("downloadType"))
}

func initConfig() {
	if cfgFile != "" {
		viper.SetConfigFile(cfgFile)
	} else {
		viper.AddConfigPath(".")
		viper.SetConfigName("config")
	}

	viper.AutomaticEnv()

	if err := viper.ReadInConfig(); err == nil {
		fmt.Println("Using config file:", viper.ConfigFileUsed())
	}
}

var rootCmd = &cobra.Command{
	Use:   "downloader",
	Short: "下载设备数据",
	Long:  `这个软件用于下载thzy监测站的数据或者图片`,
	Run: func(cmd *cobra.Command, args []string) {
		deviceIds := viper.GetString("deviceIds")
		startTime := viper.GetString("startTime")
		endTime := viper.GetString("endTime")
		downloadType := viper.GetString("downloadType")

		// 检查必要的参数是否已提供
		if deviceIds == "" || startTime == "" || endTime == "" || downloadType == "" {
			fmt.Println("缺少必要的参数。请使用 --help 查看所有可用的参数。")
			os.Exit(1)
		}

		fmt.Println("\n选择的设备 IDs:", deviceIds)
		fmt.Println("时间范围(2021-01-01):", startTime, "到", endTime)
		fmt.Println("下载类型:", downloadType)

		// Call your download function here (not implemented in this example)

		dl := utils.NewDownloader()
		defer dl.Close()

		// 分割设备id
		IDlist := strings.Split(deviceIds, ",")

		var wg sync.WaitGroup

		// 开始任务, 使用goroutine
		for _, deviceID := range IDlist {
			wg.Add(1) // 为每个 goroutine 增加等待计数
			go func(id string) {
				defer wg.Done() // goroutine 完成后减少等待计数
				dl.StartJob(id, startTime, endTime, downloadType)
			}(deviceID)
		}

		wg.Wait() // 等待所有 goroutine 完成
	},
}

func Execute() {
	if err := rootCmd.Execute(); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
