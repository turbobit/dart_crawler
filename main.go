package main

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io"
	"log"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

type CSVData struct {
	Headers []string   `json:"headers"`
	Rows    [][]string `json:"rows"`
	Total   int        `json:"total"`
}

type FileInfo struct {
	Name     string `json:"name"`
	Size     int64  `json:"size"`
	Modified string `json:"modified"`
}

type App struct {
	templates *template.Template
	dataDir   string
}

func NewApp() *App {
	app := &App{
		dataDir: "data",
	}
	
	// 템플릿 함수 정의
	funcMap := template.FuncMap{
		"formatFileSize": formatFileSize,
		"add": func(a, b int) int { return a + b },
		"sub": func(a, b int) int { return a - b },
		"max": func(a, b int) int {
			if a > b {
				return a
			}
			return b
		},
		"min": func(a, b int) int {
			if a < b {
				return a
			}
			return b
		},
	}
	
	// 템플릿 로드
	app.templates = template.Must(template.New("").Funcs(funcMap).ParseGlob("templates/*.html"))
	
	return app
}

func formatFileSize(size int64) string {
	const unit = 1024
	if size < unit {
		return fmt.Sprintf("%d B", size)
	}
	div, exp := int64(unit), 0
	for n := size / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB", float64(size)/float64(div), "KMGTPE"[exp])
}

func (app *App) indexHandler(w http.ResponseWriter, r *http.Request) {
	files, err := app.getCSVFiles()
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := struct {
		Files []FileInfo
	}{
		Files: files,
	}

	if err := app.templates.ExecuteTemplate(w, "index.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (app *App) viewHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]
	
	// 페이지네이션 파라미터
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize < 1 {
		pageSize = 100
	}
	
	// 검색 파라미터
	search := r.URL.Query().Get("search")
	
	csvData, err := app.loadCSVData(filename, page, pageSize, search)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	data := struct {
		Filename string
		Data     CSVData
		Page     int
		PageSize int
		Search   string
		TotalPages int
	}{
		Filename:   filename,
		Data:       csvData,
		Page:       page,
		PageSize:   pageSize,
		Search:     search,
		TotalPages: (csvData.Total + pageSize - 1) / pageSize,
	}

	if err := app.templates.ExecuteTemplate(w, "view.html", data); err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}

func (app *App) apiDataHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]
	
	page, _ := strconv.Atoi(r.URL.Query().Get("page"))
	if page < 1 {
		page = 1
	}
	
	pageSize, _ := strconv.Atoi(r.URL.Query().Get("pageSize"))
	if pageSize < 1 {
		pageSize = 100
	}
	
	search := r.URL.Query().Get("search")
	
	csvData, err := app.loadCSVData(filename, page, pageSize, search)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(csvData)
}

func (app *App) downloadHandler(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	filename := vars["filename"]
	
	// 새로운 파일명 생성
	newFilename := r.URL.Query().Get("newName")
	if newFilename == "" {
		newFilename = strings.TrimSuffix(filename, ".csv") + "_export.csv"
	}
	
	// CSV 파일 읽기
	filePath := filepath.Join(app.dataDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	defer file.Close()

	// 응답 헤더 설정
	w.Header().Set("Content-Type", "text/csv")
	w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", newFilename))

	// 파일 내용 복사
	io.Copy(w, file)
}

func (app *App) getCSVFiles() ([]FileInfo, error) {
	var files []FileInfo
	
	entries, err := os.ReadDir(app.dataDir)
	if err != nil {
		return nil, err
	}
	
	for _, entry := range entries {
		if !entry.IsDir() && strings.HasSuffix(entry.Name(), ".csv") {
			info, err := entry.Info()
			if err != nil {
				continue
			}
			
			files = append(files, FileInfo{
				Name:     entry.Name(),
				Size:     info.Size(),
				Modified: info.ModTime().Format("2006-01-02 15:04:05"),
			})
		}
	}
	
	return files, nil
}

func (app *App) loadCSVData(filename string, page, pageSize int, search string) (CSVData, error) {
	filePath := filepath.Join(app.dataDir, filename)
	file, err := os.Open(filePath)
	if err != nil {
		return CSVData{}, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	reader.FieldsPerRecord = -1 // 필드 수가 일정하지 않을 수 있음
	
	// 모든 데이터 읽기
	allRecords, err := reader.ReadAll()
	if err != nil {
		return CSVData{}, err
	}
	
	if len(allRecords) == 0 {
		return CSVData{}, nil
	}
	
	headers := allRecords[0]
	rows := allRecords[1:]
	
	// 검색 필터링
	if search != "" {
		var filteredRows [][]string
		searchLower := strings.ToLower(search)
		
		for _, row := range rows {
			for _, cell := range row {
				if strings.Contains(strings.ToLower(cell), searchLower) {
					filteredRows = append(filteredRows, row)
					break
				}
			}
		}
		rows = filteredRows
	}
	
	total := len(rows)
	
	// 페이지네이션
	start := (page - 1) * pageSize
	end := start + pageSize
	
	if start >= total {
		rows = [][]string{}
	} else {
		if end > total {
			end = total
		}
		rows = rows[start:end]
	}
	
	return CSVData{
		Headers: headers,
		Rows:    rows,
		Total:   total,
	}, nil
}

func openBrowser(url string) {
	var err error

	switch runtime.GOOS {
	case "linux":
		err = exec.Command("xdg-open", url).Start()
	case "windows":
		err = exec.Command("rundll32", "url.dll,FileProtocolHandler", url).Start()
	case "darwin":
		err = exec.Command("open", url).Start()
	default:
		err = fmt.Errorf("unsupported platform")
	}
	if err != nil {
		log.Printf("브라우저를 열 수 없습니다: %v", err)
	}
}

func main() {
	app := NewApp()
	
	r := mux.NewRouter()
	
	// 정적 파일 서빙
	r.PathPrefix("/static/").Handler(http.StripPrefix("/static/", http.FileServer(http.Dir("static/"))))
	
	// 라우트 설정
	r.HandleFunc("/", app.indexHandler).Methods("GET")
	r.HandleFunc("/view/{filename}", app.viewHandler).Methods("GET")
	r.HandleFunc("/api/data/{filename}", app.apiDataHandler).Methods("GET")
	r.HandleFunc("/download/{filename}", app.downloadHandler).Methods("GET")
	
	fmt.Println("CSV 뷰어 서버가 시작되었습니다.")
	fmt.Println("http://localhost:8080 에서 접속하세요.")
	
	// 2초 후 브라우저 자동 열기
	go func() {
		time.Sleep(2 * time.Second)
		openBrowser("http://localhost:8080")
	}()
	
	log.Fatal(http.ListenAndServe(":8080", r))
}
