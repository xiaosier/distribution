package scs

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"strings"
	"time"
	scs "github.com/SinaCloudStorage/SinaCloudStorage-SDK-Go"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/json-iterator/go"
	"github.com/sirupsen/logrus"
	"os"
	"path"
	"encoding/base64"
)

const (
	driverName   = "scs"
	maxChunkSize = 4 * (1 << 20)
	listMax      = 1000
)

//DriverParameters A struct that encapsulates all of the driver parameters after all values have been set
type DriverParameters struct {
	AccessKey     string
	SecretKey     string
	Bucket        string
	Endpoint      string
	RootDirectory string
}

func init() {
	factory.Register(driverName, &scsDriverFactory{})
}

// scsDriverFactory implements the factory.StorageDriverFactory interface
type scsDriverFactory struct{}

func (factory *scsDriverFactory) Create(parameters map[string]interface{}) (storagedriver.StorageDriver, error) {
	return FromParameters(parameters)
}

type driver struct {
	Client        *scs.SCS
	Bucket        *scs.Bucket
	writerPath    map[string]storagedriver.FileWriter
	rootDirectory string
	bucketName    string
	logger        *logrus.Logger
}

type baseEmbed struct {
	base.Base
}

// Driver is a storagedriver.StorageDriver implementation backed by Aliyun OSS
// Objects are stored at absolute keys in the provided bucket.
type Driver struct {
	baseEmbed
}

func FromParameters(parameters map[string]interface{}) (*Driver, error) {
	// Providing no values for these is valid in case the user is authenticating

	accessKey, ok := parameters["accesskey"]
	if !ok {
		return nil, fmt.Errorf("no accesskey parameter provided")
	}
	secretKey, ok := parameters["secretkey"]
	if !ok {
		return nil, fmt.Errorf("no secretkey parameter provided")
	}

	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("no bucket parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = ""
	}

	RootDirectory := ""

	if rootDirectory, ok := parameters["rootdirectory"]; ok {
		if _, ok := rootDirectory.(string); ok {
			RootDirectory = fmt.Sprint(rootDirectory)
		} else {
			RootDirectory = ""
		}
	} else {
		RootDirectory = ""
	}

	params := DriverParameters{
		AccessKey:     fmt.Sprint(accessKey),
		SecretKey:     fmt.Sprint(secretKey),
		Bucket:        fmt.Sprint(bucket),
		Endpoint:      fmt.Sprint(endpoint),
		RootDirectory: RootDirectory,
	}

	return New(params)
}

func New(params DriverParameters) (*Driver, error) {

	logger := logrus.New()
	loggerFile, err := os.Create("scs_debug.log")
	if err != nil {
		logrus.Fatalln("open logger failed", err)
	}
	logger.Out = loggerFile
	logger.Level = logrus.DebugLevel

	client := scs.NewSCS(params.AccessKey, params.SecretKey, params.Endpoint)
	bucket := client.Bucket(params.Bucket)

	logger.Debugln(params.Bucket)

	// Validate that the given credentials have at least read permissions in the
	// given bucket scope.
	if _, err := bucket.ListObject("", "", "", 1); err != nil {
		return nil, err
	}

	d := &driver{
		Client:     client,
		Bucket:     bucket,
		writerPath: make(map[string]storagedriver.FileWriter),
		bucketName: params.Bucket,
		logger:     logger,
	}

	return &Driver{
		baseEmbed: baseEmbed{
			Base: base.Base{
				StorageDriver: d,
			},
		},
	}, nil
}

// Implement the storagedriver.StorageDriver interface

func (d *driver) Name() string {
	return driverName
}

func parseError(path string, err error) error {
	if scsErr, ok := err.(*scs.Error); ok && scsErr.StatusCode == http.StatusNotFound && (scsErr.ErrorCode == "NoSuchKey" || scsErr.ErrorCode == "") {
		return storagedriver.PathNotFoundError{Path: path}
	}

	return err
}

// GetContent retrieves the content stored at "path" as a []byte.
func (d *driver) GetContent(ctx context.Context, path string) ([]byte, error) {
	content, err := d.Bucket.Get(path)
	if err != nil {
		return nil, parseError(path, err)
	}
	return content, nil
}

func getPermissions() scs.ACL {
	return scs.Private
}

func (d *driver) getContentType() string {
	return "application/octet-stream"
}

// PutContent stores the []byte content at a location designated by "path".
func (d *driver) PutContent(ctx context.Context, path string, contents []byte) error {
	d.logger.WithFields(logrus.Fields{
		"path": path,
		"size": len(contents),
	}).Debugln("PutContent")
	return parseError(path, d.Bucket.PutContent(d.scsPath(path), contents, getPermissions()))
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {
	d.logger.WithFields(logrus.Fields{
		"path":   path,
		"offset": offset,
	}).Debugln("Reader")
	path = d.scsPath(path)
	resp, err := d.Bucket.GetRange(path, offset)
	if err != nil {
		return nil, parseError(path, err)
	}

	return ioutil.NopCloser(bytes.NewReader([]byte(resp))), nil
}

func (d *driver) fullPath(subPath string) string {
	/*Use temp path to store files*/
	return path.Join(os.TempDir(), subPath)
}

func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	d.logger.WithFields(logrus.Fields{
		"path":   path,
		"append": append,
	}).Debugln("Writer")
	var w *writer
	scsPath := d.scsPath(path)
	if !append {
		multi, err := d.Bucket.InitMulti(scsPath)
		if err != nil {
			return nil, err
		}
		w = &writer{
			key:           scsPath,
			driver:        d,
			buffer:        make([]byte, 0, maxChunkSize),
			uploadCtxList: make([]scs.Part, 0, 1),
			multi:multi,
			number: 1,
		}
		d.writerPath[scsPath] = w
	} else {
		w = d.writerPath[scsPath].(*writer)
		w.closed = false

	}
	return w, nil
}

// Stat retrieves the FileInfo for the given path, including the current size
// in bytes and the creation time.
func (d *driver) Stat(ctx context.Context, path string) (storagedriver.FileInfo, error) {
	d.logger.WithFields(logrus.Fields{
		"path": path,
	}).Debugln("Stat")
	listResponse, err := d.Bucket.ListObject(d.scsPath(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var data = make(map[string]interface{})
	json.Unmarshal(listResponse, &data)

	content := jsoniter.Get(listResponse, "Contents")
	prefix := jsoniter.Get(listResponse, "CommonPrefixes")

	if content.Size() == 1 {
		if content.Get(0, "Name").ToString() != d.scsPath(path) {
			fi.IsDir = true
		} else {
			fi.IsDir = false
			fi.Size = content.Get(0, "Size").ToInt64()

			/*Parse file make time*/
			timeline := content.Get(0, "Last-Modified").ToString()

			dateString := strings.Split(timeline, ",")
			dateDetail := strings.Split(dateString[1], " ")

			m := make(map[string]string)

			m["jan"] = "01"
			m["feb"] = "02"
			m["mar"] = "03"
			m["apr"] = "04"
			m["may"] = "05"
			m["jun"] = "06"
			m["jul"] = "07"
			m["aug"] = "08"
			m["sep"] = "09"
			m["sept"] = "09"
			m["oct"] = "10"
			m["nov"] = "11"
			m["dec"] = "12"

			month := m[strings.ToLower(dateDetail[2])]
			timeUse := fmt.Sprintf("%s-%s-%sT%s.000Z", dateDetail[3], month, dateDetail[1], dateDetail[4])
			timestamp, err := time.Parse(time.RFC3339Nano, timeUse)
			if err != nil {
				return nil, err
			}
			fi.ModTime = timestamp
		}
	} else if prefix.Size() == 1 {
		fi.IsDir = true
	} else {
		return nil, storagedriver.PathNotFoundError{Path: path}
	}

	return storagedriver.FileInfoInternal{FileInfoFields: fi}, nil
}

/*list*/
func (d *driver) List(ctx context.Context, subPath string) ([]string, error) {
	d.logger.WithFields(logrus.Fields{
		"path": subPath,
	}).Debugln("List")

	pathUse := subPath
	if pathUse != "/" && subPath[len(pathUse)-1] != '/' {
		pathUse = pathUse + "/"
	}

	prefix := ""
	if d.scsPath("") == "" {
		prefix = "/"
	}

	scsPath := d.scsPath(pathUse)
	listResponse, err := d.Bucket.ListObject(scsPath, "/", "", listMax)
	if err != nil {
		return nil, parseError(subPath, err)
	}
	var files []string
	var directories []string
	for {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		data := make(map[string]interface{})
		json.Unmarshal(listResponse, &data)
		Contents := jsoniter.Get(listResponse, "Contents")
		CommonPrefixes := jsoniter.Get(listResponse, "CommonPrefixes")
		if Contents.Size() == 0 && CommonPrefixes.Size() == 0 {
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
		if Contents.Size() != 0 {
			for i := 0; i < Contents.Size(); i++ {
				files = append(files, strings.Replace(Contents.Get(i, "Name").ToString(), d.scsPath(""), prefix, 1))
			}
		}
		if CommonPrefixes.Size() != 0 {
			for i := 0; i < CommonPrefixes.Size(); i++ {
				tmp := CommonPrefixes.Get(i, "Prefix").ToString()
				directories = append(directories, strings.Replace(tmp[0:len(tmp)-1], d.scsPath(""), prefix, 1))
			}
		}
		if jsoniter.Get(listResponse, "IsTruncated").ToBool() {
			nextMarker := jsoniter.Get(listResponse, "NextMarker").ToString()
			listResponse, err = d.Bucket.ListObject(scsPath, "/", nextMarker, listMax)
			if err != nil {
				return nil, err
			}
		} else {
			break
		}
	}
	// This is to cover for the cases when the first key equal to ossPath.
	if len(files) > 0 && files[0] == strings.Replace(scsPath, d.scsPath(""), prefix, 1) {
		files = files[1:]
	}

	if subPath != "/" {
		if len(files) == 0 && len(directories) == 0 {
			// Treat empty response as missing directory, since we don't actually
			// have directories in s3.
			return nil, storagedriver.PathNotFoundError{Path: subPath}
		}
	}
	return append(files, directories...), nil
}

// Move moves an object stored at sourcePath to destPath, removing the original
// object.
func (d *driver) Move(ctx context.Context, sourcePath string, destPath string) error {
	d.logger.WithFields(logrus.Fields{
		"sourcePath": sourcePath,
		"destPath":   destPath,
	}).Debugln("Move")
	err := d.Bucket.Copy(d.scsPath(destPath), d.bucketName, d.scsPath(sourcePath))
	if err != nil {
		logrus.Errorf("Failed for move from %s to %s: %v", d.scsPath(sourcePath), d.scsPath(destPath), err)
		return parseError(sourcePath, err)
	}

	return d.Delete(ctx, sourcePath)
}

func (d *driver) Delete(ctx context.Context, path string) error {
	d.logger.WithFields(logrus.Fields{
		"path": path,
	}).Debugln("Delete")
	scsPath := d.scsPath(path)
	listResponse, err := d.Bucket.ListObject(scsPath, "", "", listMax)
	if err != nil {
		return storagedriver.PathNotFoundError{Path: path}
	}
	for {
		json := jsoniter.ConfigCompatibleWithStandardLibrary
		data := make(map[string]interface{})
		json.Unmarshal(listResponse, &data)
		content := jsoniter.Get(listResponse, "Contents")
		if content.Size() == 0 {
			return storagedriver.PathNotFoundError{Path: path}
		}

		if content.Size() != 0 {
			for i := 0; i < content.Size(); i++ {
				tmpName := content.Get(i, "Name").ToString()
				if len(tmpName) > len(scsPath) && tmpName[len(strings.TrimRight(scsPath, "/"))] != '/' {
					// 删除/a 不删除/ab
					break
				}
				err := d.Bucket.Del(tmpName)
				if err != nil {
					return err
				}
			}
		}
		if jsoniter.Get(listResponse, "IsTruncated").ToBool() {
			nextMarker := jsoniter.Get(listResponse, "NextMarker").ToString()
			listResponse, err = d.Bucket.ListObject(scsPath, "", nextMarker, listMax)
			if err != nil {
				return err
			}
		} else {
			break
		}
	}
	return nil
}

// URLFor returns a URL which may be used to retrieve the content stored at the given path.
// May return an UnsupportedMethodErr in certain StorageDriver implementations.
func (d *driver) URLFor(ctx context.Context, path string, options map[string]interface{}) (string, error) {
	d.logger.WithFields(logrus.Fields{
		"path": path,
	}).Debugln("URLFor")
	methodString := "GET"
	method, ok := options["method"]
	if ok {
		methodString, ok = method.(string)
		if !ok || (methodString != "GET") {
			return "", storagedriver.ErrUnsupportedMethod{}
		}
	}

	expiresTime := time.Now().Add(20 * time.Minute)

	expires, ok := options["expiry"]
	if ok {
		et, ok := expires.(time.Time)
		if ok {
			expiresTime = et
		}
	}
	logrus.Infof("methodString: %s, expiresTime: %v", methodString, expiresTime)
	signedURL := d.Bucket.SignURL(d.scsPath(path), expiresTime)
	logrus.Infof("signed URL: %s", signedURL)
	return signedURL, nil
}

func (d *driver) Walk(ctx context.Context, path string, f storagedriver.WalkFn) error {
	return storagedriver.WalkFallback(ctx, d, path, f)
}

func (d *driver) scsPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.rootDirectory, "/")+path, "/")
}

func (d *driver) RemoveWriter(key string) {
	delete(d.writerPath, key)
}

type writer struct {
	driver *driver
	key    string
	size   int64

	closed    bool
	committed bool
	cancelled bool

	buffer        []byte
	uploadCtxList []scs.Part
	multi     *scs.Multi
	number int
}

func (w *writer) Write(p []byte) (int, error) {
	w.driver.logger.WithFields(logrus.Fields{
	 	"key": w.key,
	 }).Debugln("Write")
	if w.closed {
		return 0, fmt.Errorf("already closed")
	} else if w.committed {
		return 0, fmt.Errorf("already committed")
	} else if w.cancelled {
		return 0, fmt.Errorf("already cancelled")
	}

	if err := w.flushBuffer(); err != nil {
		return 0, err
	}

	w.buffer = append(w.buffer, p...)
	if len(w.buffer) >= maxChunkSize {
		if err := w.flushBuffer(); err != nil {
			return 0, err
		}
	}

	w.size += int64(len(p))

	return len(p), nil
}

func (w *writer) Size() int64 {
	return w.size
}

func (w *writer) Close() error {
	w.driver.logger.WithFields(logrus.Fields{
		"key":    w.key,
		"size":   w.size,
		"buffer": len(w.buffer),
	}).Debugln("Close")
	if w.closed {
		return fmt.Errorf("already closed")
	}

	if err := w.flushBuffer(); err != nil {
		return err
	}
	w.closed = true
	return nil
}

func (w *writer) Cancel() error {
	w.driver.logger.WithFields(logrus.Fields{
		"key":    w.key,
		"size":   w.size,
		"buffer": len(w.buffer),
	}).Debugln("Cancel")
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	}
	w.cancelled = true
	return nil
}

func (w *writer) Commit() error {
	defer func() {
		w.driver.RemoveWriter(w.key)
	}()
	w.driver.logger.WithFields(logrus.Fields{
		"key":    w.key,
		"size":   w.size,
		"buffer": len(w.buffer),
	}).Debugln("Commit")
	if w.closed {
		return fmt.Errorf("already closed")
	} else if w.committed {
		return fmt.Errorf("already committed")
	} else if w.cancelled {
		return fmt.Errorf("already cancelled")
	}

	if err := w.flushBuffer(); err != nil {
		return err
	}

	if len(w.buffer) > 0 {
		if err := w.mkblk(w.buffer); err != nil {
			return err
		}
	}

	if err := w.mkfile(); err != nil {
		return err
	}

	w.committed = true
	return nil
}

func (w *writer) flushBuffer() error {

	for len(w.buffer) >= maxChunkSize {
		if err := w.mkblk(w.buffer[:maxChunkSize]); err != nil {
			return err
		}
		w.buffer = w.buffer[maxChunkSize:]
	}
	return nil
}

/*上传分片*/
func (w *writer) mkblk(blob []byte) error {
	contType := http.DetectContentType(blob)
	part, err := w.multi.PutPartSelf(blob, contType, scs.Private, w.number)
	if err != nil {
		w.driver.logger.WithError(err).Errorln("upload file piece error")
		return err
	}
	w.number++
	w.uploadCtxList = append(w.uploadCtxList, part)
	return nil
}

/*合并文件*/
func (w *writer) mkfile() error {
	listPart, err := w.multi.ListPart()
	if err != nil {
		return err
	}
	err = w.multi.Complete(listPart)
	if err != nil {
		w.driver.logger.WithError(err).Errorln("complete file piece error")
		return err
	}
	return nil
}

func encode(raw string) string {
	return base64.URLEncoding.EncodeToString([]byte(raw))
}