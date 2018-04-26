package scs
import (
	"bytes"
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"net/http"
	"reflect"
	"strconv"
	"strings"
	"time"

	scs "github.com/SinaCloudStorage/SinaCloudStorage-SDK-Go"
	storagedriver "github.com/docker/distribution/registry/storage/driver"
	"github.com/docker/distribution/registry/storage/driver/base"
	"github.com/docker/distribution/registry/storage/driver/factory"
	"github.com/sirupsen/logrus"
	"github.com/json-iterator/go"
	"encoding/base64"
	"encoding/json"
)

const (
	driverName = "scs"
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
		return nil, fmt.Errorf("No accesskey parameter provided")
	}
	secretKey, ok := parameters["secretkey"]
	if !ok {
		return nil, fmt.Errorf("No secretkey parameter provided")
	}


	bucket, ok := parameters["bucket"]
	if !ok || fmt.Sprint(bucket) == "" {
		return nil, fmt.Errorf("No bucket parameter provided")
	}

	endpoint, ok := parameters["endpoint"]
	if !ok {
		endpoint = ""
	}

	RootDirectory := "";

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

	client := scs.NewSCS(params.AccessKey, params.SecretKey, params.Endpoint)
	bucket := client.Bucket(params.Bucket)

	// Validate that the given credentials have at least read permissions in the
	// given bucket scope.
	if _, err := bucket.ListObject("", "", "", 1); err != nil {
		return nil, err
	}

	d := &driver{
		Client:        client,
		Bucket:        bucket,
		writerPath:    make(map[string]storagedriver.FileWriter),
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
	contentWrite := fmt.Sprintf("%s", contents)
	return parseError(path, d.Bucket.Put(path, contentWrite, getPermissions()))
}

// Reader retrieves an io.ReadCloser for the content stored at "path" with a
// given byte offset.
func (d *driver) Reader(ctx context.Context, path string, offset int64) (io.ReadCloser, error) {

	resp, err := d.Bucket.GetRange(path, offset)
	if err != nil {
		return nil, parseError(path, err)
	}

	return ioutil.NopCloser(bytes.NewReader([]byte(resp))), nil
}

// Writer returns a FileWriter which will store the content written to it
// at the location designated by "path" after the call to Commit.
func (d *driver) Writer(ctx context.Context, path string, append bool) (storagedriver.FileWriter, error) {
	var w *writer
	scsPath := d.scsPath(path);
	if !append {
		w = &writer{
			key:           scsPath,
			driver:        d,
			buffer:        make([]byte, 0, maxChunkSize),
			uploadCtxList: make([]string, 0, 1),
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
	listResponse, err := d.Bucket.ListObject(d.scsPath(path), "", "", 1)
	if err != nil {
		return nil, err
	}

	fi := storagedriver.FileInfoFields{
		Path: path,
	}

	var json = jsoniter.ConfigCompatibleWithStandardLibrary
	var data  = make(map[string]interface{})
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
			timeline_use := fmt.Sprintf("%s-%s-%sT%s.000Z", dateDetail[3], month, dateDetail[1], dateDetail[4])
			timestamp, err := time.Parse(time.RFC3339Nano, timeline_use)
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


func (d *driver) scsPath(path string) string {
	return strings.TrimLeft(strings.TrimRight(d.rootDirectory, "/")+path, "/")
}

type writer struct {
	driver *driver
	key    string
	size   int64

	closed    bool
	committed bool
	cancelled bool

	buffer        []byte
	uploadCtxList []string
}

func (d *driver) RemoveWriter(key string) {
	delete(d.writerPath, key)
}

func (w *writer) Write(p []byte) (int, error) {
	// w.driver.logger.WithFields(logrus.Fields{
	// 	"key": w.key,
	// }).Debugln("Write")
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
		if err := w.mkblk(bytes.NewReader(w.buffer), len(w.buffer)); err != nil {
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
		if err := w.mkblk(bytes.NewReader(w.buffer[:maxChunkSize]), maxChunkSize); err != nil {
			return err
		}
		w.buffer = w.buffer[maxChunkSize:]
	}
	return nil
}

func (w *writer) mkblk(blob io.Reader, blobSize int) error {
	url := fmt.Sprintf("%s/mkblk/%d", w.driver.client.UpHosts[0], blobSize)
	resp, err := w.post(url, blob, blobSize)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode == 200 {
		res := kodocli.BlkputRet{}
		err := json.NewDecoder(resp.Body).Decode(&res)
		if err != nil {
			return err
		}
		w.uploadCtxList = append(w.uploadCtxList, res.Ctx)
		return nil
	}

	if err := rpc.ResponseError(resp); err != nil {
		w.driver.logger.WithError(err).Errorln("mkblk failed")
		return err
	}
	return nil
}

func (w *writer) mkfile() error {
	url := fmt.Sprintf("%s/mkfile/%d/key/%s", w.driver.client.UpHosts[0], w.size, encode(w.key))
	buf := make([]byte, 0, 176*len(w.uploadCtxList))
	for _, ctx := range w.uploadCtxList {
		buf = append(buf, ctx...)
		buf = append(buf, ',')
	}
	if len(buf) > 0 {
		buf = buf[:len(buf)-1]
	}
	resp, err := w.post(url, bytes.NewReader(buf), len(buf))
	if err != nil {
		return err
	}

	if resp.StatusCode == 200 {
		return nil
	}
	defer resp.Body.Close()
	if err := rpc.ResponseError(resp); err != nil {
		w.driver.logger.WithFields(logrus.Fields{
			"error": err,
			"size":  w.size,
			"url":   url,
		}).Errorln("mkfile failed")
		return err
	}
	return nil
}

func (w *writer) post(url string, blob io.Reader, blobSize int) (*http.Response, error) {
	resp, err := func() (*http.Response, error) {
		req, err := http.NewRequest("POST", url, blob)
		if err != nil {
			return nil, err
		}
		policy := kodo.PutPolicy{
			Scope:   w.driver.bucket.Name,
			Expires: 3600,
			UpHosts: w.driver.bucket.UpHosts,
		}
		token, err := w.driver.client.MakeUptokenWithSafe(&policy)
		if err != nil {
			return nil, err
		}
		req.Header.Set(http.CanonicalHeaderKey("Host"), w.driver.client.UpHosts[0])
		req.Header.Set(http.CanonicalHeaderKey("Content-Type"), "application/octet-stream")
		req.Header.Set(http.CanonicalHeaderKey("Content-Length"), strconv.Itoa(blobSize))
		req.Header.Set(http.CanonicalHeaderKey("Authorization"), fmt.Sprintf("UpToken %s", token))
		client := http.Client{}
		return client.Do(req)
	}()

	if err != nil {
		w.driver.logger.WithFields(logrus.Fields{
			"url":   url,
			"error": err,
		}).Errorln("post failed")
	}
	return resp, err

}

func encode(raw string) string {
	return base64.URLEncoding.EncodeToString([]byte(raw))
}
