package file

import (
	"github.com/childoftheuniverse/filesystem"

	"golang.org/x/net/context"
	"io"
	"net/url"
	"os"
	"path"
)

var globalFileAdapter *FileAdapter

/*
Since local files do not need any configuration to set up, this adapter is
registered as soon as its relevant code is linked in.
*/
func init() {
	globalFileAdapter = &FileAdapter{}
	filesystem.AddImplementation("file", globalFileAdapter)
}

/*
File system adapter for local files. See
http://github.com/childoftheuniverse/filesystem/ for details of the API and
how to use it.
*/
type FileAdapter struct {
}

/*
ContextRespectingIoFile represents a regular file object from the OS, but with
implementations of respecting deadlines and cancellations from contexts.
*/
type ContextRespectingIoFile struct {
	actualFile *os.File
}

type asyncReadResult struct {
	Data   []byte
	Length int
	Error  error
}

func (f *ContextRespectingIoFile) asyncRead(length int, rchan chan *asyncReadResult) {
	var result = new(asyncReadResult)

	result.Data = make([]byte, length)
	result.Length, result.Error = f.actualFile.Read(result.Data)
	rchan <- result
}

func (f *ContextRespectingIoFile) asyncWrite(b []byte, lench chan int, errch chan error) {
	var length int
	var err error

	length, err = f.actualFile.Write(b)
	lench <- length
	errch <- err
}

func (f *ContextRespectingIoFile) asyncClose(errch chan error) {
	errch <- f.actualFile.Close()
}

/*
Read() provides regular read semantics, but with support for cancelling
reads or providing deadlines for them.
*/
func (f *ContextRespectingIoFile) Read(ctx context.Context, p []byte) (l int, err error) {
	var result *asyncReadResult
	var rchan = make(chan *asyncReadResult, 1)
	go f.asyncRead(len(p), rchan)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case result = <-rchan:
		if result.Error == nil {
			copy(p, result.Data)
		}
		return result.Length, result.Error
	}
}

/*
Write() provides regular write semantics, but with support for cancelling
writes or providing deadlines for them.
*/
func (f *ContextRespectingIoFile) Write(ctx context.Context, b []byte) (int, error) {
	var lench = make(chan int, 1)
	var errch = make(chan error, 1)
	var nb = make([]byte, len(b))
	var err error
	var length int
	copy(nb, b)

	go f.asyncWrite(nb, lench, errch)

	select {
	case <-ctx.Done():
		return 0, ctx.Err()
	case err = <-errch:
		length = <-lench
		return length, err
	}
}

/*
Tell() determines the current offset inside the file and returns it.
*/
func (f *ContextRespectingIoFile) Tell(ctx context.Context) (int64, error) {
	return f.actualFile.Seek(0, io.SeekCurrent)
}

/*
Seek() sets the current position in the file to the absolute offset specified.
*/
func (f *ContextRespectingIoFile) Seek(
	ctx context.Context, offset int64, whence int) (int64, error) {
	return f.actualFile.Seek(offset, io.SeekStart)
}

/*
Skip() skips forward by the specified number of bytes without actually reading
the data.
*/
func (f *ContextRespectingIoFile) Skip(ctx context.Context, n int64) error {
	var err error
	_, err = f.actualFile.Seek(n, io.SeekCurrent)
	return err
}

/*
Close() provides regular close semantics, but with support for cancelling
waiting for closes to finish (which may be important due to caches) or
providing deadlines for them.
*/
func (f *ContextRespectingIoFile) Close(ctx context.Context) error {
	var errch = make(chan error, 1)
	var err error

	go f.asyncClose(errch)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errch:
		return err
	}
}

/*
NewContextRespectingIoFile wraps a regular os.File so we get a context
respecting API on it.
*/
func NewContextRespectingIoFile(actualFile *os.File) *ContextRespectingIoFile {
	return &ContextRespectingIoFile{actualFile: actualFile}
}

func asyncOpenRead(path string, rchan chan filesystem.ReadCloser, errchan chan error) {
	var file *os.File
	var err error

	file, err = os.Open(path)
	if err != nil {
		errchan <- err
	} else {
		rchan <- NewContextRespectingIoFile(file)
	}
}

func asyncOpenWrite(fpath string, flag int, rchan chan filesystem.WriteCloser, errchan chan error) {
	var file *os.File
	var err error

	err = os.MkdirAll(path.Dir(fpath), 0755)
	if err != nil {
		errchan <- err
		return
	}

	file, err = os.OpenFile(fpath, flag, 0644)
	if err != nil {
		errchan <- err
	} else {
		rchan <- NewContextRespectingIoFile(file)
	}
}

func asnycListEntries(dirurl *url.URL, rch chan []string, errch chan error) {
	var f *os.File
	var res []string
	var err error

	f, err = os.Open(dirurl.Path)
	if err != nil {
		errch <- err
		return
	}
	defer f.Close()

	res, err = f.Readdirnames(-1)
	if err != nil {
		errch <- err
		return
	}
	rch <- res
}

func asyncRemove(objurl *url.URL, errch chan error) {
	errch <- os.Remove(objurl.Path)
}

/*
Asynchronously create a reader reading from the specified file. The actual
opening will happen in a subthread so that we have a guaranteed response time
from this function in case the operation exceeds the alotted time limits.
*/
func (file *FileAdapter) OpenReader(
	ctx context.Context, fileurl *url.URL) (rc filesystem.ReadCloser, err error) {
	var rchan = make(chan filesystem.ReadCloser, 1)
	var errchan = make(chan error, 1)
	go asyncOpenRead(fileurl.Path, rchan, errchan)
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case err = <-errchan:
		return
	case rc = <-rchan:
		return
	}
}

/*
Asynchronously create a writer writing to the specified file, overwriting all
existent contents. The actual opening will happen in a subthread so that we
have a guaranteed response time from this function in case the operation
exceeds the alotted time limits.
*/
func (file *FileAdapter) OpenWriter(
	ctx context.Context, fileurl *url.URL) (rc filesystem.WriteCloser, err error) {
	var rchan = make(chan filesystem.WriteCloser, 1)
	var errchan = make(chan error, 1)
	go asyncOpenWrite(fileurl.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		rchan, errchan)
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case err = <-errchan:
		return
	case rc = <-rchan:
		return
	}
}

/*
Asynchronously create a writer writing to the specified file, appending to the
end of existent contents. The actual opening will happen in a subthread so
that we have a guaranteed response time from this function in case the
operation exceeds the alotted time limits.
*/
func (file *FileAdapter) OpenAppender(
	ctx context.Context, fileurl *url.URL) (rc filesystem.WriteCloser, err error) {
	var rchan = make(chan filesystem.WriteCloser, 1)
	var errchan = make(chan error, 1)
	go asyncOpenWrite(fileurl.Path, os.O_WRONLY|os.O_CREATE|os.O_TRUNC,
		rchan, errchan)
	select {
	case <-ctx.Done():
		err = ctx.Err()
		return
	case err = <-errchan:
		return
	case rc = <-rchan:
		return
	}
}

/*
ListEntries asynchronously reads the contents of a directory and returns the
relative names of files and subdirectories in it. The actual enumeration will
happen in a subthread so that we have a guaranteed response time from this
function in case the operation exceeds the alotted time limits.
*/
func (file *FileAdapter) ListEntries(ctx context.Context, dirurl *url.URL) ([]string, error) {
	var rch = make(chan []string, 1)
	var errch = make(chan error, 1)
	var results []string
	var err error

	go asnycListEntries(dirurl, rch, errch)

	select {
	case <-ctx.Done():
		return results, ctx.Err()
	case err = <-errch:
		return results, err
	case results = <-rch:
		return results, nil
	}
}

/*
Watch for changes affecting the file pointed to. Context is ignored since it
probably wouldn't be meaningful in this context. The current state of the file
will be notified at first as the initial change.
*/
func (file *FileAdapter) WatchFile(ctx context.Context, fileurl *url.URL, notify filesystem.FileWatchFunc) (filesystem.CancelWatchFunc, chan error, error) {
	var watcher *FileWatcher
	var err error

	watcher, err = NewFileWatcher(ctx, fileurl, notify)
	if err != nil {
		return nil, nil, err
	}

	return watcher.Shutdown, watcher.ErrChan(), nil
}

/*
Remove asynchronously deletes the object pointed to from the file system,
without touching any entries below it (files, subdirectories). The actual
deletion will happen in a subthread so that we have a guaranteed response
time from this function in case the operation exceeds the alotted time limits.
*/
func (file *FileAdapter) Remove(ctx context.Context, objurl *url.URL) error {
	var errch = make(chan error, 1)
	var err error

	go asyncRemove(objurl, errch)

	select {
	case <-ctx.Done():
		return ctx.Err()
	case err = <-errch:
		return err
	}
}
