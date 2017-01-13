package file

import (
	"github.com/childoftheuniverse/filesystem"

	"golang.org/x/net/context"
	"gopkg.in/fsnotify.v1"
	"net/url"
	"os"
)

/*
FileWatchers are used for holding all the accounting data necessary to keep
track of changes to specific files in the file system. They follow the
specified semantics of the filesystem API.
*/
type FileWatcher struct {
	cb       filesystem.FileWatchFunc
	watcher  *fsnotify.Watcher
	path     *url.URL
	shutdown bool
}

/*
NewFileWatcher creates a new FileWatcher watching for any changes in the
specified file or, when pointed to a directory, any files inside of it.
Changes will be reported using the callback. The initial version of the file
is also reported as a change, allowing to use this for e.g. loading a
configuration file in case of modifications.
*/
func NewFileWatcher(ctx context.Context, path *url.URL, cb filesystem.FileWatchFunc) (
	*FileWatcher, error) {
	var fi os.FileInfo
	var ret *FileWatcher
	var watcher *fsnotify.Watcher
	var err error

	watcher, err = fsnotify.NewWatcher()
	if err != nil {
		return nil, err
	}

	ret = &FileWatcher{
		cb:      cb,
		watcher: watcher,
		path:    path,
	}

	fi, err = os.Stat(path.Path)
	if err != nil {
		return nil, err
	}

	// Resolve symbolic links before we do anything.
	for fi.Mode()&os.ModeSymlink == os.ModeSymlink {
		var subpath string

		subpath, err = os.Readlink(path.Path)
		if err != nil {
			return nil, err
		}

		path, err = path.Parse(subpath)
		if err != nil {
			return nil, err
		}

		// Stat the resulting link again to figure out whether it's still
		// a symbolic link.
		fi, err = os.Stat(path.Path)
		if err != nil {
			return nil, err
		}
	}

	// Start watching for changes.
	err = watcher.Add(path.Path)
	if err != nil {
		return nil, err
	}

	if fi.IsDir() {
		// Watch for changes in any files below the directory. Watcher will
		// already have done that for us, but we should report the initial
		// versions of every file in the subtree.
		var dpath *url.URL
		var names []string
		var name string
		var f *os.File

		// Create a copy of the path with an extra "/" appended.
		dpath, err = path.Parse(path.Path + "/")
		if err != nil {
			return nil, err
		}

		f, err = os.Open(path.Path)
		if err != nil {
			return nil, err
		}

		names, err = f.Readdirnames(-1)
		if err != nil {
			return nil, err
		}

		for _, name = range names {
			var combined *url.URL
			var reader filesystem.ReadCloser

			combined, err = dpath.Parse(name)
			if err != nil {
				return nil, err
			}

			// The current state of the file is reported as the first change.
			reader, err = globalFileAdapter.OpenReader(ctx, combined)
			cb(combined, reader)
		}

		f.Close()
	} else {
		var reader filesystem.ReadCloser

		reader, err = globalFileAdapter.OpenReader(ctx, path)
		if err != nil {
			return nil, err
		}

		// The current state of the file is reported as the first change.
		cb(path, reader)
	}

	// Watching for and reporting future changes is handled asynchronously.
	go ret.watchForChanges()

	return ret, nil
}

/*
watchForChanges is invoked asynchronously and handles changes events from the
file system, routing the relevant ones (write, rename, etc.) to the
callback as requested.
*/
func (f *FileWatcher) watchForChanges() {
	// Use background context as this is not a synchronous process.
	var ctx = context.Background()

	for !f.shutdown {
		var event fsnotify.Event

		event = <-f.watcher.Events

		if event.Op&(fsnotify.Write|fsnotify.Remove|fsnotify.Rename) != 0 {
			var subject *url.URL
			var reader filesystem.ReadCloser
			var err error

			subject, err = f.path.Parse(event.Name)
			if err != nil {
				f.watcher.Errors <- err
				continue
			}

			reader, err = globalFileAdapter.OpenReader(ctx, subject)
			if err == nil {
				go f.cb(subject, reader)
			} else {
				f.watcher.Errors <- err
			}
		}
	}
}

/*
Shutdown tells the system to stop watching for changes to the file(s) and
shuts down the asynchronous change watching thread.
*/
func (f *FileWatcher) Shutdown() error {
	var err error

	f.shutdown = true
	err = f.watcher.Remove(f.path.Path)
	if err != nil {
		return err
	}

	return f.watcher.Close()
}

/*
Accessor method to get the error reporting channel.
*/
func (f *FileWatcher) ErrChan() chan error {
	return f.watcher.Errors
}
