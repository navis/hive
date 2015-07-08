package org.apache.hadoop.hive.ql.io;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DFSUtil;
import org.apache.hadoop.util.Progressable;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class TestFileSystem extends FileSystem {

  private Path workingDir;

  private URI uri;
  private String homeDirPrefix = "/user";

  private final Map<String, Object> root = new LinkedHashMap<String, Object>();

  @Override
  public String getScheme() {
    return "hdfs";
  }

  @Override
  public URI getUri() {
    return this.uri;
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.setConf(conf);
    this.homeDirPrefix = conf.get("dfs.user.home.dir.prefix", "/user");
    this.uri = URI.create(uri.getScheme() + "://" + uri.getAuthority());
    this.workingDir = getHomeDirectory();
    BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(conf.get("test.fs.contents"))));

    String line;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      String[] splits = line.split(" ");
      Path path = new Path(splits[0]);
      if (splits.length == 1) {
        mkdirs(path);
      } else {
        int length = Integer.valueOf(splits[1]);
        @SuppressWarnings("unchecked")
        Map<String, Object> dir = (Map<String, Object>) _mkdirs(path.getParent());
        dir.put(path.getName(), new FileStatus(length, false, 1, 64 << 10, 0, path));
      }
    }
  }

  @Override
  public Path getHomeDirectory() {
    return makeQualified(new Path(homeDirPrefix + "/hive"));
  }

  @Override
  public FSDataInputStream open(Path f, int bufferSize) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream create(Path f, FsPermission permission, boolean overwrite, int bufferSize, short replication, long blockSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public FSDataOutputStream append(Path f, int bufferSize, Progressable progress) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean rename(Path src, Path dst) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  public boolean delete(Path f, boolean recursive) throws IOException {
    throw new UnsupportedOperationException();
  }

  @Override
  @SuppressWarnings("unchecked")
  public FileStatus[] listStatus(Path f) throws IOException {
    Object current = find(f = fixRelativePart(f));
    if (current instanceof Map) {
      Map<String, Object> directory = (Map)current;
      int i = 0;
      FileStatus[] statuses = new FileStatus[directory.size()];
      for (Map.Entry<String, Object> entry : directory.entrySet()) {
        statuses[i++] = toFileStatus(new Path(f, entry.getKey()), entry.getValue());
      }
      return statuses;
    }
    return new FileStatus[]{(FileStatus)current};
  }

  @Override
  public void setWorkingDirectory(Path new_dir) {
    String result = this.fixRelativePart(new_dir).toUri().getPath();
    if(!DFSUtil.isValidName(result)) {
      throw new IllegalArgumentException("Invalid DFS directory name " + result);
    } else {
      this.workingDir = this.fixRelativePart(new_dir);
    }
  }

  @Override
  public Path getWorkingDirectory() {
    return workingDir;
  }

  @Override
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return _mkdirs(f) != null;
  }

  @SuppressWarnings("unchecked")
  public Object _mkdirs(Path f) {
    Queue<String> names = split(f);
    Object current = root;
    while (!names.isEmpty()) {
      if (!(current instanceof Map)) {
        current = null;
        break;
      }
      String name = names.remove();
      Object child = ((Map) current).get(name);
      if (child == null) {
        ((Map) current).put(name, child = new LinkedHashMap<String, Object>());
      }
      current = child;
    }
    return current;
  }

  @Override
  public FileStatus getFileStatus(Path f) throws IOException {
    Object current = find(f = fixRelativePart(f));
    return toFileStatus(f, current);
  }

  private FileStatus toFileStatus(Path f, Object current) {
    if (current instanceof Map) {
      return new FileStatus(0, true, 0, 0, 0, f);
    }
    return (FileStatus)current;
  }

  Object find(Path f) throws FileNotFoundException {
    Queue<String> names = split(f);
    Object current = root;
    while (!names.isEmpty()) {
      if (!(current instanceof Map)) {
        throw new FileNotFoundException(f.toString());
      }
      current = ((Map) current).get(names.remove());
    }
    return current;
  }

  private Queue<String> split(Path f) {
    Queue<String> names = new LinkedList<>();
    Path cur = f;
    for (int i = f.depth(); i >= 0; i--, cur = cur.getParent()) {
      names.add(cur.getName());
    }
    return names;
  }

  protected Path fixRelativePart(Path p) {
    if (p.isUriPathAbsolute()) {
      return p;
    } else {
      return new Path(getWorkingDirectory(), p);
    }
  }
}
