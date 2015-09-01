package org.apache.hadoop.hive.ql.io;

import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.util.Progressable;

public class TestDFSFileSystem extends DistributedFileSystem {

  private final Map<String, Object> root = new LinkedHashMap<String, Object>();

  private static final Pattern PATTERN = Pattern.compile(
      "^([\\-drwx]{10})\\s+([\\-0-9]+)\\s+(.+)\\s+(.+)\\s+(\\d+)\\s+" +
      "(\\d{4}\\-\\d{2}\\-\\d{2} \\d{2}:\\d{2})\\s+(.+)$");
  private static final SimpleDateFormat DATA_FORMAT = new SimpleDateFormat("yyyy-MM-dd HH:mm");

  public static final String DFS_CONTENTS_LOC = "test.dfs.contents";

  @Override
  @SuppressWarnings("unchecked")
  public void initialize(URI uri, Configuration conf) throws IOException {
    super.initialize(uri, conf);
    this.setConf(conf);

    // drwxr-xr-x   - navis supergroup          0 2015-08-10 15:40 /user/hive/warehouse/alltypesorc
    // -rwxr-xr-x   1 navis supergroup     377237 2015-08-10 15:40 /user/hive/warehouse/alltypesorc/alltypesorc
    BufferedReader reader = new BufferedReader(new InputStreamReader(
        new FileInputStream(conf.get(DFS_CONTENTS_LOC))));
    String line;
    while ((line = reader.readLine()) != null) {
      line = line.trim();
      if (line.isEmpty()) {
        continue;
      }
      Matcher matcher = PATTERN.matcher(line);
      if (!matcher.matches()) {
        throw new IllegalArgumentException(line);
      }
      boolean isDir = matcher.group(1).startsWith("d");
      FsPermission permission = FsPermission.valueOf(matcher.group(1));
      String replica = matcher.group(2);
      String owner = matcher.group(3);
      String group = matcher.group(4);
      long length = Long.valueOf(matcher.group(5));
      long modTime;
      try {
        modTime = DATA_FORMAT.parse(matcher.group(6)).getTime();
      } catch (ParseException e) {
        throw new IOException(e);
      }
      Path path = new Path(matcher.group(7));
      if (isDir) {
        mkdirs(path);
      } else {
        Map<String, Object> dir = _mkdirs(path.getParent());
        dir.put(path.getName(), new FileStatus(length, false,
            replica.equals("-") ? 0 : Integer.valueOf(replica), 64 << 10,
            modTime, modTime, permission, owner, group, path));
      }
    }
    System.out.println("[TestDFSFileSystem/initialize] " + root.toString().replaceAll("\\{", "\\{\n"));
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
  public boolean mkdirs(Path f, FsPermission permission) throws IOException {
    return _mkdirs(f) != null;
  }

  @SuppressWarnings("unchecked")
  private Map<String, Object> _mkdirs(Path f) {
    Queue<String> names = split(f);
    Map<String, Object> current = root;
    while (!names.isEmpty()) {
      String name = names.remove();
      Object child = ((Map) current).get(name);
      if (child == null) {
        ((Map) current).put(name, child = new LinkedHashMap<String, Object>());
      }
      if (!(child instanceof Map)) {
        throw new IllegalStateException(name + " is not directory");
      }
      current = (Map<String, Object>)child;
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

  private Object find(Path f) throws FileNotFoundException {
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
    LinkedList<String> names = new LinkedList<String>();
    Path cur = f;
    for (int i = f.depth(); i >= 1; i--, cur = cur.getParent()) {
      names.add(cur.getName());
    }
    Collections.reverse(names);
    return names;
  }
}
