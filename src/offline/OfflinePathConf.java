package offline;

import org.apache.hadoop.fs.Path;

public class OfflinePathConf {
	public final static Path PATH_UNI = new Path("/output/unigram");
	public final static Path PATH_TF = new Path("/output/tf");
	public final static Path PATH_TF_IDF = new Path("/output/tf-idf");
	public final static Path PATH_ALL_WORDS = new Path("/output/allwords");
	public final static Path FILE_ALL_WORDS = new Path("/output/allwords/part-r-00000");
	public final static Path PATH_AAV = new Path("/output/aav");
}
