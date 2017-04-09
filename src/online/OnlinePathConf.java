package online;

import org.apache.hadoop.fs.Path;

public class OnlinePathConf {
	public final static Path PATH_UNKNOWN_UNI = new Path("/output/unknownUnigram");
	public final static Path PATH_UNKNOWN_TF = new Path("/output/unknownTF");
	public final static Path PATH_UNKNOWN_AAV = new Path("/output/unknownAAV");
	public final static Path FILE_UNKNOWN_AAV = new Path("/output/unknownAAV/part-r-00000");
	public final static Path PATH_RAW_RESULT = new Path("/output/rawResult");
}
