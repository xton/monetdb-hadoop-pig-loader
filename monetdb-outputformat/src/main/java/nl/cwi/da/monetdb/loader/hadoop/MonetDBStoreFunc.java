package nl.cwi.da.monetdb.loader.hadoop;

import org.apache.hadoop.fs.*;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.*;
import org.apache.pig.ResourceSchema.ResourceFieldSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;
import org.apache.pig.impl.util.ObjectSerializer;
import org.apache.pig.impl.util.UDFContext;
import org.apache.pig.impl.util.Utils;
import org.apache.pig.tools.counters.PigCounterHelper;
import org.apache.pig.tools.pigstats.PigStatusReporter;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class MonetDBStoreFunc implements StoreFuncInterface, StoreMetadata {

	public String relToAbsPathForStoreLocation(String location, Path curDir)
			throws IOException {
		return LoadFunc.getAbsolutePath(location, curDir);
	}

    private String udfcSignature = null;
	public static Logger log = Logger.getLogger(MonetDBStoreFunc.class);

	private static final String SCHEMA_SQL = "schema.sql";
	private static final String LOADER_SQL = "load.sql";
    private static final String SCHEMA_KEY = "pig.monetdb.schema";

	@SuppressWarnings("rawtypes")
	public OutputFormat getOutputFormat() throws IOException {
		return new MonetDBOutputFormat();
	}

	private MonetDBSQLSchema sqlSchema = new MonetDBSQLSchema();

    // hat tip: http://chimera.labs.oreilly.com/books/1234000001811/ch11.html#store_func_frontend

	public void setStoreLocation(String location, Job job) throws IOException {
		FileOutputFormat.setOutputPath(job, new Path(location));
	}

    @Override
    public void storeSchema(ResourceSchema s, String location, Job job) throws IOException {

        // idempotentize.. probably not necessary
        boolean addColumns = sqlSchema.getNumCols() == 0;
        PigStatusReporter sr = PigStatusReporter.getInstance();
        PigCounterHelper ch = new PigCounterHelper();

        if (sr != null) sr.setStatus("Writing SQL schema");

        for (ResourceFieldSchema rfs : s.getFields()) {
            if (!pigSqlTypeMap.containsKey(rfs.getType())) {
                throw new IOException("Unsupported Column type: "
                        + rfs.getName() + " (" + rfs.getType() + ") - Sorry!");
            }
            if (addColumns) {
                sqlSchema.addColumn(rfs.getName(),
                        pigSqlTypeMap.get(rfs.getType()));
            }
        }

        Path p = new Path(location);
        FileSystem fs = p.getFileSystem(job.getConfiguration());

        String tableName = p.getName();
        sqlSchema.setTableName(tableName);
        int numCols = sqlSchema.getNumCols();

        Path schemaPath = p.suffix("/" + SCHEMA_SQL);
        if (!fs.exists(schemaPath)) {
            FSDataOutputStream os = fs.create(schemaPath);
            os.write(sqlSchema.toSQL().getBytes());
            os.close();
        }
        log.info("Wrote SQL Schema to " + schemaPath);

        FileStatus[] partDirs = fs.listStatus(p, new PathFilter() {
            public boolean accept(Path arg0) {
                return arg0.getName().startsWith(
                        MonetDBRecordWriter.FOLDER_PREFIX);
            }
        });
        StringBuilder sb = new StringBuilder(2048);

        for (FileStatus fis : partDirs) {
            sb.append("COPY BINARY INTO \"").append(tableName).append("\" FROM (\n");
            Path cp = fis.getPath();

            if(sr != null) sr.setStatus("Writing load for slice: " + cp);
            ch.incrCounter(getClass().getName(),"slices written",1);

            for (int colId = 0; colId < numCols; colId++) {
                Path cpp = cp.suffix("/" + MonetDBRecordWriter.FILE_PREFIX
                        + colId + MonetDBRecordWriter.FILE_SUFFIX);
                if (!fs.exists(cpp)) {
                    throw new IOException("Need path " + cpp
                            + ", but is non-existent.");
                }
                String colpartName = cp.getName() + "/" + cpp.getName();
                sb.append("'$PATH/").append(colpartName).append("'");
                if (colId < numCols - 1) {
                    sb.append(",\n");
                }

            }
            sb.append("\n);\n");
        }

        Path loaderPath = p.suffix("/" + LOADER_SQL);
        if (!fs.exists(loaderPath)) {
            FSDataOutputStream os = fs.create(loaderPath);
            os.write(sb.toString().getBytes());
            os.close();
        }

        log.info("Wrote SQL Loader to " + loaderPath);
    }

	public void checkSchema(ResourceSchema s) throws IOException {
        Properties p = getUdfProperties();
        p.setProperty(SCHEMA_KEY, s.toString());
	}

	@SuppressWarnings("rawtypes")
	private RecordWriter<WritableComparable, Tuple> w;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void prepareToWrite(RecordWriter writer) throws IOException {
        Properties props = getUdfProperties();
        ResourceSchema s = new ResourceSchema(Utils.getSchemaFromString(props.getProperty(SCHEMA_KEY)));
        ((MonetDBRecordWriter)writer).setPigSchema(s);
		w = writer;
	}

	public void putNext(Tuple t) throws IOException {
		try {
			w.write(null, t);
		} catch (InterruptedException e) {
			log.warn(e);
		}
	}

    @Override
    public void setStoreFuncUDFContextSignature(String signature) {
        udfcSignature = signature;
    }

    public Properties getUdfProperties() {
        UDFContext udfc = UDFContext.getUDFContext();
        return udfc.getUDFProperties(this.getClass(), new String[]{udfcSignature});
    }

	public void cleanupOnFailure(String location, Job job) throws IOException {
		Path p = new Path(location);
		FileSystem fs = p.getFileSystem(job.getConfiguration());
		fs.delete(p, true);
	}

	public void cleanupOnSuccess(String location, Job job) throws IOException {
        // don't. work here is done by storeschema
	}

	public static String serialize(Object o) throws IOException {
		return ObjectSerializer.serialize((Serializable) o);
	}

	public static Object deserialize(String schemaStr) throws IOException {
		return ObjectSerializer.deserialize(schemaStr);
	}

	private static Map<Byte, String> pigSqlTypeMap = new HashMap<Byte, String>();
	static {
		pigSqlTypeMap.put(DataType.BOOLEAN, "BOOLEAN");
		pigSqlTypeMap.put(DataType.BYTE, "TINYINT");
		pigSqlTypeMap.put(DataType.INTEGER, "INT");
		pigSqlTypeMap.put(DataType.LONG, "BIGINT");
		pigSqlTypeMap.put(DataType.FLOAT, "REAL");
		pigSqlTypeMap.put(DataType.DOUBLE, "DOUBLE");
		pigSqlTypeMap.put(DataType.CHARARRAY, "CLOB");
	}

    @Override
    public void storeStatistics(ResourceStatistics stats, String location, Job job) throws IOException {
        // don't
    }

}
