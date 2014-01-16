package nl.cwi.da.monetdb.loader.hadoop;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.text.NumberFormat;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.pig.ResourceSchema;
import org.apache.pig.data.DataType;
import org.apache.pig.data.Tuple;

@SuppressWarnings("rawtypes")
public class MonetDBRecordWriter extends
		RecordWriter<WritableComparable, Tuple> {

	public static Logger log = Logger.getLogger(MonetDBRecordWriter.class);

	private TaskAttemptContext context;
    private MonetDBOutputFormat outputFormat;

	private Map<Integer, ValueConverter> converters = new HashMap<Integer, ValueConverter>();

	public MonetDBRecordWriter(TaskAttemptContext context, MonetDBOutputFormat outputFormat) throws IOException {
		this.context = context;
        this.outputFormat = outputFormat;
	}

	public static final String FILE_PREFIX = "col-";
	public static final String FOLDER_PREFIX = "part-";
	public static final String FILE_SUFFIX = ".bulkload";

    private ResourceSchema pigSchema;

	private Map<Integer, OutputStream> writers = new HashMap<Integer, OutputStream>();
	boolean writersInitialized = false;

    public void setPigSchema(ResourceSchema s){ pigSchema = s; }

	abstract static class ValueConverter {
        abstract byte[] convert(Object value);

        protected ByteBuffer newBuffer(int length) {
            ByteBuffer b = ByteBuffer.allocate(length);
            b.order(ByteOrder.LITTLE_ENDIAN);
            return b;
        }
	}

	private static class BooleanValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(1);

		public byte[] convert(Object value) {
			bb.clear();
            bb.order(ByteOrder.LITTLE_ENDIAN);

			Boolean val = (Boolean) value;

			if (val == null) {
				bb.put((byte) Byte.MIN_VALUE);
			}
			if (val == true) {
				bb.put((byte) 1);
			}
			if (val == false) {
				bb.put((byte) 0);
			}
			bb.put((Byte) value);
			return bb.array();
		}
	}


	private static class ByteValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(1);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Byte.MIN_VALUE;
			}
			bb.put((Byte) value);
			return bb.array();
		}
	}

	private static class ShortValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(2);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null) {
				value = Byte.MIN_VALUE;
			}
			bb.putShort((Short) value);
			return bb.array();
		}
	}

	private static class IntegerValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(4);

		public byte[] convert(Object value) {
			bb.clear();
            if (value == null || !(value instanceof Number)) {
				value = Integer.MIN_VALUE;
			}
            bb.putInt(((Number) value).intValue());
			return bb.array();
		}
	}

	private static class LongValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(8);

		public byte[] convert(Object value) {
			bb.clear();
            if (value == null || !(value instanceof Number)) {
				value = Long.MIN_VALUE;
			}
            bb.putLong(((Number) value).longValue());
            return bb.array();
		}
	}

	private static class FloatValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(4);

		public byte[] convert(Object value) {
			bb.clear();
			if (value == null || !(value instanceof Number)) {
				value = Float.MIN_VALUE;
			}
            bb.putFloat(((Number) value).floatValue());

			return bb.array();
		}
	}

	private static class DoubleValueConverter extends ValueConverter {
		private ByteBuffer bb = newBuffer(8);

		public byte[] convert(Object value) {
			bb.clear();
            if (value == null || !(value instanceof Number)) {
				value = Double.MIN_VALUE;
			}
            bb.putDouble(((Number) value).doubleValue());
			return bb.array();
		}
	}

	private static class StringValueConverter extends ValueConverter {
		public byte[] convert(Object value) {
			return (((String) value) + "\n").getBytes();
		}
	}

    public void initializeWriters() throws IOException {
        if (!writersInitialized) {
            int i = 0;
            for(ResourceSchema.ResourceFieldSchema s : pigSchema.getFields()){

                // One file per column
                Path path = new Path(FOLDER_PREFIX + String.format("%08d",context.getTaskAttemptID().getTaskID().getId()) + "/" + FILE_PREFIX + i + FILE_SUFFIX);
                Path workOutputPath = ((FileOutputCommitter)outputFormat.getOutputCommitter(context)).getWorkPath();
                Path outputFile = new Path(workOutputPath, path);

                FileSystem fs = outputFile.getFileSystem(context.getConfiguration());
                OutputStream os = fs.create(outputFile);
                writers.put(i, os);

                if(s.getType() == DataType.BOOLEAN) {
                    converters.put(i, new BooleanValueConverter());
                } else if(s.getType() == DataType.BYTE) {
                    converters.put(i, new ByteValueConverter());
                } else if(s.getType() == DataType.INTEGER) {
                    converters.put(i, new IntegerValueConverter());
                } else if (s.getType() == DataType.INTEGER) {
                    converters.put(i, new IntegerValueConverter());
                } else if (s.getType() == DataType.LONG) {
                    converters.put(i, new LongValueConverter());
                } else if (s.getType() == DataType.FLOAT) {
                    converters.put(i, new FloatValueConverter());
                } else if (s.getType() == DataType.DOUBLE) {
                    converters.put(i, new DoubleValueConverter());
                } else if (s.getType() == DataType.CHARARRAY) {
                    converters.put(i, new StringValueConverter());
                } else throw new IOException(
                            "Unable to fill converter table. Supported values are Java primitive types and Strings!");

                i++;
            }

            writersInitialized = true;
        }
    }

	public void write(WritableComparable key, Tuple t) throws IOException {
        initializeWriters();

		// TODO: check that the maps have a mapping there?
		for (int i = 0; i < t.size(); i++) {
			writers.get(i).write(converters.get(i).convert(t.get(i)));
		}
		context.progress();
	}

	@Override
	public void close(TaskAttemptContext arg0) throws IOException,
			InterruptedException {
		for (Entry<Integer, OutputStream> e : writers.entrySet()) {
			e.getValue().close();
		}

	}
}